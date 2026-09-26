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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewCheckpointKeyedScanCost;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the pieces a keyed repair is made of: the shared partition identity a view
 * names its keys through, the forward index-backed scan that follows those keys' rows, and
 * the cost model that decides whether following them is cheaper than reading the segment.
 * <p>
 * None of it changes what a repair does. A closed segment still replays whole, because a
 * keyed replay's <b>publication</b> is the piece that is missing: {@code REPLACE_RANGE}
 * deletes the segment's range wholesale, so a replay emitting only the affected keys' rows
 * would drop every unaffected key's stored row inside it. What these cases pin is that the
 * inputs to that decision are right, and that the measurement they produce is the real
 * comparison rather than a model of it.
 * <p>
 * The view is the same reported customer shape the per-segment repair cases use: an
 * anchored WINDOW carrying an unbounded cumulative sum per account, over a base whose
 * timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewCheckpointKeyedScanTest extends AbstractLiveViewTest {
    // A 1ms anchor, which createNarrowAnchorView's partition spreads so thin that a whole
    // segment of it estimates at zero rows.
    private static final String NARROW_ANCHOR = "timestamp_floor('1T', created_at)";
    // Ceiling range the mid-build OOM sweep walks, and the step it advances by. A whole keyed
    // open over the fixture below allocates around 14.4 KiB of tracked native memory, so the
    // range crosses the transition from "every point faults" to "the open completes" with room
    // to spare; the sweep's own assertions fail loudly if an allocation-path change moves it
    // past the end. The step stays below one block buffer's 32-byte floor, so it cannot walk
    // straight over the window between "one key's cursor allocated" and "the next key's
    // failed" - the only window the strand lives in.
    private static final int OOM_SWEEP_SLACK_MAX = 48 * 1024;
    private static final int OOM_SWEEP_SLACK_STEP = 16;

    @Test
    public void testACorrectionInOneClosedSegmentPricesItsKeyedScan() throws Exception {
        // The measurement the stage exists to take: one account corrected inside one closed
        // day, against a day holding every account's rows. The keyed side has to be the
        // smaller of the two, and the verdict has to say so.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // The default prices one index open at 256 base rows, which is what a real
        // hourly-partitioned base against a daily segment is worth - and at forty rows a
        // day it would (correctly) prefer the whole segment whatever the key domain. The
        // verdict this case is about is the row comparison, so the setup term is priced at
        // the scale the fixture actually has.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                Assert.assertEquals(0, job.keyedScanPricedCountForTest());

                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(
                        "the corrected closed day must be priced exactly once",
                        1,
                        job.keyedScanPricedCountForTest()
                );
                Assert.assertEquals(0, job.keyedScanUnpricedCountForTest());
                Assert.assertEquals(
                        "one account of four is less to read than the whole day",
                        1,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertTrue(
                        "the keyed scan must read fewer rows than the whole segment: posting="
                                + job.keyedScanPostingRowsForTest()
                                + " whole=" + job.keyedScanWholeRangeRowsForTest(),
                        job.keyedScanPostingRowsForTest() < job.keyedScanWholeRangeRowsForTest()
                );
                // The corrected account holds ten seeded rows in that day plus the
                // correction, so anything below eleven means the key never resolved to the
                // rows it names - which reads as a spectacular saving rather than as a bug.
                Assert.assertEquals(
                        "the priced keyed scan must find the corrected account's rows",
                        11,
                        job.keyedScanPostingRowsForTest()
                );
                Assert.assertEquals(
                        "the repair itself is unchanged - the segment still replays whole",
                        1,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testPricingLeavesTheReadersIndexUsableForALaterQuery() throws Exception {
        // The estimate reads through the repair's own pinned reader, which is a pooled one.
        // TableReader hands out an index reader per partition and, for a partition whose
        // columns it has not mapped yet, hands out AND CACHES one that yields no row at all -
        // so an estimate that skipped the open would report the keyed scan as free and leave
        // that cached no-op reader behind for the next index-driven query on the same reader.
        // Both halves are asserted here: the count the estimate reaches, and the count a
        // query reaches afterwards.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // The shipped 256-row open price puts a one-key scan's setup floor above this 41-row
        // day, so the job would decline the segment before it opens a partition. At 13 the
        // job prices it, and the walk reaches the corrected hour's partition before the
        // verdict settles: the first partition's ten postings and both partitions' setup come
        // to 10 + 2 * 13 + 2 * 2 = 40, one row below the day, so it opens that partition's
        // index. Its posting brings the price to 41, and the job still declines it, so the
        // repair reads whole as it does at the default.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 13);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                // Lands in an anchor day that is closed, and in an hour partition of its own
                // that the pinned reader has never opened.
                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(
                        "the repair's own estimate is what the query below checks up on",
                        1,
                        job.keyedScanPricedCountForTest()
                );
                Assert.assertEquals(11, job.keyedScanPostingRowsForTest());
                Assert.assertEquals(0, job.keyedScanCheaperCountForTest());

                final String indexed = "select count() from tx "
                        + "where created_at in '2026-01-02' and account_id = 'acct-1'";
                final String scanned = "select count() from tx "
                        + "where created_at in '2026-01-02' and account_id::string = 'acct-1'";
                Assert.assertEquals("the whole-scan oracle", 11, count(scanned));
                Assert.assertEquals(
                        "an index-driven query after a priced repair must still find every row",
                        11,
                        count(indexed)
                );

                final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
                try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                    cost.of(reader, sqlExecutionContext);
                    final IntList keys = new IntList();
                    keys.add(reader.getSymbolMapReader(1).keyOf("acct-1"));
                    Assert.assertEquals(
                            "the estimate must count the rows in a partition it has not opened yet",
                            11,
                            cost.estimateKeyedScanRows(
                                    ts("2026-01-02T00:00:00.000000Z"),
                                    ts("2026-01-02T23:59:59.999999Z"),
                                    1,
                                    keys,
                                    Long.MAX_VALUE
                            )
                    );
                }
                Assert.assertEquals(
                        "pricing must leave the reader's index cache usable",
                        11,
                        count(indexed)
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyDomainOverTheBudgetLeavesTheSegmentUnpriced() throws Exception {
        // A budget of one key against a correction carrying two: the segment keeps the keys
        // it collected and reports the domain incomplete, which is not a denial - it reads
        // whole, which is what it does anyway.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(2, 3, "acct-1") + ", " + row(2, 4, "acct-2"), job);

                Assert.assertEquals(0, job.keyedScanPricedCountForTest());
                Assert.assertEquals(
                        "a segment past its key budget must be reported unpriced",
                        1,
                        job.keyedScanUnpricedCountForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentEstimatedAtZeroRowsWalksNoPostings() throws Exception {
        // A 1ms anchor at the tail of a day-long partition. The whole-range estimate spreads
        // the partition's rows evenly over its span, so a 1ms segment interpolates to zero
        // rows although it really holds a few. No keyed scan undercuts a zero-row whole range
        // - isKeyedScanCheaper wants the keyed price strictly below it - so the verdict is
        // settled before a posting is counted. The corrected account holds every row of the
        // partition, and a bitmap index reports no size, so pricing it anyway walks all of
        // them to reach a verdict it already had, once per correction.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createNarrowAnchorView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                for (int correction = 1; correction <= 2; correction++) {
                    // Inside the closed 23:00:00.000 anchor, below the open 23:00:01.000 one.
                    commit("('2026-01-02T23:00:00.00000" + correction + "Z', 'acct-1', 1.0)", job);

                    Assert.assertEquals(
                            "a segment estimated at zero rows must not walk its keys' postings",
                            0,
                            job.keyedScanPostingRowsForTest()
                    );
                    Assert.assertEquals(0, job.keyedScanPricedCountForTest());
                    Assert.assertEquals(0, job.keyedScanWholeRangeRowsForTest());
                    Assert.assertEquals(0, job.keyedScanCheaperCountForTest());
                    Assert.assertEquals(
                            "the skipped segment reads whole, which is what an unpriced one does",
                            correction,
                            job.keyedScanUnpricedCountForTest()
                    );
                    Assert.assertEquals(
                            "the repair itself is unchanged - the segment still replays whole",
                            correction,
                            job.segmentRepairCountForTest()
                    );
                }

                // The corrected anchor's running sum restarts at its own floor, so its three
                // rows carry 1, 2 and 3; the open anchor's two rows carry 1 and 2.
                assertNarrowAnchorTail(5, 9);
                assertNarrowAnchorViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentBelowTheKeyedSetupFloorWalksNoPostings() throws Exception {
        // The zero-row skip's general case. A keyed scan that can win opens at least one
        // partition's index and seeks it once per key, so at the shipped 256-row open price
        // one key costs at least 256 + 42 = 298 rows before it reads a posting. The corrected
        // day holds 41 rows, so the verdict is "read whole" before a partition is opened, and
        // pricing it anyway opens the day's partitions and walks the account's postings to
        // reach that same verdict.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(
                        "a segment at or below the keyed setup floor must not walk its keys' postings",
                        0,
                        job.keyedScanPostingRowsForTest()
                );
                Assert.assertEquals(0, job.keyedScanPricedCountForTest());
                Assert.assertEquals(0, job.keyedScanWholeRangeRowsForTest());
                Assert.assertEquals(0, job.keyedScanCheaperCountForTest());
                Assert.assertEquals(
                        "the skipped segment reads whole, which is what an unpriced one does",
                        1,
                        job.keyedScanUnpricedCountForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                // acct-1 carries its ten seeded rows and the correction, 1 to 11, and each
                // other account its ten seeded rows, 1 to 10: 66 + 3 * 55.
                assertQuery("""
                        SELECT count(), sum(cumulative_sum) FROM lv
                        WHERE created_at IN '2026-01-02'""")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tsum
                                41\t231.0
                                """);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testSparseKeyPricingStopsOnceItsSetupOutpricesTheWholeSegment() throws Exception {
        // A hundred new accounts corrected into the last hour of a closed day that spans
        // 24 hourly partitions. At the shipped 256-row open price each partition costs
        // 256 + 100 * 42 = 4_456 rows of setup before it yields a posting, which clears the
        // one-partition floor against the 6_100-row day, but two partitions cost 8_912 and
        // settle the verdict. The new keys hold no posting below the last hour, so a walk
        // that stops only on posting rows probes every key in all 24 partitions to reach
        // the verdict the second partition already reached, and only the last partition
        // yields the postings it counts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 4, "
                    + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            // 250 rows in each hour of 2026-01-02.
            execute("INSERT INTO tx SELECT "
                    + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 14_400_000), "
                    + "'filler'::SYMBOL, "
                    + "1.0 "
                    + "FROM long_sequence(6_000)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum FROM tx "
                    + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(3, 1, "filler"), job);
                Assert.assertEquals(0, job.keyedScanPricedCountForTest());

                execute("INSERT INTO tx SELECT "
                        + "timestamp_sequence('2026-01-02T23:30:00.000000Z', 1), "
                        + "('new-' || x)::SYMBOL, "
                        + "1.0 "
                        + "FROM long_sequence(100)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(1, job.keyedScanPricedCountForTest());
                Assert.assertEquals(6_100, job.keyedScanWholeRangeRowsForTest());
                Assert.assertEquals(0, job.keyedScanCheaperCountForTest());
                Assert.assertEquals(
                        "pricing must stop once the setup outprices the whole day, before it"
                                + " reaches the last hour's partition and the new keys' postings",
                        0,
                        job.keyedScanPostingRowsForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAZeroRowOpenSegmentCountsPostingsOnlyUpToTheRestoreBreakEven() throws Exception {
        // The open segment's resume shares the closed segments' blind spot wherever the
        // elapsed model has no root restore to weigh: its whole side then prices at nothing
        // in both models, and no keyed price undercuts nothing. A cold replay is the first
        // such case - it never consults the elapsed model. A resume that restores a root is
        // not: the override weighs the keyed count against that restore, which no row
        // estimate bounds. That count still has an end, though - the largest keyed price
        // the override grants against that restore. A count past it has lost the override
        // whatever the uncounted postings hold, so it stops there rather than walking every
        // posting the account holds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createNarrowAnchorView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // Below the only root, which the seed sealed at the 23:00:01.000500 frontier,
                // so the replay starts cold at the open anchor's origin. Its half-millisecond
                // interval estimates at zero rows of a partition spanning 23 hours.
                commit("('2026-01-02T23:00:01.000200Z', 'acct-1', 1.0)", job);

                Assert.assertEquals(
                        "a cold replay estimated at zero rows must not walk its keys' postings",
                        0,
                        job.openSegmentColdKeyedPostingRowsForTest()
                );
                Assert.assertEquals(0, job.openSegmentColdKeyedPricedCountForTest());
                Assert.assertEquals(1, job.openSegmentColdKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentColdKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentColdKeyedReplayCountForTest());

                // The cold replay leaves a root at the old 23:00:01.000500 frontier. An
                // in-order row moves the runtime past it, and a correction between the two
                // resumes from that root - restoring it, because the runtime no longer stands
                // there.
                commit("('2026-01-02T23:00:01.000700Z', 'acct-1', 1.0)", job);
                // One nanosecond per unit on every rate. The whole side is then the 40-byte
                // root's restore alone, 40ns, under an 85% hysteresis floor of 34ns. The
                // keyed side is a nanosecond per cost row plus one for the key's state, and
                // its 150% upper bound, ceil(1.5 * (cost + 1)), stays below 34 up to a cost
                // of 21 rows. A single key merges nothing, so its 22nd posting alone prices
                // the scan past that before any setup charge, and the count stops there -
                // at 22 of the 506 postings the account holds in the partition.
                viewInstance().getOpenSegmentRepairCost().setRatesForTest(1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
                commit("('2026-01-02T23:00:01.000600Z', 'acct-1', 1.0)", job);

                Assert.assertEquals(1, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedWholeRangeRowsForTest());
                Assert.assertEquals(
                        "a restore-bearing resume estimated at zero rows must stop counting at the"
                                + " override's break-even",
                        22,
                        job.openSegmentKeyedPostingRowsForTest()
                );
                // The route the full count picked: the override declines, and the resume
                // reads the whole range off the restored root.
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentRestoreAwareCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.runtimeAnchorReuseCountForTest());

                // The open anchor now holds five rows: 23:00:01.000000, .000200, .000500,
                // .000600 and .000700.
                assertNarrowAnchorTail(6, 16);
                assertNarrowAnchorViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAReusableHeadEstimatedAtZeroRowsWalksNoPostings() throws Exception {
        // The other resume with no root to restore. Intra-commit O3 wholly above the sealed
        // head lets no row into the window pipeline before detection, so the selected anchor
        // is the runtime itself and the elapsed model prices the whole side at its scan
        // alone - zero rows, and nothing for a keyed price to undercut.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createNarrowAnchorView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                commit(
                        "('2026-01-02T23:00:01.000900Z', 'acct-1', 1.0), "
                                + "('2026-01-02T23:00:01.000800Z', 'acct-1', 1.0)",
                        job
                );

                Assert.assertEquals(1, job.runtimeAnchorReuseCountForTest());
                Assert.assertEquals(
                        "a reusable resume estimated at zero rows must not walk its keys' postings",
                        0,
                        job.openSegmentKeyedPostingRowsForTest()
                );
                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(1, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());

                // The open anchor holds 23:00:01.000000, .000500, .000800 and .000900.
                assertNarrowAnchorTail(5, 11);
                assertNarrowAnchorViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARootRestoreNoKeyedPriceUndercutsWalksNoPostings() throws Exception {
        // A zero-row resume that does restore a root, but one too small for any keyed price
        // to win the override. Under the cold priors the 40-byte root restores in 240ns,
        // under an 85% hysteresis floor of 204ns, while the transplant of the one key's
        // state alone prices the keyed side at 5_000ns - 7_500ns at its 150% upper bound -
        // before the scan adds a row. The override's break-even lies below a cost of zero,
        // so there is nothing to count up to.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createNarrowAnchorView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // An in-order row moves the runtime past the root the seed sealed at the
                // 23:00:01.000500 frontier, and a correction between the two resumes from
                // that root - restoring it, because the runtime no longer stands there.
                commit("('2026-01-02T23:00:01.000700Z', 'acct-1', 1.0)", job);
                commit("('2026-01-02T23:00:01.000600Z', 'acct-1', 1.0)", job);

                Assert.assertEquals(0, job.runtimeAnchorReuseCountForTest());
                Assert.assertEquals(
                        "a resume whose restore no keyed price undercuts must not walk its keys' postings",
                        0,
                        job.openSegmentKeyedPostingRowsForTest()
                );
                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(1, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentRestoreAwareCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());

                // The open anchor holds 23:00:01.000000, .000500, .000600 and .000700.
                assertNarrowAnchorTail(5, 11);
                assertNarrowAnchorViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARootRestoreThatOutweighsTheKeyedScanStillOverridesAZeroRowEstimate() throws Exception {
        // The other side of the break-even budget: it may stop a count only where the
        // override was lost anyway. A restore that dwarfs the keyed scan puts the break-even
        // far above every posting the account holds, so the count runs to its total and the
        // override takes the keyed route, exactly as an unbounded count does.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // The identity the keyed resume's publication upserts on. It is a CREATE-time schema
        // property, so it has to be on before the view exists.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createNarrowAnchorView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                commit("('2026-01-02T23:00:01.000700Z', 'acct-1', 1.0)", job);
                // A second per restored byte and a nanosecond per unit elsewhere: the 40-byte
                // root restores in 40s, which leaves the override's break-even in the tens of
                // billions of cost rows.
                viewInstance().getOpenSegmentRepairCost().setRatesForTest(1_000_000_000L, 1, 1, 1, 1, 1, 1, 1, 1, 1);
                commit("('2026-01-02T23:00:01.000600Z', 'acct-1', 1.0)", job);

                Assert.assertEquals(1, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedWholeRangeRowsForTest());
                Assert.assertEquals(
                        "a count below the break-even must reach every posting the account holds",
                        505,
                        job.openSegmentKeyedPostingRowsForTest()
                );
                Assert.assertEquals(
                        "the row verdict cannot prefer a keyed scan over a zero-row estimate",
                        0,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                Assert.assertEquals(
                        "the restore must still override it",
                        1,
                        job.openSegmentRestoreAwareCheaperCountForTest()
                );
                Assert.assertEquals(1, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.runtimeAnchorReuseCountForTest());

                // The open anchor holds 23:00:01.000000, .000500, .000600 and .000700.
                assertNarrowAnchorTail(5, 11);
                assertNarrowAnchorViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnindexedKeyIsNeverPriced() throws Exception {
        // Without an index there is nothing to name one key's rows with, so the question is
        // not asked at all - and the repair is exactly the one this view has today.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(0, job.keyedScanPricedCountForTest());
                Assert.assertEquals(0, job.keyedScanUnpricedCountForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                Assert.assertEquals(
                        "an unindexed SYMBOL key names no column a repair could seek through",
                        -1,
                        keyProjector().getIndexedSymbolColumnIndex()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnpriceableColumnLeavesBothSetupCountsAtZero() throws Exception {
        // The estimate abandons a column it cannot get an index reader for and reports
        // UNPRICEABLE, which the caller reads as "take the whole range". Both halves of the
        // setup term have to come back to zero on that path: getIndexOpens() and
        // getIndexSeeks() are public, and a count left over from the partitions walked
        // before the throw would price the next question with the last one's arithmetic.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE, "
                    + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("INSERT INTO tx SELECT "
                    + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 86_400_000), "
                    + "('acct-' || (x % 8))::symbol, "
                    + "x::double "
                    + "FROM long_sequence(2_000)");
            drainWalQueue();

            final IntList keys = new IntList();
            final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                keys.add(reader.getSymbolMapReader(1).keyOf("acct-3"));
                cost.of(reader, sqlExecutionContext);
                Assert.assertEquals(
                        "an unindexed column names no postings to follow",
                        LiveViewCheckpointKeyedScanCost.UNPRICEABLE,
                        cost.estimateKeyedScanRows(
                                ts("2026-01-02T00:00:00.000000Z"),
                                ts("2026-01-03T23:59:59.999999Z"),
                                1,
                                keys,
                                Long.MAX_VALUE
                        )
                );
                Assert.assertEquals(0, cost.getIndexOpens());
                Assert.assertEquals(0, cost.getIndexSeeks());
                Assert.assertEquals(0, cost.getPostingRows());
            }
        });
    }

    @Test
    public void testHotKeyPricingPrefersTheWholeSegment() throws Exception {
        // The case the cost model exists for. Pricing off affectedKeys * averageRowsPerKey
        // would call one key of four a quarter of the segment; the posting lists say this
        // key holds most of it, and the merge and setup terms put it over the top.
        Assert.assertFalse(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(900, 1, 4, 1, 1_000, 256, 42));
        // A sparse domain the setup dominates: 40 rows behind ten partition opens and 200
        // per-key-per-frame seeks is 11,200 row-equivalents against a 1,000-row segment.
        Assert.assertFalse(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(40, 10, 200, 20, 1_000, 256, 42));
        // And the shape it is there to admit: a few keys, few rows, one partition.
        Assert.assertTrue(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(40, 1, 2, 2, 10_000, 256, 42));
        // An unpriceable estimate reads as expensive rather than as free.
        Assert.assertFalse(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                LiveViewCheckpointKeyedScanCost.UNPRICEABLE, 0, 0, 1, Long.MAX_VALUE, 256, 42));
        // No term may wrap: a saturated count has to stay the most expensive answer.
        Assert.assertEquals(
                Long.MAX_VALUE,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(
                        Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 4096, 256, 42)
        );
        // One key costs the row and nothing more, so the merge term never charges a
        // single-key scan for a merge it never runs - and the two setup counts are priced
        // apart, an open at the configured price and a seek at a sixth of it.
        Assert.assertEquals(
                100 + 256 + 2 * 42,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(100, 1, 2, 1, 256, 42)
        );
        // A seek is a pooled cursor off an open reader, not a second open. The measured pair
        // is ~15us against ~124-165ns, nearer 90:1 than 6:1, and the divisor deliberately
        // does not carry that ratio over: it is a policy choice that prices the seek off the
        // one configured knob rather than off a second knob. What the measurement does fix is
        // the derived seek's magnitude at the shipped default - a sixth of 256 is 42 base
        // rows, the top of the measured 33-44 warm-seek band - which is what the second
        // assertion below pins.
        Assert.assertEquals(6, LiveViewCheckpointKeyedScanCost.INDEX_SEEKS_PER_INDEX_OPEN);
        Assert.assertEquals(42, LiveViewCheckpointKeyedScanCost.indexSeekRows(256));
        // A configured zero disables the whole setup term; the derived seek must not smuggle
        // it back in, and a price too small to divide must not round away to free.
        Assert.assertEquals(0, LiveViewCheckpointKeyedScanCost.indexSeekRows(0));
        Assert.assertEquals(0, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(100, 8, 64, 1, 0, 0) - 100);
        Assert.assertEquals(1, LiveViewCheckpointKeyedScanCost.indexSeekRows(1));
        Assert.assertEquals(1, LiveViewCheckpointKeyedScanCost.indexSeekRows(5));
    }

    @Test
    public void testTheMergeChargeGrowsWithTheLogarithmOfTheKeyCount() {
        // HeapRowCursor merges through IntLongSortedList, an array-backed binary min-heap:
        // pollAndReplace sifts the replacement down from the root in at most floor(log2 |Q|)
        // steps, whatever order the keys' rows interleave in. The per-row merge charge
        // therefore grows with the logarithm of |Q| and never with |Q| itself.

        // First, the one number this route was ever measured at. Commit 5ce0d8172a records
        // it: "On the worked shape - an 800k-row daily partition, eight shared query
        // workers, 256 keys - ... the corrected model prices 446_272 against a measured
        // cost near 450_000." That is 40_000 posting rows behind one partition open and
        // eight frames of 256 seeks, at the shipped 256-row open price.
        final long measuredShapePrice = LiveViewCheckpointKeyedScanCost.keyedScanCostRows(
                40_000, 1, 8 * 256, 256, 256, 42);
        Assert.assertEquals(446_272, measuredShapePrice);
        Assert.assertTrue(
                "the model has to price the one shape this route was measured on within a tenth"
                        + " of that measurement, and prices it " + measuredShapePrice,
                Math.abs(measuredShapePrice - 450_000) * 10 < 450_000
        );
        Assert.assertTrue(
                "the shape the route was measured on still prices below the whole range",
                LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                        40_000, 1, 8 * 256, 256, 800_000, 256, 42)
        );

        // Second, the charge itself, read with one posting row and the setup term priced at
        // zero: one row for the read plus ceil(log2 |Q|) for the heap. The loop derives the
        // logarithm by doubling rather than through the bit arithmetic the model uses, so a
        // shared slip cannot pass it, and it spans the 321 to 100_000 keys a linear charge
        // used to over-price.
        for (int keyCount = 1; keyCount <= 100_000; keyCount++) {
            int ceilLog2 = 0;
            while ((1L << ceilLog2) < keyCount) {
                ceilLog2++;
            }
            Assert.assertEquals(
                    "the merge charge at " + keyCount + " keys",
                    1 + ceilLog2,
                    LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, keyCount, 0, 0)
            );
        }
        // A single-key scan merges nothing, and a degenerate key count must neither go below
        // the row it reads nor wrap at the top of the int range.
        Assert.assertEquals(1, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 0, 0, 0));
        Assert.assertEquals(1, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, -1, 0, 0));
        Assert.assertEquals(1, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 1, 0, 0));
        Assert.assertEquals(2, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 2, 0, 0));
        Assert.assertEquals(9, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 256, 0, 0));
        Assert.assertEquals(10, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 321, 0, 0));
        Assert.assertEquals(18, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 100_000, 0, 0));
        Assert.assertEquals(
                32,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, Integer.MAX_VALUE, 0, 0)
        );
        Assert.assertFalse(
                "a keyed scan over no key has nothing to follow",
                LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(1, 0, 0, 0, 1_000, 256, 42)
        );

        // Third, the growth. Doubling a key domain adds one level to the heap, so it adds
        // one row-equivalent to the charge rather than doubling it.
        Assert.assertEquals(
                "doubling the key domain adds one heap level to the merge charge",
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 1_024, 0, 0) + 1,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(1, 0, 0, 2_048, 0, 0)
        );

        // And the verdicts it decides. The worked shape over 4_096 keys, at the open
        // segment's capped setup price of four rows for both halves: 40_000 * 13 + 4 +
        // 32_768 * 4 is 651_076 against 800_000, so the keyed route wins.
        Assert.assertEquals(
                651_076,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(40_000, 1, 8 * 4_096, 4_096, 4, 4)
        );
        Assert.assertTrue(
                "a 4_096-key heap merge over 40_000 rows reads less than the 800_000-row range",
                LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                        40_000, 1, 8 * 4_096, 4_096, 800_000, 4, 4)
        );
        // A closed segment at the shipped 256-row open price: 1_000 keys holding 50_000 rows
        // of a 1_000_000-row single-frame partition cost 50_000 * 11 + 256 + 1_000 * 42 =
        // 592_256. An indexed scan of that shape measured 2.02 ms against 5.15 ms for the
        // whole range, so the keyed route has to win here too.
        Assert.assertEquals(
                592_256,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(50_000, 1, 1_000, 1_000, 256, 42)
        );
        Assert.assertTrue(
                "a 1_000-key heap merge over 50_000 rows reads less than the 1_000_000-row range",
                LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                        50_000, 1, 1_000, 1_000, 1_000_000, 256, 42)
        );
    }

    @Test
    public void testTheEstimateChargesOneIndexOpenPerPartitionAndOneSeekPerKeyPerPageFrame() throws Exception {
        // The setup term is two counts because the scan does two things. TableReader caches
        // one index reader per (partition, column, direction) and hands it to every key of
        // every page frame, so an open is per partition and is independent of both. What
        // HeapRowCursorFactory rebuilds per (key, frame) is a seek of that already-open
        // reader - a pooled cursor and a block-chain walk, not two file opens and two mmaps.
        // A partition wider than the frame limit is where the two counts part company, and
        // pricing the seek as though it were an open is what wrongly declined the route.
        assertMemoryLeak(() -> {
            // Narrower than the configured default, which no partition this fixture can
            // afford to write would reach. changePageFrameSizes rather than a property: the
            // shared execution context caches the configured pair when it is constructed,
            // which is before a test body runs.
            sqlExecutionContext.changePageFrameSizes(100, 100);
            try {
                runTheEstimateChargesOneIndexOpenPerPartitionAndOneSeekPerKeyPerPageFrame();
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testTheForwardIndexedCursorYieldsExactlyTheNamedKeysRows() throws Exception {
        // The cursor's own contract: the subsequence of the full forward scan whose key is
        // one of the named ones, in the same order, over the same inclusive bounds - and the
        // other keys' rows not at all.
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                    + "amount double) timestamp(created_at) partition by hour wal");
            final StringBuilder rows = new StringBuilder();
            for (int hour = 0; hour < 12; hour++) {
                for (int account = 1; account <= 4; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(2, hour, "acct-" + account));
                }
            }
            execute("insert into tx values " + rows);
            drainWalQueue();

            final IntList keys = new IntList();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                keys.add(reader.getSymbolMapReader(1).keyOf("acct-2"));
                keys.add(reader.getSymbolMapReader(1).keyOf("acct-4"));
            }
            final long lowTs = ts("2026-01-02T02:00:00.000000Z");
            final long highTs = ts("2026-01-02T09:00:00.000000Z");

            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("tx")) {
                // SqlCompiler wraps every compiled query in a QueryProgress factory for
                // registry tracking; the scan underneath is what carries the substitution.
                RecordCursorFactory scan = factory;
                while (scan != null && !(scan instanceof PageFrameRecordCursorFactory)) {
                    scan = scan.getBaseFactory();
                }
                Assert.assertTrue(
                        "a plain full scan is what the substitution needs",
                        scan instanceof PageFrameRecordCursorFactory
                );
                final PageFrameRecordCursorFactory pageFrameFactory = (PageFrameRecordCursorFactory) scan;
                Assert.assertTrue(pageFrameFactory.isIndexedForwardTimestampRangeSupported(1));
                try (RecordCursor cursor = pageFrameFactory.getCursorInTimestampRangeForwardIndexed(
                        sqlExecutionContext, lowTs, highTs, 1, keys)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        actual.putISODate(record.getTimestamp(0)).putAscii('\t').put(record.getSymA(1)).putAscii('\n');
                    }
                }
            }

            final StringSink expected = new StringSink();
            try (
                    RecordCursorFactory factory = select("select created_at, account_id from tx "
                            + "where account_id in ('acct-2', 'acct-4') "
                            + "and created_at between '2026-01-02T02:00:00.000000Z' "
                            + "and '2026-01-02T09:00:00.000000Z' order by created_at");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    expected.putISODate(record.getTimestamp(0)).putAscii('\t').put(record.getSymA(1)).putAscii('\n');
                }
            }
            TestUtils.assertEquals(expected, actual);
            Assert.assertEquals(16, countLines(actual));

            // And the estimate has to name that same number. The interval covers eight
            // whole hourly partitions, so nothing here is interpolated and the count is
            // exact - which is what makes it an assertion rather than a sanity check. It is
            // also what catches the index's key space: the postings are keyed by
            // symbolKey + 1, so a table-local key passed straight through would count the
            // neighbouring account's rows and still look plausible.
            final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                cost.of(reader, sqlExecutionContext);
                Assert.assertEquals(16, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, Long.MAX_VALUE));
                Assert.assertEquals(16, cost.getPostingRows());
                // Eight hourly partitions, so eight index opens - two keys and one frame
                // each share the reader TableReader caches per partition.
                Assert.assertEquals(8, cost.getIndexOpens());
                // And two keys across those eight partitions, each of them four rows and so
                // a single page frame, which is what HeapRowCursorFactory rebuilds a row
                // cursor per key for and what the seek half of the setup term is charged for.
                Assert.assertEquals(16, cost.getIndexSeeks());
            }
        });
    }

    @Test
    public void testTheForwardIndexedScanReleasesTheRowCursorsAMidBuildFailureStrands() throws Exception {
        // HeapRowCursorFactory.getCursor() builds one index-backed row cursor per key into a
        // list it hands to its HeapRowCursor only after the last one, and
        // PageFrameRecordCursorImpl.hasNext() nulls its own rowCursor before it makes that
        // call - so a build that throws part way through leaves the cursors it had already
        // built reachable through the heap factory alone. A POSTING row cursor holds a
        // native block buffer that only its own close() releases, and the index reader's
        // close() reaches only the cursors that made it back into its free list, so nothing
        // else ever reclaims one the heap stranded.
        //
        // The throw is a native out-of-memory, which a server arms in production from
        // ram.usage.limit.* through Unsafe.setRssMemLimit (Bootstrap:232). This walks an RSS
        // ceiling across the build's allocation points: a ceiling that lets an earlier key's
        // cursor allocate and trips a later one's is what strands the earlier one, so the
        // sweep has to cross the whole failing-to-succeeding transition rather than only
        // fail at the bottom of the range. The enclosing assertMemoryLeak is what observes
        // the strand.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX TYPE POSTING, "
                    + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
            // Two properties of the seed carry the case. The accounts land irregularly, so a
            // key's row ids are not a constant stride and the reader decodes them through a
            // block buffer rather than the constant-delta path that allocates nothing. And
            // there are more keys than IndexReader.MAX_CACHED_FREE_CURSORS, so the reader's
            // free list can never satisfy a whole frame's build: every open still creates
            // fresh cursors, and a fresh cursor is the one that allocates. The cursors the
            // build had already taken when the fault lands - popped from the pool or fresh -
            // are what it strands, and each of them holds a buffer.
            execute("INSERT INTO tx SELECT "
                    + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 15_000_000), "
                    + "('acct-' || rnd_int(0, 199, 0))::symbol, "
                    + "x::double "
                    + "FROM long_sequence(3_200)");
            drainWalQueue();

            final IntList keys = new IntList();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                for (int account = 0; account < 200; account++) {
                    final int key = reader.getSymbolMapReader(1).keyOf("acct-" + account);
                    Assert.assertTrue("every seeded account must resolve to a table-local key", key >= 0);
                    keys.add(key);
                }
            }

            final long lowTs = ts("2026-01-02T00:00:00.000000Z");
            final long highTs = ts("2026-01-02T23:59:59.999999Z");

            // Single-threaded and parallel. The shared query worker count is one of the two
            // inputs to the page frame width, so it decides how many times one open rebuilds
            // the row cursors whose ownership this case is about.
            try (SqlExecutionContextImpl executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                sweepMidBuildFailures(executionContext, lowTs, highTs, keys);
            }
            try (SqlExecutionContextImpl executionContext = TestUtils.createSqlExecutionCtx(engine, 4)) {
                sweepMidBuildFailures(executionContext, lowTs, highTs, keys);
            }
        });
    }

    @Test
    public void testTheKeyedRouteFollowsTheSharedQueryWorkerCount() throws Exception {
        // The blind spot every other repair-route case shares: they all drive a job at
        // sharedQueryWorkerCount = 1. The count reaches the estimate only through
        // calculatePageFrameRowLimit, which divides a partition's rows by the worker count and
        // then clamps the result into [pageFrameMinRows, pageFrameMaxRows] - so on a fixture
        // whose partitions hold tens of rows the 1_000-row floor pins the split at one frame and
        // raising the knob asserts nothing at all. That is why counting the setup term per
        // partition rather than per page frame went unnoticed.
        //
        // This case seeds one anchor day 4_000 rows deep, four times the floor, so the same
        // partition really is one page frame at one worker and four at four. Everything else is
        // held equal and pinned by assertion: the same 501 posting rows, the same 4_001-row
        // whole-range read, one key, one priced segment. The only input that moves is the worker
        // count, and the route the repair prices has to move with it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        // Pinned rather than inherited, because the fixture's row count is sized against them:
        // 4_000 rows over four workers is exactly the floor, so the split is 4 there and 1 at one
        // worker. Left to the harness default, a later change to it would retune this case
        // silently.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        // Only the SEEK half of the setup term follows the frame split - the open half is one
        // per partition either way - so the knob has to be set high enough that the seek alone
        // spans the whole-range read. At 2_400 rows an open the derived seek is 400, and the day
        // is one partition: 501 + 2_400 + 1 * 400 = 3_301 is below the 4_001-row whole-range
        // read, while 501 + 2_400 + 4 * 400 = 4_501 is above it. A knob low enough that the seek
        // term cannot span 4_001 on its own would leave the worker count unable to move the
        // verdict and the case asserting nothing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 2_400);
        assertMemoryLeak(() -> {
            Assert.assertEquals(
                    "at one worker the day is a single page frame, so one index seek on top of the"
                            + " partition's open prices the keyed read below the whole-range one",
                    1,
                    driveWideSegmentRepair(1, 1)
            );
            Assert.assertEquals(
                    "at four workers the same day splits four ways, and four index seeks on top of"
                            + " the same single open outprice the whole-range read",
                    0,
                    driveWideSegmentRepair(4, 4)
            );
        });
    }

    @Test
    public void testTheSharedKeyProjectorNamesTheIndexedSymbolColumn() throws Exception {
        // The identity every function on the view shares, and the one thing a keyed repair
        // needs off it that the two sinks do not carry: which base column the index is on.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewCheckpointKeyProjector projector = keyProjector();
                Assert.assertEquals(1, projector.getPartitionByColumnCount());
                Assert.assertEquals(1, projector.getPartitionByColumnIndex(0));
                Assert.assertEquals(1, projector.getIndexedSymbolColumnIndex());
                Assert.assertNotNull(projector.getKeySink());
                Assert.assertNotNull(projector.getCheckpointKeySink());
                Assert.assertNotSame(
                        "a SYMBOL key column needs a second sink writing its resolved string",
                        projector.getKeySink(),
                        projector.getCheckpointKeySink()
                );
            }
        });
    }

    @Test
    public void testABudgetedEstimateSaysWhenItStoppedShortOfThePostingsItPrices() throws Exception {
        // The budget answers one question and only one: whether the keyed side already
        // reads more than the whole range. Every other consumer needs to know that a count
        // which reached it is a floor - every key below the stop and every partition above
        // it is missing from the posting rows, the index opens and the index seeks alike -
        // because the figure understates the keyed route by the most exactly where that
        // route is most expensive.
        assertMemoryLeak(() -> {
            seedEightAccountsOverTwoDays();

            final long lowTs = ts("2026-01-02T00:00:00.000000Z");
            final long highTs = ts("2026-01-03T23:59:59.999999Z");
            final long firstDayHighTs = ts("2026-01-02T23:59:59.999999Z");
            final IntList keys = new IntList();
            final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                keys.add(reader.getSymbolMapReader(1).keyOf("acct-3"));
                cost.of(reader, sqlExecutionContext);

                Assert.assertEquals(250, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, Long.MAX_VALUE));
                Assert.assertFalse(
                        "an unbounded count visited every partition and every key, so it is a total",
                        cost.isSaturated()
                );
                Assert.assertEquals(2, cost.getIndexOpens());

                // A budget the first day's 125 postings reach on their own, which leaves the
                // second day unopened, unsought and uncounted.
                Assert.assertEquals(125, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, 125));
                Assert.assertTrue(
                        "the walk stopped with a partition of this key's rows still ahead of it",
                        cost.isSaturated()
                );
                Assert.assertEquals(
                        "and the setup term stopped with it - one open, not the two the scan pays",
                        1,
                        cost.getIndexOpens()
                );
                Assert.assertEquals(125, cost.getPostingRows());

                // The same stop, with nothing behind it: the budget lands on the last key of
                // the last partition, and the key's own postings are exhausted. Nothing is
                // missing from the figures, so nothing may be reported as missing - a stop
                // is not by itself an understatement.
                Assert.assertEquals(250, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, 250));
                Assert.assertFalse(
                        "a count that stopped having already reached every posting is a total",
                        cost.isSaturated()
                );
                Assert.assertEquals(2, cost.getIndexOpens());

                // One partition and one key, so the outer walk has neither a key nor a
                // partition left to skip when it stops. Where the index reports no size the
                // count is still short, and the key's own truncated walk is the only thing
                // that can say so.
                final boolean isKeySizedByTheIndex = IndexType.isPosting(configuration.getDefaultSymbolIndexType());
                final long truncated = cost.estimateKeyedScanRows(lowTs, firstDayHighTs, 1, keys, 100);
                Assert.assertEquals(isKeySizedByTheIndex ? 125 : 100, truncated);
                Assert.assertEquals(
                        "a bitmap index reports no size, so this key's postings are walked and"
                                + " the walk is what the budget cut short; a posting index sizes"
                                + " the key exactly and loses nothing",
                        !isKeySizedByTheIndex,
                        cost.isSaturated()
                );
                Assert.assertEquals(1, cost.getIndexOpens());
            }
        });
    }

    @Test
    public void testABudgetedEstimateSaysWhenItStoppedWithKeysStillAheadOfIt() throws Exception {
        // The other half of the stop, and the one a single-key fixture cannot reach: the
        // walk can end inside a partition with keys of that same partition still unvisited,
        // and their postings are as absent from the three figures as an unopened
        // partition's. One key and two keys over the same rows, the same range and the same
        // budget are what separate the two - the one-key run stops having counted every
        // posting there is, the two-key run stops with a whole key ahead of it.
        assertMemoryLeak(() -> {
            seedEightAccountsOverTwoDays();

            final long lowTs = ts("2026-01-02T00:00:00.000000Z");
            // The first day alone. No partition stands above the stop, so the keys are the
            // only thing the walk can leave uncounted.
            final long firstDayHighTs = ts("2026-01-02T23:59:59.999999Z");
            final IntList oneKey = new IntList();
            final IntList twoKeys = new IntList();
            final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                final int firstKey = reader.getSymbolMapReader(1).keyOf("acct-3");
                oneKey.add(firstKey);
                twoKeys.add(firstKey);
                twoKeys.add(reader.getSymbolMapReader(1).keyOf("acct-5"));
                cost.of(reader, sqlExecutionContext);

                // The budget is exactly the first key's postings in the range's only
                // partition, so both runs stop on the same row of the same key.
                Assert.assertEquals(125, cost.estimateKeyedScanRows(lowTs, firstDayHighTs, 1, oneKey, 125));
                Assert.assertFalse(
                        "the only key of the only partition was counted whole, so this is a total",
                        cost.isSaturated()
                );

                Assert.assertEquals(125, cost.estimateKeyedScanRows(lowTs, firstDayHighTs, 1, twoKeys, 125));
                Assert.assertTrue(
                        "the second key's 125 postings are uncounted, so this is a floor",
                        cost.isSaturated()
                );
                Assert.assertEquals(125, cost.getPostingRows());
                Assert.assertEquals(1, cost.getIndexOpens());
            }
        });
    }

    @Test
    public void testASetupBudgetedEstimateStopsAheadOfThePartitionThatSettlesTheVerdict() throws Exception {
        // One key whose only posting sits in the last of four single-frame partitions, priced
        // at 100 rows per open and 100 per seek against a 401-row budget. Its posting count
        // stays at zero for three partitions, so the posting budget alone walks all four,
        // while the setup of two partitions prices at 400 and of three at 600.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(100, 100);
            try {
                execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 4, "
                        + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY HOUR WAL");
                // 100 rows in each of the first four hours of 2026-01-02, and one more row in
                // the fourth, which is the key's only posting.
                execute("INSERT INTO tx SELECT "
                        + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 36_000_000), "
                        + "'filler'::SYMBOL, "
                        + "1.0 "
                        + "FROM long_sequence(400)");
                execute("INSERT INTO tx VALUES ('2026-01-02T03:30:00.000000Z', 'late', 1.0)");
                drainWalQueue();

                final long lowTs = ts("2026-01-02T00:00:00.000000Z");
                final long highTs = ts("2026-01-02T03:59:59.999999Z");
                final IntList keys = new IntList();
                final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
                try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                    keys.add(reader.getSymbolMapReader(1).keyOf("late"));
                    cost.of(reader, sqlExecutionContext);

                    Assert.assertEquals(1, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, 401));
                    Assert.assertEquals(4, cost.getIndexOpens());
                    Assert.assertEquals(4, cost.getIndexSeeks());
                    Assert.assertFalse(cost.isSaturated());

                    Assert.assertEquals(0, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, 401, 100, 100));
                    Assert.assertEquals(
                            "the walk must stop on the third partition's setup, and charge it",
                            3,
                            cost.getIndexOpens()
                    );
                    Assert.assertEquals(3, cost.getIndexSeeks());
                    Assert.assertEquals(0, cost.getPostingRows());
                    Assert.assertTrue(
                            "the last partition's posting is uncounted, so the figures are floors",
                            cost.isSaturated()
                    );
                    Assert.assertEquals(
                            600,
                            LiveViewCheckpointKeyedScanCost.keyedScanCostRows(0, 3, 3, 1, 100, 100)
                    );
                    Assert.assertFalse(
                            "the stopped figures must read as not cheaper, as the full count does",
                            LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(0, 3, 3, 1, 401, 100, 100)
                    );

                    // The full price is 1 + 4 * 100 + 4 * 100 = 801, so a budget above it does
                    // not cut the walk short, and the keyed route still wins.
                    Assert.assertEquals(1, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, 802, 100, 100));
                    Assert.assertEquals(4, cost.getIndexOpens());
                    Assert.assertEquals(4, cost.getIndexSeeks());
                    Assert.assertFalse(cost.isSaturated());
                    Assert.assertTrue(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(1, 4, 4, 1, 802, 100, 100));

                    // A zero price disables the setup term, and with it the setup's bound.
                    Assert.assertEquals(1, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, 401, 0, 0));
                    Assert.assertEquals(4, cost.getIndexOpens());
                    Assert.assertEquals(4, cost.getIndexSeeks());
                    Assert.assertFalse(cost.isSaturated());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    private long count(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private static PageFrameRecordCursorFactory pageFrameScanOf(RecordCursorFactory factory) {
        // SqlCompiler wraps every compiled query in a QueryProgress factory for registry
        // tracking; the scan underneath is what carries the substitution.
        RecordCursorFactory scan = factory;
        while (scan != null && !(scan instanceof PageFrameRecordCursorFactory)) {
            scan = scan.getBaseFactory();
        }
        Assert.assertTrue(
                "a plain full scan is what the substitution needs",
                scan instanceof PageFrameRecordCursorFactory
        );
        return (PageFrameRecordCursorFactory) scan;
    }

    private static int countLines(StringSink sink) {
        int lines = 0;
        for (int i = 0, n = sink.length(); i < n; i++) {
            if (sink.charAt(i) == '\n') {
                lines++;
            }
        }
        return lines;
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute("tx", "lv");
    }

    private void assertViewMatchesRecompute(String baseName, String viewName) throws Exception {
        assertViewMatchesRecompute(
                baseName,
                viewName,
                "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)"
        );
    }

    private void assertViewMatchesRecompute(String baseName, String viewName, String bucket) throws Exception {
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from " + baseName + ")";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(" + viewName + ") order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    private void assertNarrowAnchorTail(int expectedCount, int expectedSum) throws Exception {
        assertQuery("""
                SELECT count(), sum(cumulative_sum) FROM lv
                WHERE created_at >= '2026-01-02T23:00:00.000000Z'""")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\tsum\n" + expectedCount + "\t" + expectedSum + ".0\n");
    }

    private void assertNarrowAnchorViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute("tx", "lv", NARROW_ANCHOR);
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Opens the forward index-backed scan over {@code keys} the way a repair does, pulls
     * every row and closes both the cursor and the factory, and returns the row count.
     */
    private int drainKeyedScan(SqlExecutionContextImpl executionContext, long lowTs, long highTs, IntList keys) throws Exception {
        int rows = 0;
        try (RecordCursorFactory factory = select("tx", executionContext)) {
            try (RecordCursor cursor = pageFrameScanOf(factory).getCursorInTimestampRangeForwardIndexed(
                    executionContext, lowTs, highTs, 1, keys)) {
                while (cursor.hasNext()) {
                    rows++;
                }
            }
        }
        return rows;
    }

    /**
     * Walks an RSS ceiling across one keyed open's allocation points, so that each point
     * faults a different one of them, and asserts the sweep crossed the whole
     * failing-to-succeeding transition rather than only failing at the bottom of the range.
     * What each point leaves behind is the caller's {@code assertMemoryLeak} to judge.
     */
    private void sweepMidBuildFailures(SqlExecutionContextImpl executionContext, long lowTs, long highTs, IntList keys) throws Exception {
        // Warm the reader and the compiler with the ceiling down, so the swept failure lands
        // inside the keyed open rather than in first-touch table open.
        Assert.assertEquals(3_200, drainKeyedScan(executionContext, lowTs, highTs, keys));

        boolean hasRunUnderLimit = false;
        int maxOomSlack = -1;
        for (int slack = 0; slack <= OOM_SWEEP_SLACK_MAX; slack += OOM_SWEEP_SLACK_STEP) {
            // A fresh factory per point. Reusing one would let a later successful open free,
            // through getCursor()'s own leading drain, whatever an earlier faulted one
            // stranded - which is exactly what would mask the leak.
            try (RecordCursorFactory factory = select("tx", executionContext)) {
                final PageFrameRecordCursorFactory scan = pageFrameScanOf(factory);
                RecordCursor cursor = null;
                // Arm immediately before the operation under test.
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                try {
                    cursor = scan.getCursorInTimestampRangeForwardIndexed(
                            executionContext, lowTs, highTs, 1, keys);
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // Pull every row; the heap rebuilds its row cursors per frame.
                    }
                    hasRunUnderLimit = true;
                } catch (CairoException e) {
                    Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                    maxOomSlack = slack;
                } finally {
                    // Disarm before the cursor and the factory close, so neither trips the
                    // ceiling. The cursor cannot be a try-with-resources here: an extended
                    // try-with-resources closes its resource before this finally runs, which
                    // would hold the ceiling armed across close().
                    Unsafe.setRssMemLimit(0);
                    Misc.free(cursor);
                }
            }
        }
        // The two together bracket the build's allocation span. At slack 0 the ceiling equals
        // current usage, so the open's first tracked allocation fails and an OOM alone only
        // shows the open allocates at all; pairing it with an open that survived its ceiling
        // is what shows the sweep crossed the transition the strand hides in.
        Assert.assertTrue("the keyed open only failed at the zero-slack endpoint", maxOomSlack > 0);
        Assert.assertTrue("the sweep never completed a keyed open under an armed ceiling, so it "
                        + "stopped short of the transition the strand hides in; widen OOM_SWEEP_SLACK_MAX",
                hasRunUnderLimit);

        // Recovery: with the ceiling removed the same scan reads its rows cleanly.
        Assert.assertEquals(3_200, drainKeyedScan(executionContext, lowTs, highTs, keys));
    }

    /**
     * How many page frames the executor really crosses over {@code tableName} at
     * {@code sharedQueryWorkerCount}, taken off the same forward page frame cursor a keyed scan
     * opens - not off a second copy of the formula the estimate uses, which would share any bug
     * with it.
     */
    private long countPageFrames(String tableName, int sharedQueryWorkerCount) throws Exception {
        long frames = 0;
        try (
                SqlExecutionContextImpl executionContext = TestUtils.createSqlExecutionCtx(engine, sharedQueryWorkerCount);
                RecordCursorFactory factory = select(tableName, executionContext);
                PageFrameCursor cursor = pageFrameScanOf(factory).getPageFrameCursor(
                        executionContext,
                        PartitionFrameCursorFactory.ORDER_ASC
                )
        ) {
            while (cursor.next() != null) {
                frames++;
            }
        }
        return frames;
    }

    /**
     * Seeds one anchor day 4_000 rows deep over eight accounts, closes it with a row of the next
     * day, corrects one account inside it and drives the whole repair at
     * {@code sharedQueryWorkerCount}.
     * <p>
     * Everything the keyed price is made of except the frame split is asserted here, so the
     * caller's verdict can only have moved with the split: 501 posting rows for the corrected
     * account, 4_001 whole-range rows for the day, exactly one segment priced and none unpriced.
     *
     * @return how many closed segments the repair priced the keyed route cheaper for
     */
    private long driveWideSegmentRepair(int sharedQueryWorkerCount, int expectedPageFrames) throws Exception {
        final String base = "tx_w" + sharedQueryWorkerCount;
        final String view = "lv_w" + sharedQueryWorkerCount;
        execute("CREATE TABLE " + base + " (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 8, "
                + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
        // 4_000 rows of eight accounts filling 2026-01-02 at an even 21.6-second stride, so the
        // whole anchor day is one partition and every account holds exactly 500 of its rows.
        execute("INSERT INTO " + base + " SELECT "
                + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 21_600_000), "
                + "('acct-' || (x % 8))::symbol, "
                + "x::double "
                + "FROM long_sequence(4_000)");
        drainWalQueue();
        Assert.assertEquals(
                "the shared query worker count is what splits this partition: at the pinned"
                        + " 1_000-row frame floor a 4_000-row day carries one frame per worker",
                expectedPageFrames,
                countPageFrames(base, sharedQueryWorkerCount)
        );
        execute("CREATE LIVE VIEW " + view + " FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum "
                + "FROM " + base + " WINDOW w AS (PARTITION BY account_id ORDER BY created_at "
                + "ANCHOR DAILY '00:00')");

        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, sharedQueryWorkerCount)) {
            driveSeedToCompletion(job, view);
            driveRefreshToQuiescence(job);
            // A row of the next day moves the frontier out of 2026-01-02, which closes it.
            execute("INSERT INTO " + base + " VALUES ('2026-01-03T00:00:00.000000Z', 'acct-0', 1.0)");
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("an in-order day closes without a repair to price", 0, job.keyedScanPricedCountForTest());

            // The correction: one late row of one account, inside the day that just closed. Its
            // timestamp deliberately misses the seed's stride, so no two rows of the account share
            // one and the recompute oracle's ordering stays total.
            execute("INSERT INTO " + base + " VALUES ('2026-01-02T12:00:01.000000Z', 'acct-3', 1.0)");
            drainWalQueue();
            driveRefreshToQuiescence(job);

            Assert.assertEquals(
                    "the corrected closed day must be priced exactly once",
                    1,
                    job.keyedScanPricedCountForTest()
            );
            Assert.assertEquals(0, job.keyedScanUnpricedCountForTest());
            Assert.assertEquals(
                    "one account of eight across the day, plus the correction",
                    501,
                    job.keyedScanPostingRowsForTest()
            );
            Assert.assertEquals(
                    "the whole day, plus the correction",
                    4_001,
                    job.keyedScanWholeRangeRowsForTest()
            );
            assertViewMatchesRecompute(base, view);
            return job.keyedScanCheaperCountForTest();
        }
    }

    /**
     * One account's 500 rows at a one-second stride below 00:08:20 on 2026-01-02, then a
     * tail at 23:00:00.000000, 23:00:01.000000 and 23:00:01.000500 in the same daily
     * partition, under a view anchored every millisecond. The partition spans 23 hours, so
     * any 1ms interval of it estimates at 503 * 1ms / 23h, which truncates to zero rows -
     * while the account holds hundreds of postings across the partition, and the
     * CAPACITY-only index is a bitmap one, which reports no size and has to be walked.
     */
    private void createNarrowAnchorView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 256, "
                + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
        execute("INSERT INTO tx SELECT "
                + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 1_000_000), "
                + "'acct-1'::SYMBOL, "
                + "1.0 "
                + "FROM long_sequence(500)");
        execute("""
                INSERT INTO tx VALUES
                    ('2026-01-02T23:00:00.000000Z', 'acct-1', 1.0),
                    ('2026-01-02T23:00:01.000000Z', 'acct-1', 1.0),
                    ('2026-01-02T23:00:01.000500Z', 'acct-1', 1.0)""");
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum FROM tx "
                + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR EXPRESSION " + NARROW_ANCHOR + ")");
    }

    private void createView(String seedRows, boolean isKeyIndexed) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache"
                + (isKeyIndexed ? " index capacity 4" : "") + ", "
                + "amount double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    private LiveViewCheckpointKeyProjector keyProjector() {
        final LiveViewCheckpointKeyProjector projector = viewInstance()
                .getCompiledPlan()
                .getWindowFactory()
                .getCheckpointKeyProjector();
        Assert.assertNotNull("a single-identity view must compile a shared key projector", projector);
        return projector;
    }

    /**
     * One row of {@code account} at {@code hour} on 2026-01-{@code day}, as an INSERT tuple.
     * The day is what carries the case: with a daily anchor it is also the segment.
     */
    private String row(int day, int hour, String account) {
        return row(day, hour, 0, account);
    }

    private String row(int day, int hour, int minute, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Ten rows of each of four accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04,
     * all inside one hour of their day. A correction touching one account therefore leaves
     * three quarters of the segment the keyed scan must not read, and the whole day sits in
     * one partition so the per-key-per-frame setup does not dominate the comparison at this
     * scale the way it does on a real hourly-partitioned base.
     */
    private void runTheEstimateChargesOneIndexOpenPerPartitionAndOneSeekPerKeyPerPageFrame() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 8, "
                + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
        // Two days of 1_000 rows over eight accounts, round-robin. Every partition is an
        // exact multiple of the 100-row frame limit, so the split carries no trailing frame
        // and the count is the same whatever the shared query worker count is.
        execute("INSERT INTO tx SELECT "
                + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 86_400_000), "
                + "('acct-' || (x % 8))::symbol, "
                + "x::double "
                + "FROM long_sequence(2_000)");
        drainWalQueue();

        final long lowTs = ts("2026-01-02T00:00:00.000000Z");
        final long highTs = ts("2026-01-03T23:59:59.999999Z");

        long realFrames = 0;
        try (RecordCursorFactory factory = select("tx")) {
            RecordCursorFactory scan = factory;
            while (scan != null && !(scan instanceof PageFrameRecordCursorFactory)) {
                scan = scan.getBaseFactory();
            }
            Assert.assertTrue(
                    "a plain full scan is what the substitution needs",
                    scan instanceof PageFrameRecordCursorFactory
            );
            // The frames the executor really crosses, taken off the same forward page frame
            // cursor the keyed scan opens - not a second copy of the formula the estimate
            // uses, which would share any bug with it.
            try (PageFrameCursor frames = ((PageFrameRecordCursorFactory) scan).getPageFrameCursor(
                    sqlExecutionContext,
                    PartitionFrameCursorFactory.ORDER_ASC
            )) {
                while (frames.next() != null) {
                    realFrames++;
                }
            }
        }
        Assert.assertEquals("two 1_000-row partitions at a 100-row frame limit", 20, realFrames);

        final IntList keys = new IntList();
        final long postingRows;
        final long indexOpens;
        final long indexSeeks;
        final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
        try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
            keys.add(reader.getSymbolMapReader(1).keyOf("acct-3"));
            cost.of(reader, sqlExecutionContext);
            postingRows = cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, Long.MAX_VALUE);
            indexOpens = cost.getIndexOpens();
            indexSeeks = cost.getIndexSeeks();
        }
        Assert.assertEquals("one account of eight across two days", 250, postingRows);
        Assert.assertEquals(
                "the setup term must charge one index open per partition, whatever the key"
                        + " count and the frame split are",
                2,
                indexOpens
        );
        Assert.assertEquals(
                "and one index seek per key per page frame, which is what the row cursor"
                        + " rebuild really is",
                realFrames * keys.size(),
                indexSeeks
        );
        // Which is what counting them apart is for. The default prices an open at 256 base
        // rows and a seek at a sixth of that, so this shape costs 250 + 2*256 + 20*42 =
        // 1_602 against a 2_000-row whole-range scan and takes the keyed route. Charging
        // every seek an open's price - the model this replaces - made it 250 + 20*256 =
        // 5_370 and declined it.
        Assert.assertEquals(
                1_602,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(
                        postingRows, indexOpens, indexSeeks, keys.size(), 256, 42)
        );
        Assert.assertTrue(
                "priced apart, the keyed route prices below the whole range",
                LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                        postingRows, indexOpens, indexSeeks, keys.size(), 2_000, 256, 42)
        );
        Assert.assertFalse(
                "charged an index open for every per-frame seek, the same shape reads as"
                        + " more expensive than the whole range it replaces",
                LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                        postingRows, 0, indexSeeks, keys.size(), 2_000, 256, 256)
        );
    }

    /**
     * Two days of 1_000 rows over eight accounts, round-robin: the 86.4-second step puts
     * 1_000 rows in each daily partition, and one account holds exactly 125 of each day's.
     */
    private void seedEightAccountsOverTwoDays() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 8, "
                + "amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
        execute("INSERT INTO tx SELECT "
                + "timestamp_sequence('2026-01-02T00:00:00.000000Z', 86_400_000), "
                + "('acct-' || (x % 8))::symbol, "
                + "x::double "
                + "FROM long_sequence(2_000)");
        drainWalQueue();
    }

    private String seedFourAccountsOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int minute = 0; minute < 10; minute++) {
                for (int account = 1; account <= 4; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(day, 1, minute * 4 + account, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
