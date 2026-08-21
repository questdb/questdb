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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the keyed repair of a closed anchor segment: the replay that follows only
 * the keys a correction touched, and the merge that supplies every other key's row from
 * the view's own stored output.
 * <p>
 * The property every case here rests on is that the two routes are indistinguishable in
 * what they publish. A whole-segment repair reads the segment, evaluates the window over
 * all of it and re-emits the lot; a keyed one reads the affected keys' rows through the
 * base's posting index, re-emits those, and copies the rest forward. The block carries the
 * segment's full row set either way, because {@code REPLACE_RANGE} deletes the range
 * wholesale - which is precisely why a replay emitting only its own keys' rows would drop
 * every other key's, and why the merge exists.
 * <p>
 * The one thing that is <b>not</b> the same is where an unaffected key's row comes from: a
 * whole-segment repair recomputes it from the base, a keyed one copies the stored one. So
 * a keyed repair stops being a from-base recompute of its range, which is the reason
 * {@code cairo.live.view.checkpoint.repair.keyed.replay.enabled} defaults to false and the
 * reason one case below pins the default.
 * <p>
 * The view is the same reported customer shape the pricing and per-segment repair cases
 * use: an anchored WINDOW carrying an unbounded cumulative sum per account, over a base
 * whose timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewCheckpointKeyedReplayTest extends AbstractLiveViewTest {
    private static final int ACCOUNTS = 8;
    private static final int ROWS_PER_ACCOUNT_PER_DAY = 10;

    @Test
    public void testAKeyedRepairIntroducingAnAccountEmitsItsRowsAndKeepsTheRest() throws Exception {
        // A key the view has never stored has no row for the merge to drop, and the replay
        // is the only thing that can produce one. The case that would fail is a merge
        // treating "unresolved in the view's symbol map" as "keep every row of it".
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final String before = dumpUnaffectedRowsOnTheSecond("acct-9");

                commit(correction("acct-9"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "every seeded row of the day belongs to a key the correction did not touch",
                        ACCOUNTS * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                TestUtils.assertEquals(before, dumpUnaffectedRowsOnTheSecond("acct-9"));
                Assert.assertEquals(1, count("select count() from lv where cod_acct_no = 'acct-9'"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionOnTheNullKeyIsRepairedByKey() throws Exception {
        // The null account is a partition key like any other: the base index names its
        // rows under a key of its own, and the view stores them under its own null key. A
        // merge that read "the correction's key does not resolve" as "keep every row of
        // it" would copy the null key's stale rows forward beside the replay's corrected
        // ones and double the day.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays() + ", " + seedNullAccountOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(nullCorrection(), job);

                Assert.assertEquals(
                        "a correction on the null key must be repaired by key like any other",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the named accounts' rows are the ones copied forward - the null key's are replayed",
                        ACCOUNTS * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                Assert.assertEquals(
                        ROWS_PER_ACCOUNT_PER_DAY + 1,
                        count("select count() from lv where cod_acct_no is null"
                                + " and created_at >= '2026-01-02T00:00:00.000000Z'::timestamp"
                                + " and created_at < '2026-01-03T00:00:00.000000Z'::timestamp")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentBehindAParkedOneIsStillRepairedByItsKeys() throws Exception {
        // The two default-on routes have to compose. A keyed replay never parks, so the
        // segment a loop parks on is always one the cost model turned down - and the
        // segments queued behind it are the ones most likely to be keyed. The key domain
        // is collected by the change-set decomposition into scratch that belongs to the
        // turn that classified it, so a loop carrying only its timestamps hands the
        // resuming turn nothing to arm from and every segment behind the park reads whole.
        //
        // The shape is the reported one: an eight-account correction on the older day,
        // which the cost model prices whole and which a one-row replay budget parks, and a
        // one-account correction on the day above it, which it prices keyed.
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correctionOnEveryAccount(2) + ", " + row(3, 0, 30, 0, "acct-1"), job);

                Assert.assertTrue(
                        "a one-row replay budget must park the loop's first segment",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "the eight-account day must be priced whole and the one-account day keyed",
                        1,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertEquals(
                        "the segment behind the parked one must still be repaired by its keys",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "and its merge must copy every unaffected account's row forward",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(
                        "both segments must be repaired, and each exactly once",
                        2,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairIsNotTakenWhenTheWholeSegmentIsCheaper() throws Exception {
        // The route is armed and the gate is open; the cost model is what turns it down.
        // At the default index-open price a forty-row day is not worth seeking through, so
        // the segment reads whole - which is what every repair did before the route existed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the whole-segment read is priced below the keyed one at this scale",
                        0,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertEquals(0, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(0, job.keyedReplayMergedRowsForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairLeavesEveryUnaffectedKeysRowExactlyAsItStood() throws Exception {
        // The property item 4's designed publication could not hold, and the one the merge
        // exists for: a REPLACE_RANGE over the segment deletes every row in it, so a replay
        // emitting only the corrected account's rows would take the other three accounts'
        // rows out of the day with it.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");
                final String before = dumpUnaffectedRowsOnTheSecond("acct-1");

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the seven untouched accounts' rows must be copied forward, not recomputed",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                TestUtils.assertEquals(
                        "an unaffected key's stored rows must survive the replacement unchanged",
                        before,
                        dumpUnaffectedRowsOnTheSecond("acct-1")
                );
                Assert.assertEquals(
                        "the block carries the segment's whole row set, so the view gains one row",
                        rowsBefore + 1,
                        count("select count() from lv")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairSurvivesARestartThroughItsOwnCheckpointRoots() throws Exception {
        // A keyed replay's state describes its own keys and no others, so the roots it
        // re-versions have to take the replayed entry for those and leave every other key's
        // exactly as the old root wrote it. Nothing detects a root that got that wrong at
        // the seal or at read time - the runtime still holds the truth - so the restore is
        // what reads it back.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);
                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                // The rows that read the restored accumulators back: one for a key the
                // keyed replay described and one for a key it did not.
                commit(row(5, 5, "acct-1") + ", " + row(5, 6, "acct-3"), job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairsRowPositionsSurviveASecondCorrectionBelowThem() throws Exception {
        // The ladder's cumulative positions are the thing a merged block can get wrong: a
        // boundary's position is the count of rows at or below it, and the replay emits
        // only some of them. A second correction below the first is what reads those
        // positions back, because it plans against the roots the first repair published.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(3, 0, 30, 0, "acct-2"), job);
                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "both corrections must be repaired by key",
                        2,
                        job.keyedReplaySegmentCountForTest()
                );
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnindexedKeyLeavesEverySegmentReadingWhole() throws Exception {
        // Without an index there is nothing to name one key's rows with, so the route is
        // not offered at all - and the repair is exactly the one this view has today.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays(), false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(0, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(0, job.keyedReplayMergedRowsForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheKeyedRouteIsOffByDefault() throws Exception {
        // The pricing says the keyed read is the smaller one and nothing takes it, because
        // what the route changes is not only the read: a copied-forward row is no longer
        // recomputed from the base. Turning it on is the decision rather than shipping it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the pricing still runs, and still says the keyed read is smaller",
                        1,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertEquals(
                        "and nothing takes it, because the switch is off",
                        0,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(0, job.keyedReplayMergedRowsForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTwoCorrectionsInOneClosedSegmentFollowBothKeys() throws Exception {
        // Two of four accounts corrected in one day: half the day is replayed and half is
        // copied forward, and the two halves have to add up to the day.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(correction("acct-1") + ", " + row(2, 0, 31, 0, "acct-2"), job);

                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(
                        "the six untouched accounts' rows are the ones copied forward",
                        (ACCOUNTS - 2) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 2, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * Turns the keyed route on, and prices one index open at one base row.
     * <p>
     * The default prices it at 256, which is what a real hourly-partitioned base against a
     * daily anchor segment is worth - and at forty rows a day it would (correctly) prefer
     * the whole segment whatever the key domain. What these cases are about is the replay
     * and its merge, so the setup term is priced at the scale the fixture actually has.
     */
    private void armKeyedReplay() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
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

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
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

    private void createView(String seedRows) throws Exception {
        createView(seedRows, true);
    }

    private void createView(String seedRows, boolean isKeyIndexed) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache"
                + (isKeyIndexed ? " index capacity 8" : "") + ", "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * The view's stored rows for 2026-01-02 that the correction does not touch, as text.
     * This is the image a keyed repair must leave byte for byte where it found it.
     */
    private String dumpUnaffectedRowsOnTheSecond(String correctedAccount) throws Exception {
        return TestUtils.printSqlToString(
                engine,
                sqlExecutionContext,
                "select * from lv where created_at >= '2026-01-02T00:00:00.000000Z'::timestamp"
                        + " and created_at < '2026-01-03T00:00:00.000000Z'::timestamp"
                        + " and cod_acct_no != '" + correctedAccount + "' order by 2, 1",
                new StringSink()
        );
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * One row of {@code account} at {@code hour}:{@code minute} on 2026-01-{@code day}, as
     * an INSERT tuple. The day is what carries the case: with a daily anchor it is also the
     * segment.
     */
    private String row(int day, int hour, String account) {
        return row(day, hour, 0, 0, account);
    }

    private String row(int day, int hour, int minute, int second, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":" + String.format("%02d", second)
                + ".000000Z', '" + account + "', 1.0)";
    }

    /**
     * The same correction on the null account, which both indexes name under a key of
     * their own.
     */
    private String nullCorrection() {
        return "('2026-01-02T00:30:00.000000Z', null, 1.0)";
    }

    /**
     * One correction of {@code account} on 2026-01-02, below every row that day already
     * holds - which is what puts the segment's stored rows inside the range the
     * replacement deletes, and so in front of the merge.
     */
    private String correction(String account) {
        return row(2, 0, 30, 0, account);
    }

    /**
     * One correction of every seeded account on 2026-01-{@code day}, at 00:30 like
     * {@link #correction}. Eight keys against eighty rows is what makes the cost model
     * price the day's whole-segment read below its keyed one, which is the only way to get
     * a segment that both reads whole and may therefore park.
     */
    private String correctionOnEveryAccount(int day) {
        final StringBuilder rows = new StringBuilder();
        for (int account = 1; account <= ACCOUNTS; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append(row(day, 0, 30, account, "acct-" + account));
        }
        return rows.toString();
    }

    /**
     * Ten rows of each of eight accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04,
     * all inside the 01:00 hour of their day.
     * <p>
     * Two things about the shape carry the cases. A correction touching one account leaves
     * seven eighths of the segment for the merge to copy forward, and the whole day sits in
     * one partition, so the per-key-per-frame setup does not dominate the comparison at
     * this scale the way it does on a real hourly-partitioned base. And every seeded row
     * sits ABOVE the correction {@link #correction} lands at, so the replacement's floor is
     * below the lot and the merge really has the day's rows in front of it - a correction
     * above them would leave the segment's stored rows outside the replaced range and the
     * merge with nothing to do.
     */
    private String seedEightAccountsOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int i = 0; i < ROWS_PER_ACCOUNT_PER_DAY; i++) {
                for (int account = 1; account <= ACCOUNTS; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    final int offset = i * ACCOUNTS + account;
                    rows.append(row(day, 1, offset / 60, offset % 60, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }

    /**
     * Ten rows of the null account on each of the three seeded days, in the 02:00 hour so
     * they sit above {@link #nullCorrection} and inside the same anchor segment.
     */
    private String seedNullAccountOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int i = 0; i < ROWS_PER_ACCOUNT_PER_DAY; i++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append("('2026-01-0").append(day).append("T02:00:")
                        .append(String.format("%02d", i)).append(".000000Z', null, 1.0)");
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
