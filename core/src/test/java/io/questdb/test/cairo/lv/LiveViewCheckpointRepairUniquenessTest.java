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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the uniqueness verdict a real segment repair produces: whether the output
 * it emits carries each {@code (designated timestamp, projected partition key)} pair once.
 * <p>
 * The verdict runs <b>dark</b>. Every repair here publishes its whole replaced range with
 * {@code REPLACE_RANGE} exactly as it did before the detector existed, and each case
 * asserts that alongside the count: the view still matches a from-base recompute and holds
 * the same rows whichever way the verdict came out. What the counters are for is the rate
 * at which a sparse keyed publication - which needs the pair as a dedup identity, and would
 * silently collapse a repeat admitted to it - would have to fall back to the whole segment
 * instead.
 * <p>
 * The view is the reported customer shape the keyed-replay and per-segment cases use: an
 * anchored WINDOW carrying an unbounded cumulative sum per account, over a base whose
 * timestamps span several anchor days so closed segments exist at all. Every correction
 * lands at 00:30 of its day, below every row that day already holds, so the replacement's
 * floor sits under the lot and the replay re-emits the whole segment - which is what puts
 * the segment's own rows in front of the check.
 */
public class LiveViewCheckpointRepairUniquenessTest extends AbstractLiveViewTest {
    private static final int ACCOUNTS = 4;
    private static final int ROWS_PER_ACCOUNT_PER_DAY = 4;

    @Test
    public void testACorrectedSegmentWithOneRowPerPairIsReportedUnique() throws Exception {
        // The base case, and the shape a sparse publication exists for: every account
        // reports at its own instant, so every group holds one row and no pair repeats.
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(1, job.outputUniquenessCheckedRepairsForTest());
                Assert.assertEquals(1, job.outputUniquenessUniqueRepairsForTest());
                Assert.assertEquals(0, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertEquals(0, job.outputUniquenessUncheckedRepairsForTest());
                Assert.assertEquals(
                        "a group of one never widens the detector's scratch",
                        1,
                        job.outputUniquenessMaxGroupRowsForTest()
                );
                Assert.assertEquals(
                        "the check walks exactly the rows the replacement carries",
                        rowsOnTheSecond(),
                        job.outputUniquenessCheckedRowsForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testEqualTimestampsAcrossAccountsStayUnique() throws Exception {
        // The measured production base stamps whole seconds, so every account's row of one
        // instant lands in one group. That is a wide group rather than a duplicate, and a
        // detector reporting it as one would rule sparse publication out on the very shape
        // it was designed for.
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays() + ", " + oneInstantForEveryAccount(2));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(1, job.outputUniquenessCheckedRepairsForTest());
                Assert.assertEquals(1, job.outputUniquenessUniqueRepairsForTest());
                Assert.assertEquals(0, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertEquals(
                        "the four accounts sharing one instant are one group",
                        ACCOUNTS,
                        job.outputUniquenessMaxGroupRowsForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTwoRowsOfOneAccountAtOneInstantAreReportedAsADuplicate() throws Exception {
        // The fallback case. Two qualifying rows of one account at one timestamp produce two
        // output rows carrying different cumulative sums, and a sparse commit keyed on the
        // pair would keep one of them. The repair still publishes the whole range, so the
        // view is correct either way - which is the half the counter cannot say.
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays() + ", " + repeatOfTheFirstRow(2, 1));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(correction("acct-2"), job);

                Assert.assertEquals(1, job.outputUniquenessCheckedRepairsForTest());
                Assert.assertEquals(
                        "the segment's output holds a repeated pair, so it is not publishable sparsely",
                        0,
                        job.outputUniquenessUniqueRepairsForTest()
                );
                Assert.assertEquals(1, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testADuplicateSplitAcrossAParkIsStillFound() throws Exception {
        // The carrier claim on the production shape. A one-row replay budget parks the
        // repair after every row it emits, so the two rows of the repeated pair are emitted
        // by different turns - and a detector re-armed by the resuming turn would see the
        // second as the first row of its group and call the segment unique. It is exactly
        // the shape the keyed replay's own key domain had to be carried for, and it fails
        // the same silent way.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays() + ", " + repeatOfTheFirstRow(2, 1));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);

                commit(correction("acct-2"), job);

                Assert.assertTrue(
                        "a one-row replay budget must park the segment repair on its turn budget",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(1, job.outputUniquenessCheckedRepairsForTest());
                Assert.assertEquals(1, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertEquals(0, job.outputUniquenessUniqueRepairsForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewThatDoesNotCarryItsKeyIsCountedUnchecked() throws Exception {
        // The pair cannot be named through output that does not hold the key, so the repair
        // is recorded as one no verdict was taken for rather than as one that failed. A
        // detector reporting it unique would offer a sparse publication an identity the
        // stored rows do not carry.
        assertMemoryLeak(() -> {
            createKeylessView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(1, job.outputUniquenessUncheckedRepairsForTest());
                Assert.assertEquals(0, job.outputUniquenessCheckedRepairsForTest());
                Assert.assertEquals(0, job.outputUniquenessCheckedRowsForTest());
                assertNoRefreshFaults("lv");
            }
        });
    }

    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1, 3",
                "(lv) order by 2, 1, 3",
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

    /**
     * One correction of {@code account} on 2026-01-02, below every row that day already
     * holds, so the replacement's floor sits under the whole segment.
     */
    private String correction(String account) {
        return row(2, 0, 30, 0, account);
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

    private void createBase(String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 8, "
                + "amount double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
    }

    /**
     * The same view with the key left out of its SELECT. The window still partitions on it,
     * so the repair runs exactly as it does elsewhere - what it does not do is store a
     * column the pair could be named through.
     */
    private void createKeylessView(String seedRows) throws Exception {
        createBase(seedRows);
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, sum(amount) over w as cumulative_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    private void createView(String seedRows) throws Exception {
        createBase(seedRows);
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    /**
     * One row of every seeded account at 2026-01-{@code day}T03:30:00, which is the whole
     * seconds a real base stamps reduced to one group.
     */
    private String oneInstantForEveryAccount(int day) {
        final StringBuilder rows = new StringBuilder();
        for (int account = 1; account <= ACCOUNTS; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append(row(day, 3, 30, 0, "acct-" + account));
        }
        return rows.toString();
    }

    private String row(int day, int hour, int minute, int second, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":" + String.format("%02d", second)
                + ".000000Z', '" + account + "', 1.0)";
    }

    /**
     * A second row of {@code acct-account} at the exact instant its first seeded row of
     * 2026-01-{@code day} holds. Two base rows there produce two output rows carrying
     * different cumulative sums under one {@code (timestamp, key)} pair, which is the shape
     * a sparse commit keyed on that pair would collapse.
     */
    private String repeatOfTheFirstRow(int day, int account) {
        // i = 0 in the seed's own offset, so this tracks the seed rather than restating it.
        return row(day, 1, account / 60, account % 60, "acct-" + account);
    }

    /**
     * The view's stored rows on 2026-01-02, which is the segment every correction here
     * lands in and therefore the row set a repair of it re-emits.
     */
    private long rowsOnTheSecond() throws Exception {
        return count("select count() from lv"
                + " where created_at >= '2026-01-02T00:00:00.000000Z'::timestamp"
                + " and created_at < '2026-01-03T00:00:00.000000Z'::timestamp");
    }

    /**
     * Four rows of each of four accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04,
     * every one of them at its own second inside the 01:00 hour of its day - so the seeded
     * output holds one row per pair and a case that wants a repeat has to add it.
     */
    private String seedAccountsOverThreeDays() {
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
}
