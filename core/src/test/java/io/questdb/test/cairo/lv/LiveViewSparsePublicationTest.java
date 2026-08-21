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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the identity a sparse repair publication stands on: the dedup keys a live
 * view's own table carries, and the upsert commit that publishes onto them.
 * <p>
 * A repair publishes with {@code WAL_DEDUP_MODE_REPLACE_RANGE}, which deletes the replaced
 * interval wholesale and so has to carry every row of it. Publishing only the rows it
 * recomputed instead needs {@code WAL_DEDUP_MODE_UPSERT_NEW} over
 * {@code (designated timestamp, projected partition key)}, which needs the view table to
 * carry that pair as its dedup keys. This is what puts them there, behind
 * {@code cairo.live.view.checkpoint.repair.sparse.publication.enabled}, and what proves the
 * publisher does what such a repair would need.
 * <p>
 * It runs <b>dark</b>: no repair takes the route, and the case below pins that a segment
 * repair of a dedup-keyed view still publishes its whole range. What the switch does change
 * is the view's <b>ordinary</b> path, which is the reason the ordinary commit is stamped
 * {@code WAL_DEDUP_MODE_NO_DEDUP} - a view may legitimately emit two rows sharing the pair,
 * and a default-mode commit on a dedup-keyed table would collapse them.
 * <p>
 * The view is the reported customer shape the keyed-replay, per-segment and uniqueness
 * cases use: an anchored WINDOW carrying an unbounded cumulative sum per account, over a
 * base whose timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewSparsePublicationTest extends AbstractLiveViewTest {
    private static final int ACCOUNTS = 4;
    private static final int ROWS_PER_ACCOUNT_PER_DAY = 4;

    @Test
    public void testASegmentRepairOnADedupKeyedViewStillPublishesItsWholeRange() throws Exception {
        // The dark half, and the property the fallback rests on: REPLACE_RANGE is a valid
        // publication on a dedup-keyed table. TableWriter.isCommitDedupMode() is false for
        // it, so the replacement removes the old interval without collapsing the equal
        // pairs the newly emitted one carries - which is exactly what a repair whose output
        // turns out to hold a repeat has to fall back to.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays() + ", " + repeatOfTheFirstRow(2, 1));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");
                Assert.assertEquals(
                        "the repeated pair must be in the view before the repair reads it",
                        2,
                        count("select count() from lv where cod_acct_no = 'acct-1'"
                                + " and created_at = '2026-01-02T01:00:01.000000Z'::timestamp")
                );

                commit(correction("acct-2"), job);

                Assert.assertEquals(
                        "the repair's own output holds the repeat, which is what rules a sparse commit out",
                        1,
                        job.outputUniquenessDuplicateRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                Assert.assertEquals(
                        "the replacement carries both rows of the pair and deletes neither",
                        2,
                        count("select count() from lv where cod_acct_no = 'acct-1'"
                                + " and created_at = '2026-01-02T01:00:01.000000Z'::timestamp")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewCarriesTheDedupKeysASparsePublicationWouldUpsertOn() throws Exception {
        // The designated timestamp goes in beside the key because
        // TableWriter.isDeduplicationEnabled() keys on it: a table whose timestamp is not a
        // dedup key does not deduplicate at all, whatever else is flagged.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("created_at,cod_acct_no", dedupKeysOf("lv"));
        });
    }

    @Test
    public void testAViewCreatedWithoutTheSwitchCarriesNoDedupKeys() throws Exception {
        // The default, and what every live view that exists today carries. Pinned here
        // because the flags are a schema property: a view that gained them by accident
        // would keep them, and its ordinary path would pay for an identity nothing asked
        // for.
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("", dedupKeysOf("lv"));
        });
    }

    @Test
    public void testAViewWhoseOutputDropsTheKeyCarriesNoDedupKeys() throws Exception {
        // There is no identity to publish on, so the switch resolves nothing and the table
        // stays as it is. Marking the timestamp alone would be worse than doing nothing: it
        // turns deduplication on for a table whose only remaining key is a timestamp many
        // of its rows share.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createKeylessView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("", dedupKeysOf("lv"));
        });
    }

    @Test
    public void testADedupKeyedViewKeepsTheForwardPathOffTheLagAndTheBlock() throws Exception {
        // What the identity costs the path that does not use it, and why it is affordable.
        // A non-default dedup mode makes WalTxnDetails stamp FORCE_FULL_COMMIT on the
        // transaction, which disables WAL lag retention and block coalescing - the tax the
        // design priced this decision on. For a live view both are already off: the view's
        // table declares maxUncommittedRows = 0, and TableWriter.getWalMaxLagRows() clamps
        // the lag budget AND the block-size budget to exactly that, so every live-view
        // commit already applies alone and in full. The mode changes which of two equal
        // paths it takes, not how many it takes.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("lv"))) {
                Assert.assertEquals(0, metadata.getMaxUncommittedRows());
            }
        });
    }

    @Test
    public void testAnOrdinaryRefreshOnADedupKeyedViewKeepsBothRowsOfARepeatedPair() throws Exception {
        // The case that carries the claim. Two base rows of one account at one instant
        // produce two output rows carrying different cumulative sums under one
        // (timestamp, key) pair, and the view's forward path has no identity to offer: it
        // reports what the base holds. On a commit left at the default dedup mode this
        // fails at expected:<2> but was:<1>, having silently kept the second row and
        // dropped the first - the shape the ordinary path is stamped NO_DEDUP to avoid.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final long rowsBefore = count("select count() from lv");

                // Forward rows, above everything the view holds: the ordinary drain, not a
                // repair. Both land at one instant under one account.
                commit(row(5, 1, 0, 0, "acct-1") + ", " + row(5, 1, 0, 0, "acct-1"), job);

                Assert.assertEquals(rowsBefore + 2, count("select count() from lv"));
                Assert.assertEquals(
                        2,
                        count("select count() from lv where cod_acct_no = 'acct-1'"
                                + " and created_at = '2026-01-05T01:00:00.000000Z'::timestamp")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARestartedViewStillKeepsBothRowsOfARepeatedPair() throws Exception {
        // The identity is a schema property, so the instance a restart builds has to
        // rediscover it from the table's own metadata - the configuration cannot answer it
        // (the switch may have moved) and the sequencer metadata the WAL writer reads does
        // not carry the flags. An instance that came back reading "no dedup keys" would
        // commit at the default mode and collapse a pair the pre-restart view kept, which
        // is a row lost to a restart and nothing else.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                final long rowsBefore = count("select count() from lv");

                commit(row(5, 1, 0, 0, "acct-1") + ", " + row(5, 1, 0, 0, "acct-1"), job);

                Assert.assertEquals(rowsBefore + 2, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUpsertPublicationReplacesItsOwnPairAndLeavesTheRest() throws Exception {
        // What the publisher is for: a block carrying one row per corrected pair replaces
        // exactly those rows and adds a pair the view did not hold, while every other
        // stored row stays where it stands. A REPLACE_RANGE over the same interval would
        // have had to carry all of them.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final long rowsBefore = count("select count() from lv");
                final String untouchedBefore = dumpRowsOf("acct-2");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                final TableToken viewToken = engine.verifyTableName("lv");
                try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                    // One pair the view already holds, one it does not.
                    appendViewRow(walWriter, ts("2026-01-02T01:00:01.000000Z"), "acct-1", 99.0);
                    appendViewRow(walWriter, ts("2026-01-02T01:00:02.000000Z"), "acct-1", 98.0);
                    walWriter.commitLiveViewWithUpsert(instance.getLastProcessedSeqTxn());
                }
                // The live view's own WAL is applied by the refresh job rather than by the
                // generic drain, so the block only lands once the job runs again.
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "the block replaced one stored row and inserted one new pair",
                        rowsBefore + 1,
                        count("select count() from lv")
                );
                assertQuery("select created_at, cod_acct_no, cumulative_sum from lv"
                        + " where cod_acct_no = 'acct-1'"
                        + " and created_at >= '2026-01-02T01:00:01.000000Z'::timestamp"
                        + " and created_at <= '2026-01-02T01:00:02.000000Z'::timestamp")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns("created_at\tcod_acct_no\tcumulative_sum\n"
                                + "2026-01-02T01:00:01.000000Z\tacct-1\t99.0\n"
                                + "2026-01-02T01:00:02.000000Z\tacct-1\t98.0\n");
                TestUtils.assertEquals(untouchedBefore, dumpRowsOf("acct-2"));
            }
        });
    }

    /**
     * One row of the view's own output, as the repair's copier writes it: the designated
     * timestamp, the projected key and the window's value.
     */
    private static void appendViewRow(WalWriter walWriter, long ts, String account, double cumulativeSum) {
        final TableWriter.Row row = walWriter.newRow(ts);
        row.putSym(1, account);
        row.putDouble(2, cumulativeSum);
        row.append();
    }

    /**
     * Turns the CREATE-time identity on. Nothing else about a view moves: the switch is
     * read once, at CREATE, and what it decides is the table's own metadata.
     */
    private void armSparsePublication() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
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
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 8, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
    }

    /**
     * The same view with the key left out of its SELECT. The window still partitions on it;
     * what the output does not carry is a column the pair could be named through.
     */
    private void createKeylessView(String seedRows) throws Exception {
        createBase(seedRows);
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, sum(amt_txn) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * Drops every registered instance and rebuilds the view graph off disk, which is the
     * catalogue load a restart runs.
     */
    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private void createView(String seedRows) throws Exception {
        createBase(seedRows);
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * The named table's dedup key columns, in column order, as a comma-separated list. Read
     * off the table's own metadata rather than off the structure that created it, so what
     * the case asserts is what landed in {@code _meta}.
     */
    private String dedupKeysOf(String tableName) {
        final StringBuilder keys = new StringBuilder();
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.isDedupKey(i)) {
                    if (keys.length() > 0) {
                        keys.append(',');
                    }
                    keys.append(metadata.getColumnName(i));
                }
            }
        }
        return keys.toString();
    }

    /**
     * The view's stored rows for one account, as text - the image a publication that does
     * not name that account must leave exactly where it found it.
     */
    private String dumpRowsOf(String account) throws Exception {
        return TestUtils.printSqlToString(
                engine,
                sqlExecutionContext,
                "select * from lv where cod_acct_no = '" + account + "' order by 1, 3",
                new StringSink()
        );
    }

    /**
     * A second row of {@code acct-account} at the exact instant its first seeded row of
     * 2026-01-{@code day} holds. Two base rows there produce two output rows carrying
     * different cumulative sums under one {@code (timestamp, key)} pair.
     */
    private String repeatOfTheFirstRow(int day, int account) {
        // i = 0 in the seed's own offset, so this tracks the seed rather than restating it.
        return row(day, 1, account / 60, account % 60, "acct-" + account);
    }

    private String row(int day, int hour, int minute, int second, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":" + String.format("%02d", second)
                + ".000000Z', '" + account + "', 1.0)";
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
