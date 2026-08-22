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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
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
 * A <b>keyed</b> repair of such a view acts on it: when its output names each pair once it
 * commits only the rows it recomputed, and when the output repeats a pair it abandons the
 * attempt before committing anything and publishes its whole range with
 * {@code REPLACE_RANGE}, which collapses nothing. A repair that reads its segment whole has
 * no smaller set to publish and takes the replacement either way, which the case below
 * pins.
 * <p>
 * The switch also changes the view's <b>ordinary</b> path, which is why the ordinary commit
 * is stamped {@code WAL_DEDUP_MODE_NO_DEDUP} - a view may legitimately emit two rows sharing
 * the pair, and a default-mode commit on a dedup-keyed table would collapse them.
 * <p>
 * That stamp is decided in one place, {@code commitLiveViewBlock}, and reached from four
 * forward sites: the lead flush, the emergency flush a stalled tier publish falls back to,
 * the coupled drain a DEDUP base routes the view through, and the seed sweep. Four cases
 * drive a repeated pair through them - split across two flushes, held in the lead and
 * carried by one delayed flush, written beside its stored twin by an emergency flush, and
 * split across two coupled commits - because a site that forgot the stamp loses a row
 * nothing downstream can detect. The seed sweep is covered by the seeded repeat the repair
 * cases start from. A pair split across two commits is the shape an in-block repeat cannot
 * reach: the apply deduplicates a block against the stored partition as well as against
 * itself, so the second commit's row is the one that would overwrite the first.
 * <p>
 * Keeping every row is one half of the enabling gate; the other half is that nothing else
 * moved. Three <b>differential</b> cases put a second view over the same base with the switch
 * off at its CREATE and drive one workload through both: an anchor resume, a closed-segment
 * replacement and the ordinary forward path. Each asserts the two arms hold the same rows,
 * read the same number of base rows, take the same route and reach it in the same number of
 * transactions. A from-base oracle cannot say that on its own - two arms that lost the same
 * row match neither, and one that lost it on neither matches both - so the control is what
 * attributes a divergence to the dedup keys rather than to the workload.
 * <p>
 * A stamp is a thing that can be forgotten, so the seal no longer takes it on trust: it holds
 * every checkpoint root to the rows the view's table actually holds, because a root carries
 * the rows the view <i>emitted</i> as its cumulative position and a collapse would put a
 * ladder on disk that nothing detects and a restart can only fail on. The two cases at the
 * bottom drive both arms of that invariant.
 * <p>
 * The view is the reported customer shape the keyed-replay, per-segment and uniqueness
 * cases use: an anchored WINDOW carrying an unbounded cumulative sum per account, over a
 * base whose timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewSparsePublicationTest extends AbstractLiveViewTest {
    private static final int ACCOUNTS = 4;
    private static final int ROWS_PER_ACCOUNT_PER_DAY = 4;

    @Test
    public void testABoundaryTheKeyedScanNeverCrossesKeepsItsOwnPosition() throws Exception {
        // The sparse route's half of the ladder property LiveViewCheckpointKeyedReplayTest
        // pins for the merged one. A sparse publication writes none of the rows its merge
        // walks, but it still accounts for every one of them - a row left where it stands is
        // still a row below a boundary - so the ladder the two routes publish is the same,
        // to the row, and so is the way it goes wrong.
        //
        // The correction touches acct-1, whose rows in the repaired day stop at 01:00, while
        // acct-2 carries five above it. The keyed cursor therefore crosses none of the five
        // boundaries those rows sealed, and each has to take the rows at or below itself
        // rather than the segment's total.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(row(2, 1, 0, 0, "acct-1"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                for (int minute = 10; minute <= 50; minute += 10) {
                    commit(row(2, 1, minute, 0, "acct-2"), job);
                }
                // The head, which closes the second day below it.
                commit(row(5, 1, 0, 0, "acct-1"), job);
                assertLadderCountsRowsAtOrBelowEachBoundary("before");

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the correction must publish sparsely, or the case covers nothing",
                        1,
                        job.sparsePublicationCountForTest()
                );
                Assert.assertEquals(
                        "the merge must account for every row above the last key the replay followed",
                        5,
                        job.sparsePublicationRowsKeptForTest()
                );
                assertLadderCountsRowsAtOrBelowEachBoundary("after");
                assertViewMatchesRecompute();
            }
        });
    }

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
                        rowsAt("2026-01-02T01:00:01.000000Z", "acct-1")
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
                        rowsAt("2026-01-02T01:00:01.000000Z", "acct-1")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAUniqueSegmentRepairPublishesOnlyTheKeysItRecomputed() throws Exception {
        // The route this stage exists for. A keyed repair of a dedup-keyed view whose
        // output names each pair once commits the rows it recomputed and nothing else,
        // upserted onto (created_at, account_id): every other account's stored row stays
        // exactly where it stands rather than being rewritten as itself, which is what a
        // REPLACE_RANGE over the same interval has to do.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");
                final String untouchedBefore = dumpRowsOf("acct-3");

                commit(correction("acct-2"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key for there to be a smaller set to publish",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(
                        "the three accounts the correction did not touch keep every row of the day",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.sparsePublicationRowsKeptForTest()
                );
                Assert.assertEquals(
                        "a sparse publication writes none of the rows it kept",
                        0,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                TestUtils.assertEquals(untouchedBefore, dumpRowsOf("acct-3"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairAbandonsTheSparseAttemptAndPublishesTheWholeRange() throws Exception {
        // The fallback, and the case that carries it. The repeated pair belongs to the
        // account the correction touches, so it is in the set a sparse commit would have
        // carried - and an upsert on (created_at, account_id) would collapse it to one
        // row. The repair abandons the attempt before it commits anything: the merge
        // writes the rows it had only counted and the whole range goes out as a
        // REPLACE_RANGE, which collapses nothing.
        //
        // What makes this a fallback rather than a rollback is where the abandoning
        // happens. A sparse attempt reads the view's stored rows to count them and writes
        // none of them, so by the time the duplicate is known the rows a replacement needs
        // have already been walked past; rows_kept below is what the merge then re-reads
        // and writes.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays() + ", " + repeatOfTheFirstRow(2, 1));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");
                final String untouchedBefore = dumpRowsOf("acct-3");

                commit(correction("acct-1"), job);

                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(
                        "the repeat is in the set a sparse commit would have carried",
                        1,
                        job.outputUniquenessDuplicateRowsForTest()
                );
                Assert.assertEquals(0, job.sparsePublicationCountForTest());
                Assert.assertEquals(1, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(
                        "the abandoned attempt writes the rows it had only counted",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(0, job.sparsePublicationRowsKeptForTest());
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                Assert.assertEquals(
                        "the replacement carries both rows of the pair and deletes neither",
                        2,
                        rowsAt("2026-01-02T01:00:01.000000Z", "acct-1")
                );
                TestUtils.assertEquals(untouchedBefore, dumpRowsOf("acct-3"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASparselyPublishedSegmentSurvivesARestartAndAFurtherRepair() throws Exception {
        // The ladder a sparse publication leaves behind, end to end. Its cadence
        // boundaries carry cumulative live-view row positions, and a sparse commit
        // rewrites none of the rows below them - so the merge has to go on counting the
        // rows it no longer writes. Nothing reads those positions back until a restart
        // rebuilds the runtime from the roots that carry them, which is what this drives,
        // and a second correction on top is what makes the rebuilt state produce output
        // again.
        //
        // A merge that stopped counting does not reach this case, and the reason is worth
        // recording: the repair proves its own row arithmetic against the durable
        // row-count change before it publishes the splice, so a short count refuses the
        // publication rather than writing a ladder no reader could detect. What that
        // leaves this case pinning is the other half - that a published ladder describes
        // the rows on disk, including the ones the publication left alone.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                commit(correction("acct-2"), job);
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
            }
            final long rowsBeforeRestart = count("select count() from lv");

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(rowsBeforeRestart, count("select count() from lv"));
                Assert.assertEquals(
                        "the restored ladder credits the view with every row on disk, including"
                                + " the ones the sparse publication left alone",
                        rowsBeforeRestart,
                        engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal()
                );
                assertViewMatchesRecompute();

                commit(correction("acct-3"), job);

                Assert.assertEquals(rowsBeforeRestart + 1, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewWithoutTheDedupKeysNeverPublishesSparsely() throws Exception {
        // The reason the route needs no switch of its own: the identity is a CREATE-time
        // schema property, so a view that does not carry it has no pair to upsert on
        // however the keyed read is configured. The keyed repair below runs and publishes
        // its whole range, exactly as it did before this stage existed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(correction("acct-2"), job);

                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(0, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(
                        "the replacement carries every other account's row for the day",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
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
            Assert.assertEquals("created_at,account_id", dedupKeysOf("lv"));
        });
    }

    @Test
    public void testAViewCreatedWithTheSwitchDeclinedCarriesNoDedupKeys() throws Exception {
        // The switch in the direction that is now the non-default one, and what every live
        // view created before this identity existed carries. Pinned here because the flags
        // are a schema property: a view that gained them by accident would keep them, and
        // its ordinary path would pay for an identity nothing asked for.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
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
                        rowsAt("2026-01-05T01:00:00.000000Z", "acct-1")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairSplitAcrossTwoFlushesKeepsBothRowsAndItsLadder() throws Exception {
        // The half of the identity an in-block repeat cannot reach. Here the two rows of
        // the pair are emitted by two different refresh turns, so the second one's block
        // meets its twin already on the view's own partition - and an apply at the default
        // dedup mode deduplicates a block against the stored rows as well as against
        // itself. The stamped NO_DEDUP mode is what keeps the first row where it stands.
        //
        // A cadence seal runs between the two, because the checkpoint interval is one row:
        // the pair therefore straddles a checkpoint as well as a commit, and the seal that
        // follows the second row is held to a table that must hold both. On a forward commit
        // forced back to the default mode this fails at expected:<50> but was:<49>.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long durableBefore = durableRows();
                final long o3ReplayRowsBefore = instance.getO3ReplayScanRows();
                final long viewTxnBefore = liveViewWriterTxn();

                commit(row(5, 1, 0, 0, "acct-1"), job);

                final long viewTxnBetween = liveViewWriterTxn();
                Assert.assertTrue(
                        "the first row of the pair must be durable before the second is emitted",
                        viewTxnBetween > viewTxnBefore
                );
                Assert.assertEquals(durableBefore + 1, durableRows());

                commit(row(5, 1, 0, 0, "acct-1"), job);

                Assert.assertTrue(
                        "the second row goes out in a commit of its own, against a table already"
                                + " holding its pair",
                        liveViewWriterTxn() > viewTxnBetween
                );
                Assert.assertEquals(
                        "the second commit's apply leaves the stored twin alone",
                        durableBefore + 2,
                        durableRows()
                );
                Assert.assertEquals(2, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                Assert.assertEquals(
                        "both rows are forward rows at the frontier - an equal timestamp is not"
                                + " an out-of-order one, so no replay republished the pair",
                        o3ReplayRowsBefore,
                        instance.getO3ReplayScanRows()
                );
                Assert.assertEquals(0, instance.getCheckpointRowCountMismatches());
                Assert.assertEquals(durableRows(), instance.getLvRowsTotal());
                Assert.assertTrue(
                        "the seal across the pair stamped a root rather than refusing one",
                        instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL
                );
                assertViewMatchesRecompute();

                // The publication half. A correction below the pair replays the range that
                // holds it and republishes the lot with REPLACE_RANGE, which deletes the
                // interval and carries every row of it - the publication a repeated pair
                // always takes, and the one that has to bring both rows back.
                commit(row(5, 0, 30, 0, "acct-2"), job);

                Assert.assertTrue(
                        "the correction must replay the range the pair sits in",
                        instance.getO3ReplayScanRows() > o3ReplayRowsBefore
                );
                Assert.assertEquals(2, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                Assert.assertEquals(0, instance.getCheckpointRowCountMismatches());
                Assert.assertEquals(durableRows(), instance.getLvRowsTotal());
                assertViewMatchesRecompute();
            }

            // The checkpoint half. Nothing reads the positions a seal stamped until a restart
            // rebuilds the runtime off the roots that carry them, so this is where a ladder
            // that credited the split pair with one row rather than two would surface.
            final long rowsBeforeRestart = durableRows();
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(rowsBeforeRestart, durableRows());
                Assert.assertEquals(
                        "the restored ladder credits the view with both rows of the split pair",
                        rowsBeforeRestart,
                        engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal()
                );
                Assert.assertEquals(2, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairHeldInTheLeadReachesDiskThroughOneDelayedFlush() throws Exception {
        // The delayed flush: a FLUSH EVERY longer than the drive loop's own clock step, so
        // the drained rows sit in the in-memory tier as an un-flushed lead and reach disk
        // only when the cadence comes round. Both rows of the pair are drained in separate
        // turns and land in one block, which is a third way for the apply to see them -
        // neither the same drain nor the same commit, but the same flush.
        //
        // The lead is also what the view serves while the rows are off disk, so the case
        // pins that the pair is complete in the view before the flush and complete on disk
        // after it. A seal only follows the flush, which is why the invariant is asserted
        // there rather than over the lead. On a forward commit forced back to the default
        // mode this fails at expected:<51> but was:<50>.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createBase(seedAccountsOverThreeDays());
            createViewOverBase("10s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                // One forward row to arm the cadence. The seed reaches disk through the sweep,
                // which leaves the flush clock unset - so without this the first lead flush is
                // due immediately and the pair is split before the case can hold it.
                commit(row(5, 0, 0, 0, "acct-2"), job);
                final long durableBefore = durableRows();

                commit(row(5, 1, 0, 0, "acct-1"), job);
                commit(row(5, 1, 0, 0, "acct-1"), job);

                Assert.assertEquals(
                        "the cadence has not come round, so neither row is on disk yet",
                        durableBefore,
                        durableRows()
                );
                Assert.assertEquals("both rows are held as the un-flushed lead", 2, instance.getLeadRowCount());
                Assert.assertEquals(
                        "the view serves the pair out of the tier while it waits for the flush",
                        2,
                        rowsAt("2026-01-05T01:00:00.000000Z", "acct-1")
                );

                // Cross the FLUSH EVERY deadline: one delayed flush carries the whole lead.
                setCurrentMicros(currentMicros + 11_000_000L);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "the delayed flush wrote both rows of the pair",
                        durableBefore + 2,
                        durableRows()
                );
                Assert.assertEquals(2, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                Assert.assertEquals(0, instance.getCheckpointRowCountMismatches());
                Assert.assertEquals(durableRows(), instance.getLvRowsTotal());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairAnEmergencyFlushWroteKeepsBothRows() throws Exception {
        // The emergency flush: the tier publish fails mid-swap, so finishLeadRefresh writes
        // the staging rows straight to disk rather than re-draining them. The row it writes
        // that way is the second of a pair whose first row is already stored, so the route
        // has to reach the same stamped commit the ordinary flush does. It does, because
        // commitLiveViewBlock is the one place that decides the mode - which is exactly the
        // claim this case exists to hold, since an emergency flush is the forward site
        // easiest to forget.
        //
        // The injection only fires on the publish slow path, so the growth budget is zeroed
        // to force it. On a forward commit forced back to the default mode this fails at
        // expected:<50> but was:<49>.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long durableBefore = durableRows();

                commit(row(5, 1, 0, 0, "acct-1"), job);
                Assert.assertEquals(durableBefore + 1, durableRows());

                final LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull("the view must hold a tier for its publish to be failed", tier);
                final int publishedBeforeFailure = tier.getPublishedIdx();
                tier.setFailNextPublishSwap(new RuntimeException("test: simulated mid-swap failure"));

                commit(row(5, 1, 0, 0, "acct-1"), job);

                Assert.assertEquals(
                        "the publish must have failed for the emergency flush to be the route",
                        publishedBeforeFailure,
                        tier.getPublishedIdx()
                );
                Assert.assertEquals(
                        "the emergency flush recovers the cycle rather than retrying it",
                        0,
                        instance.getFlushRetryCount()
                );
                Assert.assertEquals(
                        "the row the emergency flush wrote did not collapse its stored twin",
                        durableBefore + 2,
                        durableRows()
                );
                Assert.assertEquals(2, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                Assert.assertEquals(0, instance.getCheckpointRowCountMismatches());
                Assert.assertEquals(durableRows(), instance.getLvRowsTotal());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairSplitAcrossTwoCoupledCommitsKeepsBothRows() throws Exception {
        // The fourth forward site: the coupled drain, which commits and applies every cycle
        // with no in-memory lead at all. A DEDUP base is what routes a view there - the
        // refresh reads the applied, post-dedup base rather than the raw WAL - so the rows
        // reach the view's table through a different drain from the three cases above.
        //
        // The base keeps both rows of the pair because its own dedup keys carry the amount;
        // what repeats in the view's output is (created_at, account_id), which is the pair
        // the view's table deduplicates on and the one at risk. On a forward commit forced
        // back to the default mode this fails at expected:<50> but was:<49>.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createDedupBaseView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long durableBefore = durableRows();

                commit(row(5, 1, 0, 0, "acct-1", 1.0), job);
                commit(row(5, 1, 0, 0, "acct-1", 2.0), job);

                Assert.assertEquals(
                        "a DEDUP base takes the coupled cadence, which holds no un-flushed lead",
                        0,
                        instance.getLeadRowCount()
                );
                Assert.assertEquals(
                        "the base kept both rows, so the view emitted both",
                        durableBefore + 2,
                        durableRows()
                );
                Assert.assertEquals(2, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                Assert.assertEquals(0, instance.getCheckpointRowCountMismatches());
                Assert.assertEquals(durableRows(), instance.getLvRowsTotal());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACollapsedForwardRowIsRefusedBeforeItReachesTheLadder() throws Exception {
        // The permanent invariant, and the failure it exists for. Every timeline root
        // carries the rows the view has emitted as its cumulative lvRowPosition, so the
        // seal compares that count against the rows the table actually holds before it
        // stamps one.
        //
        // The drift is produced the only way it can be produced without an unrelated
        // defect: the forward commit goes out at the default dedup mode - what the
        // ordinary path did before the mode was stamped - so the apply collapses the two
        // output rows sharing (created_at, account_id) into the last one written. The
        // view emitted two rows and its table kept one, and nothing downstream would
        // notice: the seal would go on stamping the count the view emitted, and a ladder
        // whose positions overstate the output is not something a later restart can
        // detect, only fail on.
        //
        // What the seal does instead is decline: it re-seats the counter on the table's
        // own size, retires the timeline over the roots it can no longer vouch for, and
        // leaves the next cadence to open a fresh history at the corrected position. The
        // lost row itself comes back from the base, through the rebuild a retired
        // timeline routes the view to.
        armSparsePublication();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long durableBefore = durableRows();
                Assert.assertEquals(
                        "the counter and the table agree before anything collapses",
                        durableBefore,
                        instance.getLvRowsTotal()
                );

                job.setSimulateForwardCommitDedupCollapseForTest(true);
                commit(row(5, 1, 0, 0, "acct-1") + ", " + row(5, 1, 0, 0, "acct-1"), job);

                Assert.assertEquals(
                        "the table kept one of the two rows the view emitted",
                        durableBefore + 1,
                        durableRows()
                );
                Assert.assertEquals(
                        "the seal caught the drift rather than stamping it into a root",
                        1,
                        instance.getCheckpointRowCountMismatches()
                );
                Assert.assertEquals(
                        "the counter is re-seated on the rows the table can account for",
                        durableBefore + 1,
                        instance.getLvRowsTotal()
                );
                Assert.assertEquals(
                        "the head is cleared, so the next cadence opens a fresh history",
                        Numbers.LONG_NULL,
                        instance.getHeadCheckpointLvSeqTxn()
                );
                assertQuery("SELECT checkpoint_row_count_mismatches, checkpoint_timeline_generation"
                        + " FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("checkpoint_row_count_mismatches\tcheckpoint_timeline_generation\n"
                                + "1\tnull\n");
            }
        });
    }

    @Test
    public void testTheOrdinarySealStampsTheRowCountItsTableHolds() throws Exception {
        // The other arm, and what says the invariant costs the healthy routes nothing.
        // The same repeated pair a stamped NO_DEDUP commit keeps, then a keyed repair
        // that publishes sparsely on the dedup keys - two routes that move the counter
        // and the table in different ways, one adding what it appended and the other
        // re-seating off the durable size. Both leave the two equal, so every seal in the
        // run stamps a position the table can account for and the mismatch counter never
        // moves.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                commit(row(5, 1, 0, 0, "acct-1") + ", " + row(5, 1, 0, 0, "acct-1"), job);
                commit(correction("acct-2"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key for the sparse route to be exercised",
                        1,
                        job.sparsePublicationCountForTest()
                );
                Assert.assertEquals(0, instance.getCheckpointRowCountMismatches());
                Assert.assertEquals(durableRows(), instance.getLvRowsTotal());
                Assert.assertTrue(
                        "a seal that was refused would have cleared the head",
                        instance.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL
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
                assertQuery("select created_at, account_id, cumulative_sum from lv"
                        + " where account_id = 'acct-1'"
                        + " and created_at >= '2026-01-02T01:00:01.000000Z'::timestamp"
                        + " and created_at <= '2026-01-02T01:00:02.000000Z'::timestamp")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns("created_at\taccount_id\tcumulative_sum\n"
                                + "2026-01-02T01:00:01.000000Z\tacct-1\t99.0\n"
                                + "2026-01-02T01:00:02.000000Z\tacct-1\t98.0\n");
                TestUtils.assertEquals(untouchedBefore, dumpRowsOf("acct-2"));
            }
        });
    }

    @Test
    public void testTheDedupKeysLeaveAnInlineResumeExactlyAsThePlainViewHasIt() throws Exception {
        // The enabling gate's other half, and the route the cases above never reach. A
        // correction inside the view's own active segment resumes from the anchor below it
        // and republishes (anchorMaxTs, +inf) with REPLACE_RANGE - replayFromAnchor's own
        // commit site, not the segment repair's - and it is what 94% of the measured
        // workload's corrections take.
        //
        // What makes this a differential rather than a third recompute check is the second
        // view. Both read the same base through the same SELECT at the same cadence; the
        // only thing that separates them is the dedup keys one of them carries, so a
        // divergence has exactly one possible cause. The from-base oracle cannot say that:
        // a repair that lost a row on both arms matches neither, and one that lost it on
        // neither matches both.
        //
        // The repeated pair sits inside the resumed range, which is what gives the case
        // teeth: it is the row a publication that consulted the table's dedup keys would
        // collapse. Quoted against a resume that commits with commitLiveViewWithUpsert
        // instead of the replacement - the wrong publication on the right route, and a
        // mistake only reachable now that the sparse publisher exists - the arms come apart
        // in both directions at once: the keyed one collapses the pair to a single row and
        // the plain one, whose table has no keys to upsert on, keeps three.
        assertMemoryLeak(() -> {
            createBothArms(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance keyed = instanceOf("lv");
                final LiveViewInstance plain = instanceOf("lv_plain");
                Assert.assertTrue("the keyed arm must carry the identity", keyed.isDedupKeyed());
                Assert.assertFalse("the control arm must not", plain.isDedupKeyed());
                // One forward turn above the seed. Its seal is the anchor the correction
                // resumes from: an anchor covers rows up to and including its own maxTs, so
                // only a root a forward turn already sealed can sit below a later
                // correction, and the sweep that seeds a view seals none.
                commit(row(4, 1, 0, 20, "acct-1"), job);
                // The repeated pair, two turns above that anchor and inside the range the
                // resume republishes.
                commit(row(4, 1, 0, 30, "acct-1"), job);
                commit(row(4, 1, 0, 30, "acct-1"), job);
                Assert.assertEquals(
                        "both rows of the pair must be in the keyed arm before the repair reads it",
                        2,
                        rowsAt("lv", "2026-01-04T01:00:30.000000Z", "acct-1")
                );
                final long resumeRowsBefore = keyed.getO3ResumeReplayRows();

                // Above the anchor and below the pair, so the turn resumes from that anchor
                // and the range it republishes holds both rows of the pair.
                commit(row(4, 1, 0, 25, "acct-2"), job);

                Assert.assertTrue(
                        "the correction must take the resume disposition; nothing else exercises"
                                + " replayFromAnchor's replacement",
                        keyed.getO3ResumeReplayRows() > resumeRowsBefore
                );
                Assert.assertEquals(
                        "the identity may not move the repair onto another route",
                        plain.getO3ResumeReplayRows(),
                        keyed.getO3ResumeReplayRows()
                );
                Assert.assertEquals(
                        "nor may it change what the replay reads",
                        plain.getO3ReplayScanRows(),
                        keyed.getO3ReplayScanRows()
                );
                Assert.assertEquals(
                        "the identity alone leaves nothing sparse to publish - there is no smaller"
                                + " set without the keyed read",
                        0,
                        job.sparsePublicationCountForTest()
                );
                Assert.assertEquals(
                        "the replacement carries both rows of the pair on the keyed arm",
                        2,
                        rowsAt("lv", "2026-01-04T01:00:30.000000Z", "acct-1")
                );
                assertArmsAgree();
            }
        });
    }

    @Test
    public void testTheDedupKeysLeaveAClosedSegmentReplacementExactlyAsThePlainViewHasIt() throws Exception {
        // The same differential over the second replacement site: a correction in a closed
        // segment, repaired per segment and published with REPLACE_RANGE over that segment
        // alone. testASegmentRepairOnADedupKeyedViewStillPublishesItsWholeRange proves the
        // keyed arm keeps its pair and matches a from-base recompute; what this adds is the
        // control that says the keys changed nothing - the same rows, the same reads and the
        // same number of repairs as a view without them.
        //
        // This is the arm that dies on TableWriter.isCommitDedupMode() extended to admit
        // WAL_DEDUP_MODE_REPLACE_RANGE, at expected:<2> but was:<1>: the replacement
        // collapses the pair it carries. The resume case above survives that same mutation,
        // and the two replacements differ in where they land - this one rewrites a partition
        // two days below the frontier, the resume's only the last one - so whichever apply
        // path the mutation reaches, the two cases are not one case written twice.
        assertMemoryLeak(() -> {
            createBothArms(seedAccountsOverThreeDays() + ", " + repeatOfTheFirstRow(2, 1));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance keyed = instanceOf("lv");
                final LiveViewInstance plain = instanceOf("lv_plain");
                final long repairsBefore = job.segmentRepairCountForTest();

                // 2026-01-02, with two later days above it: a closed segment on both arms.
                commit(correction("acct-2"), job);

                Assert.assertEquals(
                        "both arms must repair the closed segment; a route that skipped one would"
                                + " leave the arms agreeing about nothing",
                        repairsBefore + 2,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the identity may not change what a segment repair reads",
                        plain.getO3ReplayScanRows(),
                        keyed.getO3ReplayScanRows()
                );
                Assert.assertEquals(0, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(
                        "the replacement carries both rows of the pair on the keyed arm",
                        2,
                        rowsAt("lv", "2026-01-02T01:00:01.000000Z", "acct-1")
                );
                assertArmsAgree();
            }
        });
    }

    @Test
    public void testTheDedupKeysLeaveTheOrdinaryForwardPathExactlyAsThePlainViewHasIt() throws Exception {
        // The forward half of the gate. The cases above prove a dedup-keyed view keeps
        // both rows of a repeated pair; this one proves it keeps everything else the same
        // way a view without the keys does - the same rows, and the same number of
        // transactions to put them there.
        //
        // The transaction count is the assertion worth having, because the mode is the one
        // thing the identity does change on this path. A NO_DEDUP stamp makes WalTxnDetails
        // write FORCE_FULL_COMMIT for the transaction, which disables lag retention and
        // caps block coalescing - the tax the design priced this decision on. If it applied
        // to a live view, the arms would need a different number of applies to write the
        // same rows. They do not, because the view's table declares maxUncommittedRows = 0
        // and both budgets are already clamped to it.
        //
        // On a forward commit forced back to the default dedup mode - the pre-item-2 path,
        // restored through setSimulateForwardCommitDedupCollapseForTest - this fails at
        // expected:<2> but was:<1> on the keyed arm, while the plain arm keeps both rows and
        // the keyed arm's next seal counts the drift. That asymmetry is the reason the
        // control is a control: the same workload, the same mutation, and only the arm
        // carrying the keys loses anything.
        assertMemoryLeak(() -> {
            createBothArms(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance keyed = instanceOf("lv");
                final LiveViewInstance plain = instanceOf("lv_plain");
                final long replayRowsBefore = keyed.getO3ReplayScanRows();

                // Three forward turns above everything either view holds, the middle one
                // carrying two rows of one account at one instant.
                commit(row(5, 1, 0, 0, "acct-1"), job);
                commit(row(5, 1, 0, 1, "acct-1") + ", " + row(5, 1, 0, 1, "acct-1"), job);
                commit(row(5, 1, 0, 2, "acct-2"), job);

                Assert.assertEquals(
                        "forward rows only: an equal timestamp is not an out-of-order one",
                        replayRowsBefore,
                        keyed.getO3ReplayScanRows()
                );
                Assert.assertEquals(
                        "the pair reached the keyed arm's table whole",
                        2,
                        rowsAt("lv", "2026-01-05T01:00:01.000000Z", "acct-1")
                );
                Assert.assertEquals(
                        "the identity costs the forward path no extra transaction, and saves it"
                                + " none either",
                        liveViewWriterTxn("lv_plain"),
                        liveViewWriterTxn("lv")
                );
                Assert.assertEquals(0, keyed.getCheckpointRowCountMismatches());
                Assert.assertEquals(0, plain.getCheckpointRowCountMismatches());
                Assert.assertEquals(
                        "every seal in the run stamped a position its own table can account for",
                        durableRows("lv"),
                        keyed.getLvRowsTotal()
                );
                Assert.assertEquals(durableRows("lv_plain"), plain.getLvRowsTotal());
                assertArmsAgree();
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

    /**
     * Turns the CREATE-time identity on and puts the keyed read behind it, which is the
     * pair a sparse publication needs: the identity gives it a pair to upsert on, and the
     * keyed read is what leaves a smaller set of rows to publish.
     */
    private void armSparseRepair() {
        armSparsePublication();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
    }

    /**
     * The differential the item 7 cases stand on: the two arms hold the same output, and
     * each holds the output its base says it should. The second half is not redundant - two
     * arms that lost the same row agree with each other and with nothing else - and the
     * first is what the from-base oracle cannot say, which is that a divergence came from
     * the dedup keys rather than from the workload.
     */
    private void assertArmsAgree() throws Exception {
        TestUtils.assertEquals(dumpView("lv_plain"), dumpView("lv"));
        Assert.assertEquals(durableRows("lv_plain"), durableRows("lv"));
        assertViewMatchesRecompute("lv");
        assertViewMatchesRecompute("lv_plain");
    }

    /**
     * Holds every timeline boundary to the number of live-view rows at or below its own
     * timestamp, read off the published ladder and off the table it describes.
     */
    private void assertLadderCountsRowsAtOrBelowEachBoundary(String stage) throws Exception {
        final LongList ladder = snapshotCheckpointLadder(engine.getLiveViewRegistry().getViewInstance("lv"));
        Assert.assertTrue(stage + ": the view must have sealed a ladder to check", ladder.size() > 0);
        for (int i = 0, n = ladder.size() / 2; i < n; i++) {
            final long maxTimestamp = ladder.getQuick(i * 2);
            Assert.assertEquals(
                    stage + ": boundary " + i + " at " + maxTimestamp
                            + " must count the rows at or below it",
                    count("select count() from lv where created_at <= " + maxTimestamp + "::timestamp"),
                    ladder.getQuick(i * 2 + 1)
            );
        }
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute("lv");
    }

    private void assertViewMatchesRecompute(String viewName) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1, 3",
                "(" + viewName + ") order by 2, 1, 3",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
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
        createBase(seedRows, "");
    }

    /**
     * The base table, optionally with a dedup clause of its own. A DEDUP base is how a case
     * reaches the <b>coupled</b> refresh cadence: {@code isDedupBase} makes the view read the
     * applied base and commit every cycle, so its rows never pass through the in-memory tier's
     * un-flushed lead.
     */
    private void createBase(String seedRows, String dedupClause) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 8, "
                + "amount double) timestamp(created_at) partition by hour wal" + dedupClause);
        execute("insert into tx values " + seedRows);
        drainWalQueue();
    }

    /**
     * The base and both arms of an item 7 differential: {@code lv_plain} created with the
     * identity switched off and {@code lv} with it on, over one base, through one SELECT, at
     * one cadence. The switch is read once per CREATE, so toggling it between the two is what
     * puts the dedup keys on one table and not the other; nothing else about the two views
     * differs, which is what makes a divergence between them attributable.
     */
    private void createBothArms(String seedRows) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        createBase(seedRows);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        createViewOverBase("lv_plain", "100ms");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        createViewOverBase("lv", "100ms");
    }

    /**
     * The same view over a base that deduplicates on {@code (created_at, account_id, amount)}.
     * The third key is what lets the base keep two rows at one instant for one account, which is
     * the pair the view's own output then repeats; keying on the first two alone would collapse
     * the case's input before the view ever saw it.
     */
    private void createDedupBaseView(String seedRows) throws Exception {
        createBase(seedRows, " dedup upsert keys(created_at, account_id, amount)");
        createViewOverBase("100ms");
    }

    /**
     * The same view with the key left out of its SELECT. The window still partitions on it;
     * what the output does not carry is a column the pair could be named through.
     */
    private void createKeylessView(String seedRows) throws Exception {
        createBase(seedRows);
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, sum(amount) over w as cumulative_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
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
        createViewOverBase("100ms");
    }

    /**
     * The view, at the given FLUSH EVERY cadence. An interval longer than the drive loop's own
     * clock step leaves the drained rows in the tier as an un-flushed lead, which is what a
     * delayed flush case needs.
     */
    private void createViewOverBase(String flushEvery) throws Exception {
        createViewOverBase("lv", flushEvery);
    }

    private void createViewOverBase(String viewName, String flushEvery) throws Exception {
        execute("create live view " + viewName + " flush every " + flushEvery + " start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
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
     * The rows the view's own table holds, read straight off it rather than through the
     * view: a live-view SELECT merges the un-flushed in-memory tier, and what the seal's
     * invariant compares against is the durable output alone.
     */
    private long durableRows() {
        return durableRows("lv");
    }

    private long durableRows(String viewName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(viewName))) {
            return reader.size();
        }
    }

    /**
     * The named view's output as text, in a total order - the two arms of a differential
     * hold the same rows or they do not. Read through the view rather than off its table so
     * an un-flushed lead row counts as one the view holds.
     */
    private String dumpView(String viewName) throws Exception {
        return TestUtils.printSqlToString(
                engine,
                sqlExecutionContext,
                "select created_at, account_id, cumulative_sum from " + viewName + " order by 1, 2, 3",
                new StringSink()
        );
    }

    /**
     * The view's stored rows for one account, as text - the image a publication that does
     * not name that account must leave exactly where it found it.
     */
    private String dumpRowsOf(String account) throws Exception {
        return TestUtils.printSqlToString(
                engine,
                sqlExecutionContext,
                "select * from lv where account_id = '" + account + "' order by 1, 3",
                new StringSink()
        );
    }

    private LiveViewInstance instanceOf(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        return instance;
    }

    /**
     * The live view table's own writer transaction. Two forward rows that went out in separate
     * commits leave it further along than two that shared a block, which is what separates a
     * pair split across turns from one an apply saw whole.
     */
    private long liveViewWriterTxn() {
        return liveViewWriterTxn("lv");
    }

    private long liveViewWriterTxn(String viewName) {
        return engine.getTableSequencerAPI()
                .getTxnTracker(engine.verifyTableName(viewName))
                .getWriterTxn();
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
        return row(day, hour, minute, second, account, 1.0);
    }

    private String row(int day, int hour, int minute, int second, String account, double amount) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":" + String.format("%02d", second)
                + ".000000Z', '" + account + "', " + amount + ")";
    }

    /**
     * The view's rows carrying one {@code (created_at, account_id)} pair, read through the view
     * so an un-flushed lead row counts as one the view holds.
     */
    private long rowsAt(String timestamp, String account) throws Exception {
        return rowsAt("lv", timestamp, account);
    }

    private long rowsAt(String viewName, String timestamp, String account) throws Exception {
        return count("select count() from " + viewName + " where account_id = '" + account + "'"
                + " and created_at = '" + timestamp + "'::timestamp");
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
