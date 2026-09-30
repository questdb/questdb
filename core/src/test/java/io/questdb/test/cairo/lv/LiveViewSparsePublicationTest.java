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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

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
    // Forward commits the cost differential runs. Enough that a per-commit divergence
    // between the arms cannot hide inside a single boundary case, and small enough that the
    // case stays a sub-second unit test.
    private static final int FORWARD_COMMITS = 32;
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
    public void testADetachedPartitionUnderACorrectedKeyAbandonsTheSparseAttempt() throws Exception {
        // DETACH PARTITION reaches the view the way DROP PARTITION does: a non-DATA commit
        // the view walks past, keeping what it derived from the partition's rows.
        armSparseRepair();
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DETACH PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0),
                true,
                false
        );
    }

    @Test
    public void testADroppedPartitionUnderACorrectedKeyAbandonsTheSparseAttempt() throws Exception {
        // The view walks the DROP PARTITION and keeps the acct-1 row it derived from the
        // dropped hour, as it keeps every row derived from removed base data. A later
        // correction of acct-1 on the same closed day then recomputes acct-1's rows from a
        // base that no longer holds that hour, so the replay emits no row carrying the
        // stored row's (timestamp, key) pair. An upsert would leave that stored row in
        // place beside the recomputed ones, and the repair's own row arithmetic - which
        // counts every stored acct-1 row as superseded - would then disagree with the table
        // and retire the timeline, which a restart then meets as a view it may not rebuild.
        // The repair has to notice before it commits and publish the replacement instead.
        armSparseRepair();
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0),
                true,
                false
        );
    }

    @Test
    public void testADroppedPartitionUnderACorrectedKeyAbandonsTheSparseAttemptAcrossParks() throws Exception {
        // One replayed row per refresh turn, so the repair parks after every row it emits,
        // and whatever the merge has paired or left waiting crosses each park inside it.
        armSparseRepair();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0),
                true,
                true
        );
    }

    @Test
    public void testADroppedPartitionUnderACorrectedKeyOfAViewWithoutTheDedupKeysIsReplaced() throws Exception {
        // The control: the same drop and the same correction on a view created without
        // the identity. Its keyed repair never attempts an upsert, so the replacement
        // deletes the row the view derived from the dropped hour and the timeline splices.
        // The sparse attempt, once it abandons, has to land exactly here.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0),
                false,
                false
        );
    }

    @Test
    public void testATtlEvictedPartitionUnderACorrectedKeyAbandonsTheSparseAttempt() throws Exception {
        // TTL evicts partitions while the apply commits a DATA transaction, with no
        // sequencer entry of its own, so no walk of the base's transaction log sees the
        // removal: every pass over it is insert-only. The stored acct-1 row at 10:00
        // outlives its evicted base row all the same. The correction below it lands in an
        // hour past the TTL horizon too, so the apply writes it and evicts it again, and
        // the repair it triggers recomputes acct-1 from what the base still holds.
        armSparseRepair();
        assertMemoryLeak(() -> {
            final StringBuilder seed = new StringBuilder();
            seed.append(row(2, 10, 0, 0, "acct-1", 2.0))
                    .append(", ").append(row(2, 12, 0, 0, "acct-1", 4.0));
            for (int minute = 1; minute < 60; minute++) {
                seed.append(", ").append(row(2, 12, minute, 0, "acct-3"));
            }
            seed.append(", ").append(row(2, 13, 0, 0, "acct-2", 32.0))
                    .append(", ").append(row(3, 1, 0, 0, "acct-1"))
                    .append(", ").append(row(3, 1, 10, 0, "acct-2"));
            createBase(seed.toString());
            execute("ALTER TABLE tx SET TTL 24 HOURS");
            drainWalQueue();
            createViewOverBase("100ms");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // TTL measures age against the lower of the newest row and the wall clock, and
                // the simulated clock starts far below the data.
                setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
                // The head. Its apply ages the 10:00 hour of 2026-01-02 past the horizon and
                // keeps the 12:00 hour, which ends 23 hours below it.
                commit(row(3, 12, 0, 0, "acct-2"), job);
                Assert.assertEquals(1, rowsAt("2026-01-02T10:00:00.000000Z", "acct-1"));

                assertCorrectionRepairedByReplacement(
                        job,
                        row(2, 5, 0, 0, "acct-1", 100.0),
                        "2026-01-02T10:00:00.000000Z",
                        "acct-1",
                        true
                );
                Assert.assertEquals(
                        "TTL must have evicted the 10:00 hour and the correction's own",
                        0,
                        count("select count() from tx where created_at < '2026-01-02T12:00:00.000000Z'")
                );
            }
            // Short of the TTL horizon's reach, so the restart's own row evicts nothing.
            assertRestartRestoresFromTimeline(row(3, 12, 10, 0, "acct-2"));
        });
    }

    @Test
    public void testAColdKeyedHeadMissOverADetachedPartitionReplacesItsRange() throws Exception {
        // DETACH PARTITION takes the acct-1 row at 10:00 the way DROP PARTITION does, and the
        // cold route has to notice it the same way.
        armSparseRepair();
        assertOpenDayCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DETACH PARTITION LIST '2026-01-05T10'"
        );
    }

    @Test
    public void testAColdKeyedHeadMissOverADroppedPartitionReplacesItsRange() throws Exception {
        // The open-day sibling of the closed-segment cases above. The correction at 05:00 finds
        // no checkpoint below it, so its repair would replay acct-1 cold from the day's origin
        // and derive every checkpoint position from the exact insert delta, never walking the
        // stored rows. The stored acct-1 row at 10:00 outlives its dropped base row, so an
        // upsert would leave it beside the recomputed rows while that arithmetic still
        // balanced: no fault, and a timeline that restores the wrong rows. The repair has to
        // count before it replays, and replace the range instead.
        armSparseRepair();
        assertOpenDayCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-05T10'"
        );
    }

    @Test
    public void testAColdKeyedHeadMissOverAnAttachedPartitionReplacesItsRange() throws Exception {
        // The count has to balance in both directions, not only when the view holds more rows
        // than the base. The acct-1 row at 10:00 was detached before the view existed, so the
        // view never derived a row from it, and ATTACH PARTITION brings it back as a non-DATA
        // commit the view walks past. The cold route would recompute acct-1 with it and add
        // one row more than its insert delta says, which only its post-apply row count would
        // notice, after the commit. The count has to decline first, and the replacement picks
        // the row up.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createBase(seedAnOpenDayWithALoneRowAtTen());
            execute("ALTER TABLE tx DETACH PARTITION LIST '2026-01-05T10'");
            drainWalQueue();
            createViewOverBase("100ms");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final TableToken baseToken = engine.verifyTableName("tx");
                try (Path detached = new Path(); Path attachable = new Path()) {
                    detached.of(configuration.getDbRoot()).concat(baseToken).concat("2026-01-05T10")
                            .put(TableUtils.DETACHED_DIR_MARKER).$();
                    attachable.of(configuration.getDbRoot()).concat(baseToken).concat("2026-01-05T10")
                            .put(configuration.getAttachPartitionSuffix()).$();
                    Assert.assertTrue(Files.rename(detached.$(), attachable.$()) > -1);
                }
                execute("ALTER TABLE tx ATTACH PARTITION LIST '2026-01-05T10'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, count("select count() from tx where created_at = '2026-01-05T10:00:00.000000Z'"));
                Assert.assertEquals(
                        "the view never derived a row from the attached base row",
                        0,
                        rowsAt("2026-01-05T10:00:00.000000Z", "acct-1")
                );

                commit(row(5, 5, 0, 0, "acct-1", 100.0), job);

                Assert.assertEquals(1, rowsAt("2026-01-05T10:00:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
                assertColdKeyedRouteDeclined(job);
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(5, 14, 0, 0, "acct-2"));
        });
    }

    @Test
    public void testAColdKeyedHeadMissOverARemovalBelowItsFloorStillPublishesSparsely() throws Exception {
        // The drop takes the acct-1 row at 01:00, below the correction at 05:00 and so below
        // every row the repair rewrites. The rows the cold route recomputes and the rows it
        // leaves in place are exactly what replacing [05:00, +inf) would leave, so the count
        // has to balance and the route has to stay. The stored 01:00 row stays either way: a
        // repair converges only the range it replaces.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAnOpenDayWithALoneRowAtTen());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-05T01'");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                commit(row(5, 5, 0, 0, "acct-1", 100.0), job);

                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                TestUtils.assertEquals(
                        """
                                created_at\taccount_id\tcumulative_sum
                                2026-01-02T01:00:00.000000Z\tacct-1\t1.0
                                2026-01-05T01:00:00.000000Z\tacct-1\t1.0
                                2026-01-05T05:00:00.000000Z\tacct-1\t100.0
                                2026-01-05T10:00:00.000000Z\tacct-1\t102.0
                                2026-01-05T12:00:00.000000Z\tacct-1\t106.0
                                """,
                        dumpRowsOf("acct-1")
                );
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
        });
    }

    @Test
    public void testAColdKeyedHeadMissOverATtlEvictedPartitionReplacesItsRange() throws Exception {
        // TTL evicts the oldest hours of the open day while the apply commits a DATA
        // transaction, so no walk of the base's log sees it, and the view keeps the rows it
        // derived from them. The correction at 00:30 lands past the TTL horizon too, so the
        // apply writes it and evicts it again: the insert delta counts a row the base no
        // longer holds, beside stored rows whose base rows are gone.
        armSparseRepair();
        assertMemoryLeak(() -> {
            final StringBuilder seed = new StringBuilder();
            seed.append(row(5, 1, 0, 0, "acct-1", 1.0))
                    .append(", ").append(row(5, 2, 0, 0, "acct-2", 8.0))
                    .append(", ").append(row(5, 10, 0, 0, "acct-1", 2.0));
            for (int minute = 0; minute < 60; minute++) {
                seed.append(", ").append(row(5, 11, minute, 0, "acct-3"));
            }
            seed.append(", ").append(row(5, 12, 0, 0, "acct-1", 4.0))
                    .append(", ").append(row(5, 13, 0, 0, "acct-2", 32.0));
            createBase(seed.toString());
            // Twelve hours below the newest row at 13:00 is 01:00, so the seed keeps every hour.
            execute("ALTER TABLE tx SET TTL 12 HOURS");
            drainWalQueue();
            createViewOverBase("100ms");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // TTL measures age against the lower of the newest row and the wall clock.
                setCurrentMicros(ts("2026-01-06T00:00:00.000000Z"));
                // The head. Its apply moves the horizon to 04:00, which ages the 01:00 and
                // 02:00 hours out and keeps every hour from 10:00 up.
                commit(row(5, 16, 0, 0, "acct-2"), job);
                Assert.assertEquals(
                        "TTL must have evicted the 01:00 and 02:00 hours",
                        0,
                        count("select count() from tx where created_at < '2026-01-05T10:00:00.000000Z'")
                );
                Assert.assertEquals(
                        "the view keeps the row it derived from the evicted base row",
                        1,
                        rowsAt("2026-01-05T01:00:00.000000Z", "acct-1")
                );

                commit(row(5, 0, 30, 0, "acct-1", 100.0), job);

                Assert.assertEquals(
                        "TTL must have evicted the correction's own hour",
                        0,
                        count("select count() from tx where created_at < '2026-01-05T10:00:00.000000Z'")
                );
                Assert.assertEquals(0, rowsAt("2026-01-05T01:00:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
                assertColdKeyedRouteDeclined(job);
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            // Short of the TTL horizon's reach, so the restart's own row evicts nothing.
            assertRestartRestoresFromTimeline(row(5, 17, 0, 0, "acct-2"));
        });
    }

    @Test
    public void testAColdKeyedHeadMissWhoseFloorSitsInAParquetPartitionReplacesItsRange() throws Exception {
        // The count needs the first base row at or above the correction, and a Parquet
        // partition has no mapped timestamp column to binary-search for it. The correction
        // lands in one, so the count is unavailable and the cold route has to decline. The
        // fixture holds no base row below the correction and one stored row above it whose
        // base row is gone, which is the shape where taking the unsearchable partition's -1
        // for a row count would balance the comparison.
        armSparseRepair();
        assertMemoryLeak(() -> {
            final StringBuilder seed = new StringBuilder();
            seed.append(row(5, 2, 30, 0, "acct-2", 8.0))
                    .append(", ").append(row(5, 3, 0, 0, "acct-3", 64.0));
            for (int minute = 1; minute < 60; minute++) {
                seed.append(", ").append(row(5, 3, minute, 0, "acct-3"));
            }
            seed.append(", ").append(row(5, 10, 0, 0, "acct-1", 2.0))
                    .append(", ").append(row(5, 12, 0, 0, "acct-1", 4.0))
                    .append(", ").append(row(5, 13, 0, 0, "acct-2", 32.0));
            createView(seed.toString());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                execute("ALTER TABLE tx CONVERT PARTITION TO PARQUET LIST '2026-01-05T02'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-05T10'");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                commit(row(5, 2, 0, 0, "acct-1", 100.0), job);

                Assert.assertEquals(
                        "the correction must have landed in the Parquet partition, or the count can search it",
                        1,
                        count("select count() from table_partitions('tx') where name = '2026-01-05T02' and isParquet")
                );
                Assert.assertEquals(0, rowsAt("2026-01-05T10:00:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
                assertColdKeyedRouteDeclined(job);
            }
        });
    }

    @Test
    public void testAColdKeyedHeadMissWithoutARemovalStillPublishesSparsely() throws Exception {
        // The control for the cold cases above: the same open day and the same correction with
        // nothing removed. The count the cold route takes before it replays has to balance
        // here, or every cold repair would pay for the whole-range replacement it exists to
        // avoid.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAnOpenDayWithALoneRowAtTen());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                commit(row(5, 5, 0, 0, "acct-1", 100.0), job);

                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                assertViewMatchesRecompute();
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(5, 14, 0, 0, "acct-2"));
        });
    }

    @Test
    public void testAKeyedResumeOverADroppedPartitionReplacesItsRange() throws Exception {
        // The anchored sibling of the cold case: the correction at 02:35 on the open day
        // resumes from the root at 01:40, follows acct-1 alone and derives its checkpoint
        // positions from the exact insert delta, as the cold route does. The drop took hour
        // 05, one row of each account, after the view had derived rows from it.
        armSparseRepair();
        assertOpenDayResumeAfter("ALTER TABLE tx DROP PARTITION LIST '2026-01-04T05'");
    }

    @Test
    public void testAKeyedResumeWithoutARemovalStillPublishesSparsely() throws Exception {
        // The control for the case above: nothing removed, so the count balances and the
        // resume follows acct-1 alone.
        armSparseRepair();
        assertOpenDayResumeAfter(null);
    }

    @Test
    public void testAKeyedResumeWhoseStoredRowCountFaultsReadsTheRangeWhole() throws Exception {
        // The count the keyed resume takes once it has armed its replay opens a reader of the
        // view's table and a partition of each table, and any of those can fail. A count that
        // could not be taken proves nothing about the stored rows, so the resume declines the
        // keyed route and reads every key above the anchor, as it does when it cannot measure
        // its durable coordinate: no refresh fault, and no replay left armed on the worker.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(hoursOfFourAccounts(2, 0, 10) + ", " + hoursOfFourAccounts(3, 0, 10));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                for (int hour = 0; hour < 10; hour++) {
                    commit(hoursOfFourAccounts(4, hour, hour + 1), job);
                }
                job.setSimulateStoredSuffixCountFaultForTest();

                execute("insert into tx values " + row(4, 2, 35, 0, "acct-1"));
                drainWalQueue();
                advanceClockToNextRefreshPass();
                drainJob(job);

                Assert.assertFalse(
                        "the count must have faulted, or the case covers nothing",
                        job.isStoredSuffixCountFaultArmedForTest()
                );
                assertNoRefreshFaults("lv");
                Assert.assertFalse(
                        "a count that faulted must leave the keyed replay unarmed",
                        job.isKeyedReplayArmedForTest()
                );
                Assert.assertEquals(
                        "the keyed resume must have priced cheaper, or the case covers nothing",
                        1,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                Assert.assertEquals(
                        "a count that faulted rules the keyed resume out",
                        0,
                        job.openSegmentKeyedResumeCountForTest()
                );
                Assert.assertEquals(0, job.openSegmentArithmeticRowPositionCountForTest());
                Assert.assertEquals(0, job.sparsePublicationCountForTest());
                Assert.assertNull(
                        "a declined resume must not build the isolated runtime a keyed one replays in",
                        instanceOf("lv").getRepairRuntime()
                );

                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(4, 10, 10, 0, "acct-1"));
        });
    }

    @Test
    public void testAColdKeyedHeadMissWhoseStoredRowCountFaultsReplacesItsRange() throws Exception {
        // The cold route takes the same count before it replays, and a count that could not be
        // taken declines the route there too, as a floor the count cannot search does. The
        // repair replaces its range whole rather than failing.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedAnOpenDayWithALoneRowAtTen());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                job.setSimulateStoredSuffixCountFaultForTest();

                commit(row(5, 5, 0, 0, "acct-1", 100.0), job);

                Assert.assertFalse(
                        "the count must have faulted, or the case covers nothing",
                        job.isStoredSuffixCountFaultArmedForTest()
                );
                assertViewMatchesRecompute();
                assertColdKeyedRouteDeclined(job);
                Assert.assertFalse(
                        "a count that faulted must leave the keyed replay unarmed",
                        job.isKeyedReplayArmedForTest()
                );
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(5, 14, 0, 0, "acct-2"));
        });
    }

    @Test
    public void testAFaultedKeyedResumeLeavesTheNextViewsResumeUnfaulted() throws Exception {
        // The worker keeps one keyed replay for every view it repairs. lv's keyed resume arms
        // it and binds a sparse publication, then faults as its replay starts. The next repair
        // on the worker is lv_plain's: a view without the dedup keys, over another base, whose
        // resume never arms the replay. A replay left armed and sparse would have that resume
        // abandon a sparse publication it never attempted and fault the view, until lv's own
        // retry re-armed the replay and so cleared it.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createLvBesideASecondBase();
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
            createViewOverBase("lv_plain", "tx2", "100ms");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final boolean isArmedAfterFault = faultAKeyedResumeOfLv(job);

                // lv waits out its retry backoff, so a drain that leaves the clock alone runs
                // lv_plain's repair and not lv's retry.
                execute("insert into tx2 values " + row(4, 2, 35, 0, "acct-1"));
                drainWalQueue();
                drainJob(job);
                assertNoRefreshFaults("lv_plain");

                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute("lv_plain", "tx2");
                Assert.assertEquals("lv faults once, where the case injected the fault", 1, instanceOf("lv").getRefreshFaultCount());
                assertViewRowsMatchRecompute("lv", "tx");
                Assert.assertFalse(
                        "a keyed resume that faulted must not leave the worker's keyed replay armed",
                        isArmedAfterFault
                );
            }
        });
    }

    @Test
    public void testAFaultedKeyedResumeOfADroppedViewLeavesTheNextViewValid() throws Exception {
        // The case above, with lv dropped while it waits out its retry, so no resume of lv runs
        // again to re-arm the worker's keyed replay and so clear it. A replay left armed and
        // sparse would fault every resume of lv_plain until its retry budget ran out and the
        // view was invalidated: a transient fault of one view stopping another for good.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createLvBesideASecondBase();
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
            createViewOverBase("lv_plain", "tx2", "100ms");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final boolean isArmedAfterFault = faultAKeyedResumeOfLv(job);
                execute("drop live view lv");
                drainWalQueue();
                drainJob(job);

                commit("tx2", row(4, 2, 35, 0, "acct-1"), job);

                Assert.assertFalse("lv's fault must not invalidate lv_plain", instanceOf("lv_plain").isInvalid());
                assertViewMatchesRecompute("lv_plain", "tx2");
                Assert.assertFalse(
                        "a keyed resume that faulted must not leave the worker's keyed replay armed",
                        isArmedAfterFault
                );
            }
        });
    }

    @Test
    public void testAFaultedKeyedResumeLeavesTheNextViewsRangeRepairWhole() throws Exception {
        // The next repair on the worker is lv_range's: a RANGE-frame view without the dedup
        // keys, over another base, whose localized repair of an acct-2 correction reads its
        // interval whole and replaces it. A replay left armed and sparse would take that repair
        // onto lv's acct-1 key domain and publish it as an upsert, which a table without dedup
        // keys applies as a plain insert: the rows the repair recomputed would land beside the
        // rows they were meant to replace, and no fault would say so.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createLvBesideASecondBase();
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
            final String rangeSelect = "SELECT created_at, account_id, sum(amount) OVER (PARTITION BY account_id "
                    + "ORDER BY created_at RANGE BETWEEN '7200' SECOND PRECEDING AND CURRENT ROW) AS s FROM tx2";
            execute("CREATE LIVE VIEW lv_range FLUSH EVERY 100ms START FROM BEGINNING AS " + rangeSelect);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final boolean isArmedAfterFault = faultAKeyedResumeOfLv(job);

                execute("insert into tx2 values " + row(4, 2, 35, 0, "acct-2", 1000.0));
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals(
                        "lv_range carries no dedup keys, so its repair must not publish sparsely",
                        0,
                        job.sparsePublicationCountForTest()
                );

                driveRefreshToQuiescence(job);
                Assert.assertEquals(count("select count() from tx2"), durableRows("lv_range"));
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + rangeSelect + ") order by 2, 1, 3",
                        "(lv_range) order by 2, 1, 3",
                        LOG,
                        true
                );
                assertNoRefreshFaults("lv_range");
                Assert.assertEquals("lv faults once, where the case injected the fault", 1, instanceOf("lv").getRefreshFaultCount());
                assertViewRowsMatchRecompute("lv", "tx");
                Assert.assertFalse(
                        "a keyed resume that faulted must not leave the worker's keyed replay armed",
                        isArmedAfterFault
                );
            }
        });
    }

    @Test
    public void testANullKeyStoredRowTheReplayReEmitsStillPublishesSparsely() throws Exception {
        // A row without an account carries NULL as its key. The base and the view both
        // encode NULL as the same integer, VALUE_IS_NULL, so this case exercises no
        // translation between their symbol maps. What it pins is the lookup the pairing makes
        // for a replayed NULL: it has to resolve to the view's NULL key rather than to a
        // value the view has never stored, or every stored NULL row would read as unpaired
        // and a NULL-key correction could never publish sparsely. The cases over
        // seedWithALoneRowAtTenInAnotherSymbolOrder() pin the translation itself.
        armSparseRepair();
        assertCorrectionPublishesSparsely(
                seedWithALoneRowAtTenAndNullKeyRows(),
                row(2, 4, 0, 0, null, 100.0),
                1,
                false
        );
    }

    @Test
    public void testANullKeyStoredRowWithoutAPairAbandonsTheSparseAttempt() throws Exception {
        // The removal case with NULL as the corrected key: the view keeps the NULL row it
        // derived from the dropped 11:00 hour, and the replay emits no NULL row there.
        armSparseRepair();
        assertCorrectionOverARemovedRowRepairsByReplacement(
                seedWithALoneRowAtTenAndNullKeyRows(),
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T11'",
                "2026-01-02T11:00:00.000000Z",
                null,
                row(2, 4, 0, 0, null, 100.0),
                true,
                false
        );
    }

    @Test
    public void testAStaleRowWaitingAtAnInstantTheDrainWalksPastAbandonsTheSparseAttempt() throws Exception {
        // The late acct-2 row repopulates the dropped 10:00 instant, so the stale acct-1 row
        // there sits at the bound of the drain ahead of it and waits for a pair instead of
        // being ruled out on the spot. The acct-2 row does not pair it. The replay's next row
        // is acct-1's at 12:00, and the drain ahead of that one walks onto the stored acct-1
        // row at 12:00: walking past the waiting instant is what closes the wait unpaired.
        armSparseRepair();
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0) + ", " + row(2, 10, 0, 0, "acct-2", 100.0),
                true,
                false
        );
    }

    @Test
    public void testAStaleRowWaitingAtAnInstantTheReplayMovesPastAbandonsTheSparseAttempt() throws Exception {
        // The same wait at 10:00, but a second late acct-2 row at 11:00 comes next, and no
        // stored row of either corrected key sits between the two. The drain ahead of the
        // 11:00 row walks nothing, so the replayed row landing past the waiting instant is
        // what closes the wait unpaired.
        armSparseRepair();
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0)
                        + ", " + row(2, 10, 0, 0, "acct-2", 100.0)
                        + ", " + row(2, 11, 0, 0, "acct-2", 100.0),
                true,
                false
        );
    }

    @Test
    public void testAStaleRowWaitingAtAnInstantTheReplayMovesPastWithItsOwnKeyAbandonsTheSparseAttempt() throws Exception {
        // The same wait at 10:00, but the replay moves past it with a late row of acct-1
        // itself at 11:00, and no stored row of either corrected key sits between the two.
        // The drain ahead of the 11:00 row walks nothing, so only the replayed row's later
        // timestamp can close the wait. A pairing that matched the 11:00 row to the waiting
        // acct-1 row by key alone would clear the wait, and the upsert would then keep the
        // stale 10:00 row beside the recomputed ones.
        armSparseRepair();
        assertCorrectionOverARemovedRowRepairsByReplacement(
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'",
                row(2, 5, 0, 0, "acct-1", 100.0)
                        + ", " + row(2, 10, 0, 0, "acct-2", 100.0)
                        + ", " + row(2, 11, 0, 0, "acct-1", 100.0),
                true,
                false
        );
    }

    @Test
    public void testAStaleRowWaitingAtTheLastReplayedInstantAbandonsTheSparseAttempt() throws Exception {
        // The drop takes the acct-2 row at 13:00, and the late acct-1 row repopulates that
        // instant, so the stale acct-2 row waits there for a pair. The acct-1 row is the last
        // one the replay emits, and no stored row of either corrected key sits above it on
        // that day: nothing moves past 13:00, and only the final drain is left to close the
        // wait unpaired.
        armSparseRepair();
        assertStaleRowWaitingAtTheLastReplayedInstantRepairsByReplacement(seedWithALoneRowAtTen(), "acct-1", false);
    }

    @Test
    public void testAStaleRowWaitingAtTheLastReplayedInstantAbandonsTheSparseAttemptAcrossAPark() throws Exception {
        // The same wait with a park inside it: one replayed row per refresh turn, so the
        // repair parks right after it emits the acct-1 row at 13:00, with the stale acct-2
        // row still waiting there. The final drain runs on the turn that resumes it, so the
        // wait has to cross the park intact for that drain to close it unpaired.
        armSparseRepair();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertStaleRowWaitingAtTheLastReplayedInstantRepairsByReplacement(seedWithALoneRowAtTen(), "acct-1", true);
    }

    @Test
    public void testAStoredRepeatAtARemovedInstantThatOneLateRowRepopulatesAbandonsTheSparseAttempt() throws Exception {
        // Two base rows of acct-1 at 10:00 give the view two rows under one (timestamp, key)
        // pair, which the forward path keeps. The drop takes both base rows, and the view
        // keeps both stored rows. The correction then puts back one acct-1 row at 10:00, so
        // the replay emits the pair once: its output is unique, and its one row at 10:00 is
        // the pair both stored rows wait for. The upsert would give each stored row that
        // one row's values and leave two identical rows where the base holds one. A
        // pairing that counted the stored repeat as one waiting row would let the single
        // replayed row pair both.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedWithALoneRowAtTen() + ", " + row(2, 10, 0, 0, "acct-1", 3.0));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // The head, which closes 2026-01-02 below it.
                commit(row(5, 1, 0, 0, "acct-1"), job);
                execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-02T10'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the view keeps both rows it derived from the removed base rows",
                        2,
                        rowsAt("2026-01-02T10:00:00.000000Z", "acct-1")
                );

                commit(row(2, 5, 0, 0, "acct-1", 100.0) + ", " + row(2, 10, 0, 0, "acct-1", 7.0), job);

                Assert.assertEquals(
                        "the correction must be repaired by key, or the case covers nothing",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the replay's own output names each pair once, so only the stored repeat can rule the upsert out",
                        0,
                        job.outputUniquenessDuplicateRowsForTest()
                );
                Assert.assertEquals(
                        "a stored repeat that the replay emits once rules the upsert out",
                        0,
                        job.sparsePublicationCountForTest()
                );
                Assert.assertEquals(1, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(
                        "the replacement keeps the one row the base now holds at the pair",
                        1,
                        rowsAt("2026-01-02T10:00:00.000000Z", "acct-1")
                );
                assertViewMatchesRecompute();
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(5, 2, 0, 0, "acct-2"));
        });
    }

    @Test
    public void testAReplayedRowOfAnotherKeyWhoseBaseSymbolMatchesTheStaleRowsDoesNotPairIt() throws Exception {
        // The wait at the last replayed instant, with acct-9 as the late row's account. The
        // replay emits it under the base's integer for acct-9, which is the integer the view
        // stores acct-2 under. The pairing has to compare the two keys by the value each
        // names: compared by integer, the acct-9 row would pair the stale acct-2 row, and the
        // upsert would keep that row beside the recomputed ones.
        armSparseRepair();
        assertStaleRowWaitingAtTheLastReplayedInstantRepairsByReplacement(
                seedWithALoneRowAtTenInAnotherSymbolOrder(),
                "acct-9",
                false
        );
        assertTheBaseAndTheViewNumberSymbolsInDifferentOrders();
    }

    @Test
    public void testAReplayedKeyResolvedEarlierInTheRepairStillDoesNotPairAnotherKeysStaleRow() throws Exception {
        // The same wait, with stored rows of acct-9 at 12:30 and of acct-2 at 12:45 that the
        // replay re-emits, so the repair resolves each of the two accounts once and caches
        // the answer before 13:00. There the stale acct-2 row waits, and the late acct-9 row
        // resolves acct-9 a second time, through the cache. The base's integer for acct-9 is
        // the view's for acct-2, so the cache has to map the base's integer to the view's:
        // caching the base's integer as the answer for acct-9, or filing the answer for
        // acct-2 under the view's integer, hands the late row the view's acct-2 and pairs
        // the stale row.
        armSparseRepair();
        assertStaleRowWaitingAtTheLastReplayedInstantRepairsByReplacement(
                seedWithALoneRowAtTenInAnotherSymbolOrder()
                        + ", " + row(2, 12, 30, 0, "acct-9", 7.0)
                        + ", " + row(2, 12, 45, 0, "acct-2", 3.0),
                "acct-9",
                false
        );
        assertTheBaseAndTheViewNumberSymbolsInDifferentOrders();
    }

    @Test
    public void testATranslationResolvedForOneViewDoesNotCarryIntoAnotherViewsRepair() throws Exception {
        // One refresh job serves both views, through one keyed replay and the translation
        // cache it holds. lv2 repairs first and pairs its stored acct-2 row at 13:00, which
        // caches tx2's integer for acct-2 as lv2's. lv then takes the wait at the last
        // replayed instant with a late acct-9 row. tx names acct-9 with tx2's integer for
        // acct-2, and lv stores acct-2 under lv2's integer for it, so a cache that outlived
        // lv2's repair would resolve the acct-9 row to lv's acct-2 and pair the stale row.
        armSparseRepair();
        assertMemoryLeak(() -> {
            createView(seedWithALoneRowAtTenInAnotherSymbolOrder());
            createBase("tx2", seedWithALoneRowAtTen(), "");
            createViewOverBase("lv2", "tx2", "100ms");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // The heads, which close 2026-01-02 below them in both views.
                commit("tx2", row(5, 1, 0, 0, "acct-1"), job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-02T13'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the view keeps the row it derived from the removed base row",
                        1,
                        rowsAt("2026-01-02T13:00:00.000000Z", "acct-2")
                );

                commit("tx2", row(2, 5, 0, 0, "acct-2", 100.0), job);
                Assert.assertEquals(
                        "lv2's correction must be repaired by key, or the case covers nothing",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "lv2's repair must pair its stored rows, or it caches no translation",
                        1,
                        job.sparsePublicationCountForTest()
                );
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                assertViewMatchesRecompute("lv2", "tx2");
                Assert.assertEquals(
                        "tx's integer for acct-9 must be tx2's for acct-2, or lv's repair never looks lv2's up",
                        symbolKeyOf("tx2", "acct-2"),
                        symbolKeyOf("tx", "acct-9")
                );
                Assert.assertEquals(
                        "lv's integer for acct-2 must be lv2's, or lv2's answer cannot pair lv's stale row",
                        symbolKeyOf("lv2", "acct-2"),
                        symbolKeyOf("lv", "acct-2")
                );

                commit(row(2, 5, 0, 0, "acct-2", 100.0) + ", " + row(2, 13, 0, 0, "acct-9", 100.0), job);
                Assert.assertEquals(
                        "lv's correction must be repaired by key, or the case covers nothing",
                        2,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "a stored row the replay did not re-emit rules the upsert out",
                        1,
                        job.sparsePublicationCountForTest()
                );
                Assert.assertEquals(1, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(0, rowsAt("2026-01-02T13:00:00.000000Z", "acct-2"));
                assertViewMatchesRecompute();
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(5, 2, 0, 0, "acct-2"));
        });
    }

    @Test
    public void testANarrowSparseRepairAfterOneWiderThanTheRetainedKeyBoundStillPairsItsStoredRows() throws Exception {
        // One refresh job serves both repairs, through one keyed replay. The first correction
        // touches one account past LiveViewCheckpointOutputKeyDomain.MAX_RETAINED_KEYS on a
        // closed day, so the key domain it arms is too wide to keep, and the clear() that ends
        // the repair drops the replay's pairing tables with it rather than holding their
        // storage for the next repair. arm() is the only place that restores them. The second
        // correction touches one of those accounts again, and its replay re-emits the stored
        // w-7 row at 01:00:06, which the pairing has to record through those tables. A replay
        // that never got them back faults on that row on every retry until the refresh budget
        // invalidates the view.
        //
        // The heavy account makes the day expensive to read whole, which is what prices both
        // corrections onto the keyed route: a repair that reads the day whole never pairs.
        // Roots every 10_000 rows keep that day from sealing one per row.
        armSparseRepair();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 10_000);
        final int wideKeys = LiveViewCheckpointOutputKeyDomain.MAX_RETAINED_KEYS + 1;
        final int heavyRows = 50_000;
        assertMemoryLeak(() -> {
            createBase(row(2, 3, 0, 0, "acct-1")
                    + ", " + row(3, 1, 0, 0, "acct-1")
                    + ", " + row(3, 1, 0, 1, "acct-2")
                    + ", " + row(4, 1, 0, 0, "acct-1"));
            execute("INSERT INTO tx SELECT timestamp_sequence('2026-01-02T01:00:00.000000Z', 1_000_000),"
                    + " ('w-' || x)::SYMBOL, 1.0 FROM long_sequence(" + wideKeys + ")");
            execute("INSERT INTO tx SELECT timestamp_sequence('2026-01-02T02:00:00.000000Z', 10_000),"
                    + " 'heavy'::SYMBOL, 1.0 FROM long_sequence(" + heavyRows + ")");
            drainWalQueue();
            createViewOverBase("100ms");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // The head, which closes 2026-01-02 below it.
                commit(row(5, 1, 0, 0, "acct-1"), job);
                Assert.assertTrue(
                        "the view must carry the dedup keys a sparse publication upserts on",
                        instanceOf("lv").isDedupKeyed()
                );

                // One late row of every w- account, below every row the day holds.
                execute("INSERT INTO tx SELECT timestamp_sequence('2026-01-02T00:30:00.000000Z', 1_000),"
                        + " ('w-' || x)::SYMBOL, 2.0 FROM long_sequence(" + wideKeys + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "the wide correction must be repaired by key, or the case covers nothing",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(
                        "every w- account must be replayed by key, so the domain the replay armed is past its retained bound",
                        heavyRows + 1,
                        job.sparsePublicationRowsKeptForTest()
                );
                assertViewMatchesRecompute();

                commit(row(2, 0, 45, 0, "w-7", 3.0), job);

                Assert.assertEquals(
                        "the narrow correction must not fault on the replay the wide one cleared",
                        0,
                        instanceOf("lv").getRefreshFaultCount()
                );
                Assert.assertFalse(instanceOf("lv").isInvalid());
                Assert.assertEquals(
                        "the narrow correction must be repaired by key, or it never pairs a stored row",
                        2,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(2, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv WHERE account_id = 'w-7'")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns("""
                                created_at\taccount_id\tcumulative_sum
                                2026-01-02T00:30:00.006000Z\tw-7\t2.0
                                2026-01-02T00:45:00.000000Z\tw-7\t5.0
                                2026-01-02T01:00:06.000000Z\tw-7\t6.0
                                """);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testStoredRowsOfTwoCorrectedKeysAtOneInstantPairAcrossAPark() throws Exception {
        // The same wait with a park inside it: the replay emits one of the two rows at
        // 10:00, parks, and pairs the other on the turn that resumes it.
        armSparseRepair();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertStoredRowsOfTwoCorrectedKeysAtOneInstantPair(true);
    }

    @Test
    public void testStoredRowsOfTwoCorrectedKeysAtOneInstantPairAndPublishSparsely() throws Exception {
        // The pairing's wide wait: both corrected accounts hold a stored row at 10:00, so
        // both wait at that timestamp until the replay re-emits each of them. Nothing was
        // removed, every pair is found, and the repair publishes sparsely as it would have
        // without the pairing.
        armSparseRepair();
        assertStoredRowsOfTwoCorrectedKeysAtOneInstantPair(false);
    }

    @Test
    public void testStoredRowsPairAcrossDivergentBaseAndViewSymbolOrdersAndPublishSparsely() throws Exception {
        // The replay emits each row under the base's integer for its key, and the view stores
        // acct-2 under a different integer. The pairing has to resolve the replayed row's key
        // into the view's through the value it names, or no stored acct-2 row would find its
        // pair and the correction could never publish sparsely.
        armSparseRepair();
        assertCorrectionPublishesSparsely(
                seedWithALoneRowAtTenInAnotherSymbolOrder(),
                row(2, 5, 0, 0, "acct-2", 100.0),
                1,
                false
        );
        assertTheBaseAndTheViewNumberSymbolsInDifferentOrders();
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

    @Test
    public void testTheIdentityCostsTheForwardPathNoExtraApplyAndNoExtraWrittenRow() throws Exception {
        assertMemoryLeak(() -> {
            createBothArms(seedAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                // The arms really are the two arms: the switch put the pair on one table's own
                // _meta and nothing on the other's.
                Assert.assertEquals("created_at,account_id", dedupKeysOf("lv"));
                Assert.assertEquals("", dedupKeysOf("lv_plain"));

                final long keyedWalBefore = liveViewWriterTxn("lv");
                final long plainWalBefore = liveViewWriterTxn("lv_plain");
                final long keyedAppliesBefore = tableTransactions("lv");
                final long plainAppliesBefore = tableTransactions("lv_plain");
                final long keyedRowsBefore = durableRows("lv");
                final long plainRowsBefore = durableRows("lv_plain");
                long physicallyWrittenRows = 0;

                // Forward-only commits above everything either view holds, strictly increasing,
                // so no repair fires and what is left is the forward path alone. The explicit
                // drainWalQueue above applies the base commit before the window opens.
                // driveRefreshToQuiescence then calls drainWalQueue again INSIDE the window, but
                // it writes nothing there: the base has nothing left to apply, and
                // ApplyWal2TableJob.doRun drops every live-view notification while refresh is
                // enabled, so the refresh worker's own inline apply is the only writer in the
                // window.
                for (int i = 0; i < FORWARD_COMMITS; i++) {
                    execute("insert into tx values "
                            + row(5, 1, 0, i, "acct-1") + ", " + row(5, 1, 0, i, "acct-2"));
                    drainWalQueue();
                    final long writtenBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
                    driveRefreshToQuiescence(job);
                    physicallyWrittenRows +=
                            engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore;
                }

                final long keyedWalTxns = liveViewWriterTxn("lv") - keyedWalBefore;
                final long plainWalTxns = liveViewWriterTxn("lv_plain") - plainWalBefore;
                final long keyedApplies = tableTransactions("lv") - keyedAppliesBefore;
                final long plainApplies = tableTransactions("lv_plain") - plainAppliesBefore;
                final long keyedRows = durableRows("lv") - keyedRowsBefore;
                final long plainRows = durableRows("lv_plain") - plainRowsBefore;

                Assert.assertEquals(
                        "each forward commit must reach the keyed arm as exactly one live-view WAL transaction",
                        FORWARD_COMMITS,
                        keyedWalTxns
                );
                Assert.assertEquals("both arms must see the same commits", keyedWalTxns, plainWalTxns);
                Assert.assertEquals(
                        "the identity must cost the keyed arm no extra apply",
                        plainApplies,
                        keyedApplies
                );
                Assert.assertEquals(
                        "one table transaction per live-view WAL transaction: the keyed arm's"
                                + " FORCE_FULL_COMMIT takes away a lag the forward path never had",
                        keyedWalTxns,
                        keyedApplies
                );
                Assert.assertEquals(
                        "and the plain arm retains nothing in its lag either, which is what makes"
                                + " the equality above a statement about the path rather than a coincidence",
                        plainWalTxns,
                        plainApplies
                );
                Assert.assertEquals("the two arms must append the same rows", plainRows, keyedRows);
                // getPhysicallyWrittenRows counts the whole engine, so this equality also
                // assumes the two views are the only tables written inside the window - true
                // here for the reason the loop comment gives. A third writer would break the
                // equality outright rather than bias it quietly.
                Assert.assertEquals(
                        "neither arm may write a row more than once - the identity must buy no"
                                + " write amplification",
                        keyedRows + plainRows,
                        physicallyWrittenRows
                );
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
     * Holds a correction in the open day to the whole-range replacement a cold keyed route
     * declines to: the route priced cheaper, so it was the one the repair would have taken,
     * and yet it replayed nothing by key, derived no checkpoint position from the insert
     * delta and published nothing sparsely.
     */
    private void assertColdKeyedRouteDeclined(LiveViewRefreshJob job) {
        Assert.assertEquals(
                "the cold keyed route must have priced cheaper, or the case covers nothing",
                1,
                job.openSegmentColdKeyedCheaperCountForTest()
        );
        Assert.assertEquals(
                "stored rows that are not the base rows the view consumed rule the cold keyed route out",
                0,
                job.openSegmentColdKeyedReplayCountForTest()
        );
        Assert.assertEquals(0, job.openSegmentArithmeticRowPositionCountForTest());
        Assert.assertEquals(0, job.sparsePublicationCountForTest());
        Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
    }

    /**
     * The case below over {@link #seedWithALoneRowAtTen()}, with {@code removal} taking the
     * acct-1 row at 10:00.
     */
    private void assertCorrectionOverARemovedRowRepairsByReplacement(
            String removal,
            String correction,
            boolean isSparseAttempted,
            boolean isParked
    ) throws Exception {
        assertCorrectionOverARemovedRowRepairsByReplacement(
                seedWithALoneRowAtTen(),
                removal,
                "2026-01-02T10:00:00.000000Z",
                "acct-1",
                correction,
                isSparseAttempted,
                isParked
        );
    }

    /**
     * Drives a correction over a closed day from which {@code removal} took one row, and
     * holds the repair to what a from-base recompute produces: rows first, then a restart
     * that has to come back on the timeline the repair published rather than stop the view.
     *
     * @param seedRows          the base's rows, which put the removed row alone in its
     *                          hourly partition on 2026-01-02
     * @param removal           the statement that takes the removed row out of the base
     * @param removedAt         the removed row's designated timestamp
     * @param removedAccount    the removed row's account, or null for a row without one
     * @param correction        the late rows on 2026-01-02, naming the removed row's account
     *                          among the keys they correct
     * @param isSparseAttempted whether the view carries the dedup keys, so the repair
     *                          attempts a sparse publication before it falls back
     * @param isParked          whether the repair's replay is budgeted to park between rows
     */
    private void assertCorrectionOverARemovedRowRepairsByReplacement(
            String seedRows,
            String removal,
            String removedAt,
            @Nullable String removedAccount,
            String correction,
            boolean isSparseAttempted,
            boolean isParked
    ) throws Exception {
        assertMemoryLeak(() -> {
            createView(seedRows);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // The head, which closes 2026-01-02 below it.
                commit(row(5, 1, 0, 0, "acct-1"), job);
                execute(removal);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the view keeps the row it derived from the removed base row",
                        1,
                        rowsAt(removedAt, removedAccount)
                );

                assertCorrectionRepairedByReplacement(job, correction, removedAt, removedAccount, isSparseAttempted);
                Assert.assertEquals(
                        "the replay must park where the case asks it to, and only there",
                        isParked,
                        job.segmentYieldCountForTest() > 0
                );
            }
            assertRestartRestoresFromTimeline(row(5, 2, 0, 0, "acct-2"));
        });
    }

    /**
     * Commits {@code correction} over a view seeded with {@code seedRows} and holds its keyed
     * repair to a sparse publication: every stored row of a corrected key found its pair, so
     * the upsert adds the {@code correctedRows} late rows and nothing else, and the view
     * equals a from-base recompute.
     */
    private void assertCorrectionPublishesSparsely(
            String seedRows,
            String correction,
            int correctedRows,
            boolean isParked
    ) throws Exception {
        assertMemoryLeak(() -> {
            createView(seedRows);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, 0, 0, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(correction, job);

                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(isParked, job.segmentYieldCountForTest() > 0);
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(rowsBefore + correctedRows, count("select count() from lv"));
                assertViewMatchesRecompute();
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
        });
    }

    /**
     * Commits {@code correction} and holds its keyed repair to the replacement: no sparse
     * publication, the row the view derived from the removed base row gone, the view equal
     * to a from-base recompute and its ladder counting the rows it describes.
     */
    private void assertCorrectionRepairedByReplacement(
            LiveViewRefreshJob job,
            String correction,
            String removedAt,
            @Nullable String removedAccount,
            boolean isSparseAttempted
    ) throws Exception {
        commit(correction, job);

        Assert.assertEquals(
                "the correction must be repaired by key, or the case covers nothing",
                1,
                job.keyedReplaySegmentCountForTest()
        );
        Assert.assertEquals(
                "a stored row the replay did not re-emit rules the upsert out",
                0,
                job.sparsePublicationCountForTest()
        );
        Assert.assertEquals(isSparseAttempted ? 1 : 0, job.sparsePublicationFallbackCountForTest());
        Assert.assertEquals(0, rowsAt(removedAt, removedAccount));
        assertViewMatchesRecompute();
        assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
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

    /**
     * Drives an acct-1 correction at 05:00 into the open day
     * {@link #seedAnOpenDayWithALoneRowAtTen()} seeds, after {@code removal} took the acct-1
     * row at 10:00 out of the base, and holds the repair to the whole-range replacement: the
     * cold keyed route declined, the row derived from the removed base row gone, the view
     * equal to a from-base recompute, and a restart that restores from the timeline the
     * repair published.
     */
    private void assertOpenDayCorrectionOverARemovedRowRepairsByReplacement(String removal) throws Exception {
        assertMemoryLeak(() -> {
            createView(seedAnOpenDayWithALoneRowAtTen());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                execute(removal);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the view keeps the row it derived from the removed base row",
                        1,
                        rowsAt("2026-01-05T10:00:00.000000Z", "acct-1")
                );

                commit(row(5, 5, 0, 0, "acct-1", 100.0), job);

                Assert.assertEquals(0, rowsAt("2026-01-05T10:00:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
                assertColdKeyedRouteDeclined(job);
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(5, 14, 0, 0, "acct-2"));
        });
    }

    /**
     * Drives the open day 2026-01-04 in order, one commit per hour so the cadence seals a
     * root inside it, then commits an acct-1 correction at 02:35, above the root at 01:40.
     * The repair resumes from that root. With nothing removed it follows acct-1 alone and
     * publishes sparsely; after {@code removal} took hour 05 it has to decline the keyed
     * resume and replace the range, which drops the rows the view derived from that hour.
     *
     * @param removal the statement that takes hour 05 out of the base, or null for none
     */
    private void assertOpenDayResumeAfter(@Nullable String removal) throws Exception {
        assertMemoryLeak(() -> {
            createView(hoursOfFourAccounts(2, 0, 10) + ", " + hoursOfFourAccounts(3, 0, 10));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                for (int hour = 0; hour < 10; hour++) {
                    commit(hoursOfFourAccounts(4, hour, hour + 1), job);
                }
                if (removal != null) {
                    execute(removal);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals(
                            "the view keeps the row it derived from the removed base row",
                            1,
                            rowsAt("2026-01-04T05:10:00.000000Z", "acct-1")
                    );
                }

                commit(row(4, 2, 35, 0, "acct-1"), job);

                if (removal != null) {
                    Assert.assertEquals(0, rowsAt("2026-01-04T05:10:00.000000Z", "acct-1"));
                }
                assertViewMatchesRecompute();
                Assert.assertEquals(
                        "the keyed resume must have priced cheaper, or the case covers nothing",
                        1,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                if (removal != null) {
                    Assert.assertEquals(
                            "a stored row whose base row is gone rules the keyed resume out",
                            0,
                            job.openSegmentKeyedResumeCountForTest()
                    );
                    Assert.assertEquals(0, job.openSegmentArithmeticRowPositionCountForTest());
                    Assert.assertEquals(0, job.sparsePublicationCountForTest());
                    Assert.assertNull(
                            "a declined resume must not build the isolated runtime a keyed one replays in",
                            instanceOf("lv").getRepairRuntime()
                    );
                } else {
                    Assert.assertEquals(1, job.openSegmentKeyedResumeCountForTest());
                    Assert.assertEquals(1, job.openSegmentSparseResumeCountForTest());
                    Assert.assertNotNull(instanceOf("lv").getRepairRuntime());
                }
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertFalse(
                        "the resume must leave its keyed replay unarmed, whichever way it went",
                        job.isKeyedReplayArmedForTest()
                );
                assertLadderCountsRowsAtOrBelowEachBoundary("after the repair");
            }
            assertRestartRestoresFromTimeline(row(4, 10, 10, 0, "acct-1"));
        });
    }

    /**
     * Restarts the view and holds its recovery to the timeline the last repair published:
     * restored rather than rebuilt or blocked, and still equal to a from-base recompute
     * once {@code inOrderRow}, which is what makes the recompiled view rehydrate, lands.
     */
    private void assertRestartRestoresFromTimeline(String inOrderRow) throws Exception {
        final long rowsBeforeRestart = durableRows();
        restartCycle();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commit(inOrderRow, job);
            assertRestoredFromTimeline("lv");
            final LiveViewInstance instance = instanceOf("lv");
            Assert.assertFalse(instance.isInvalid());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(rowsBeforeRestart + 1, durableRows());
            assertViewMatchesRecompute();
        }
    }

    /**
     * The drop takes the acct-2 row at 13:00 on 2026-01-02, alone in its hour, and a late row
     * of {@code lateAccount} repopulates that instant beside a correction of acct-2 at 05:00.
     * The stale acct-2 row waits at 13:00 for a pair that the late row must not give it. The
     * late row is the last one the replay emits, and no stored row of either corrected key
     * sits above it on that day, so only the final drain is left to close the wait unpaired.
     */
    private void assertStaleRowWaitingAtTheLastReplayedInstantRepairsByReplacement(
            String seedRows,
            String lateAccount,
            boolean isParked
    ) throws Exception {
        assertCorrectionOverARemovedRowRepairsByReplacement(
                seedRows,
                "ALTER TABLE tx DROP PARTITION LIST '2026-01-02T13'",
                "2026-01-02T13:00:00.000000Z",
                "acct-2",
                row(2, 5, 0, 0, "acct-2", 100.0) + ", " + row(2, 13, 0, 0, lateAccount, 100.0),
                true,
                isParked
        );
    }

    private void assertStoredRowsOfTwoCorrectedKeysAtOneInstantPair(boolean isParked) throws Exception {
        assertCorrectionPublishesSparsely(
                seedWithALoneRowAtTen() + ", " + row(2, 10, 0, 0, "acct-2", 16.0),
                row(2, 5, 0, 0, "acct-1", 100.0) + ", " + row(2, 6, 0, 0, "acct-2", 100.0),
                2,
                isParked
        );
    }

    /**
     * Holds the base and the view to the divergent symbol orders
     * {@link #seedWithALoneRowAtTenInAnotherSymbolOrder()} sets up: the base's integer for
     * acct-9 is the view's integer for acct-2. A case over that seed covers the pairing's
     * translation between the two maps only while this holds.
     */
    private void assertTheBaseAndTheViewNumberSymbolsInDifferentOrders() throws Exception {
        assertMemoryLeak(() -> Assert.assertEquals(
                "the base's integer for acct-9 must be the view's for acct-2, or the case covers no translation",
                symbolKeyOf("tx", "acct-9"),
                symbolKeyOf("lv", "acct-2")
        ));
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute("lv");
    }

    private void assertViewMatchesRecompute(String viewName) throws Exception {
        assertViewMatchesRecompute(viewName, "tx");
    }

    private void assertViewMatchesRecompute(String viewName, String baseName) throws Exception {
        assertViewRowsMatchRecompute(viewName, baseName);
        assertNoRefreshFaults(viewName);
    }

    /**
     * The rows half of {@link #assertViewMatchesRecompute(String, String)}, for a view a case
     * faulted on purpose and so cannot hold to a fault count of zero.
     */
    private void assertViewRowsMatchRecompute(String viewName, String baseName) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from " + baseName + ")";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1, 3",
                "(" + viewName + ") order by 2, 1, 3",
                LOG,
                true
        );
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        commit("tx", values, job);
    }

    private void commit(String tableName, String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into " + tableName + " values " + values);
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
        createBase("tx", seedRows, dedupClause);
    }

    private void createBase(String tableName, String seedRows, String dedupClause) throws Exception {
        execute("create table " + tableName + " (created_at timestamp, account_id symbol nocache index capacity 8, "
                + "amount double) timestamp(created_at) partition by hour wal" + dedupClause);
        execute("insert into " + tableName + " values " + seedRows);
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
     * lv over tx, and a second base tx2 seeded with the same rows for another view to stand
     * on. Both bases seed 2026-01-02 and 2026-01-03, so the day
     * {@link #faultAKeyedResumeOfLv(LiveViewRefreshJob)} drives in is the open one.
     */
    private void createLvBesideASecondBase() throws Exception {
        final String seed = hoursOfFourAccounts(2, 0, 10) + ", " + hoursOfFourAccounts(3, 0, 10);
        createBase(seed);
        createBase("tx2", seed, "");
        createViewOverBase("lv", "tx", "100ms");
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
        createViewOverBase(viewName, "tx", flushEvery);
    }

    private void createViewOverBase(String viewName, String baseName, String flushEvery) throws Exception {
        execute("create live view " + viewName + " flush every " + flushEvery + " start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                + "from " + baseName + " window w as (partition by account_id order by created_at anchor daily '00:00')");
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

    /**
     * Drives the open day 2026-01-04 into both bases in order, one commit per hour so lv's
     * cadence seals a root inside it, then commits an acct-1 correction at 02:35 to tx alone.
     * lv resumes from the root at 01:40 by key: it arms the worker's keyed replay and binds a
     * sparse publication, and then faults as its replay starts, the way a base scan I/O error
     * would. Returns with lv waiting out its refresh-retry backoff, so the next drain that
     * leaves the clock alone runs any other view's repair first.
     *
     * @return whether the worker's keyed replay was still armed right after the fault
     */
    private boolean faultAKeyedResumeOfLv(LiveViewRefreshJob job) throws Exception {
        driveRefreshToQuiescence(job);
        for (int hour = 0; hour < 10; hour++) {
            final String rows = hoursOfFourAccounts(4, hour, hour + 1);
            execute("insert into tx values " + rows);
            execute("insert into tx2 values " + rows);
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        final AtomicBoolean hasReplayStarted = new AtomicBoolean();
        job.setSimulateResumeReplayStartForTest(() -> {
            hasReplayStarted.set(true);
            throw CairoException.critical(0).put("simulated base scan fault");
        });
        execute("insert into tx values " + row(4, 2, 35, 0, "acct-1"));
        drainWalQueue();
        advanceClockToNextRefreshPass();
        drainJob(job);
        Assert.assertTrue("lv's resume must have reached its replay, or the case covers nothing", hasReplayStarted.get());
        Assert.assertEquals("lv's resume must have followed acct-1 alone", 1, job.openSegmentKeyedResumeCountForTest());
        Assert.assertEquals(1, instanceOf("lv").getRefreshFaultCount());
        return job.isKeyedReplayArmedForTest();
    }

    /**
     * One row of each of acct-1 to acct-4 in every hour from {@code fromHour} up to, but not
     * including, {@code toHour} on 2026-01-{@code day}: acct-k at minute k * 10.
     */
    private String hoursOfFourAccounts(int day, int fromHour, int toHour) {
        final StringBuilder rows = new StringBuilder();
        for (int hour = fromHour; hour < toHour; hour++) {
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(day, hour, account * 10, 0, "acct-" + account));
            }
        }
        return rows.toString();
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

    /**
     * One base row as an INSERT tuple. A null {@code account} leaves the row without one,
     * which is the NULL symbol key.
     */
    private String row(int day, int hour, int minute, int second, @Nullable String account, double amount) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":" + String.format("%02d", second)
                + ".000000Z', " + (account != null ? "'" + account + "'" : "null") + ", " + amount + ")";
    }

    /**
     * The view's rows carrying one {@code (created_at, account_id)} pair, read through the view
     * so an un-flushed lead row counts as one the view holds. A null {@code account} counts
     * the rows without one.
     */
    private long rowsAt(String timestamp, @Nullable String account) throws Exception {
        return rowsAt("lv", timestamp, account);
    }

    private long rowsAt(String viewName, String timestamp, @Nullable String account) throws Exception {
        return count("select count() from " + viewName + " where account_id"
                + (account != null ? " = '" + account + "'" : " is null")
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

    /**
     * The shape of {@link #seedWithALoneRowAtTen()} moved onto 2026-01-05, which is the open
     * day, over two rows on a closed 2026-01-02. The seed is one commit, so the view's only
     * root sits at the top of the day, and a correction inside the day finds no checkpoint
     * below it and replays cold from the day's origin. The acct-1 row at 10:00 sits alone in
     * its hourly partition, and so does the acct-1 row at 01:00.
     */
    private String seedAnOpenDayWithALoneRowAtTen() {
        final StringBuilder rows = new StringBuilder();
        rows.append(row(2, 1, 0, 0, "acct-1"))
                .append(", ").append(row(2, 1, 10, 0, "acct-2"))
                .append(", ").append(row(5, 1, 0, 0, "acct-1", 1.0))
                .append(", ").append(row(5, 2, 0, 0, "acct-2", 8.0))
                .append(", ").append(row(5, 3, 0, 0, "acct-3", 64.0));
        for (int minute = 1; minute < 60; minute++) {
            rows.append(", ").append(row(5, 3, minute, 0, "acct-3"));
        }
        rows.append(", ").append(row(5, 10, 0, 0, "acct-1", 2.0))
                .append(", ").append(row(5, 12, 0, 0, "acct-1", 4.0))
                .append(", ").append(row(5, 13, 0, 0, "acct-2", 32.0));
        return rows.toString();
    }

    /**
     * acct-1 at 01:00, 10:00 and 12:00 on 2026-01-02, beside acct-2 and an hour of acct-3
     * rows dense enough that a read by key prices cheaper than reading the day whole, and
     * a few rows on each of the next two days. The acct-1 row at 10:00 sits alone in its
     * hourly partition, so removing that partition takes one acct-1 row and nothing else.
     */
    private String seedWithALoneRowAtTen() {
        final StringBuilder rows = new StringBuilder();
        rows.append(row(2, 1, 0, 0, "acct-1", 1.0))
                .append(", ").append(row(2, 2, 0, 0, "acct-2", 8.0))
                .append(", ").append(row(2, 3, 0, 0, "acct-3", 64.0));
        for (int minute = 1; minute < 60; minute++) {
            rows.append(", ").append(row(2, 3, minute, 0, "acct-3"));
        }
        rows.append(", ").append(row(2, 10, 0, 0, "acct-1", 2.0))
                .append(", ").append(row(2, 12, 0, 0, "acct-1", 4.0))
                .append(", ").append(row(2, 13, 0, 0, "acct-2", 32.0))
                .append(", ").append(row(3, 1, 0, 0, "acct-1"))
                .append(", ").append(row(3, 1, 10, 0, "acct-2"))
                .append(", ").append(row(3, 1, 20, 0, "acct-3"))
                .append(", ").append(row(4, 1, 0, 0, "acct-1"))
                .append(", ").append(row(4, 1, 10, 0, "acct-2"));
        return rows.toString();
    }

    /**
     * {@link #seedWithALoneRowAtTen()} plus two rows without an account on 2026-01-02, at
     * 11:00 and at 14:00. Each sits alone in its hourly partition, so removing the 11:00
     * one takes one NULL-key row and nothing else.
     */
    private String seedWithALoneRowAtTenAndNullKeyRows() {
        return seedWithALoneRowAtTen()
                + ", " + row(2, 11, 0, 0, null, 5.0)
                + ", " + row(2, 14, 0, 0, null, 6.0);
    }

    /**
     * {@link #seedWithALoneRowAtTen()} behind two rows on 2026-01-04, acct-1 at 02:00 and then
     * acct-9 at 03:00, which puts the base's symbol integers and the view's in different
     * orders. The base numbers each value as its first row arrives: acct-1, acct-9, acct-2,
     * acct-3. The view numbers each value as it emits its first row, in timestamp order:
     * acct-1, acct-2, acct-3, acct-9. So the base's integer for acct-9 is the view's integer
     * for acct-2, and the base's integer for acct-2 is not the view's.
     */
    private String seedWithALoneRowAtTenInAnotherSymbolOrder() {
        return row(4, 2, 0, 0, "acct-1")
                + ", " + row(4, 3, 0, 0, "acct-9")
                + ", " + seedWithALoneRowAtTen();
    }

    /**
     * The integer the named table's own symbol map gives {@code account}. The account is
     * column 1 in both the base and the view.
     */
    private int symbolKeyOf(String tableName, String account) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getSymbolMapReader(1).keyOf(account);
        }
    }

    /**
     * The named view table's own transaction number - the count of {@code _txn} commits it has
     * taken. One per applied live-view WAL transaction when the apply takes the full-commit
     * path, fewer when the WAL lag absorbs several transactions before one commit publishes
     * them all.
     */
    private long tableTransactions(String viewName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(viewName))) {
            return reader.getTxn();
        }
    }
}
