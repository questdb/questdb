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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code TableWriter.wouldBreachCompactionThresholds} forecasts the shape the plan
 * {@code O3PartitionJob.processCompositePartition} is about to execute would leave behind, and
 * {@code O3PartitionJob.shouldAssembleFreshPartitionVersion} folds the whole partition into a fresh
 * directory when that forecast breaches {@code PartitionCompactionPolicy.exceedsThresholds}. The forecast
 * therefore has to account for every {@code O3CompositeMergeStrategy.ActionType} the plan can carry.
 * <p>
 * Two of the five tests here pin the two answers apart on ONE fixture shape - a day left with 200 live rows
 * and 100 dead rows in a single tail-owning piece - that differs only in where this commit's ten rows land:
 * <ul>
 *     <li>above the piece's tsHi, so the plan is APPEND: the piece is extended in place, leaving 210 live
 *     rows in one piece and adding no dead rows at all. 100 dead against 210 live is below the 0.48 waste
 *     ratio that test pins, so the commit must write the ten rows and leave the directory where it is.</li>
 *     <li>inside the piece's range, so the plan is MERGE: the piece is re-written at the shared file tail
 *     and its previous copy becomes dead, leaving 300 dead against 210 live. That IS past the waste ratio,
 *     so the commit must fold the partition into a fresh directory instead of publishing waste the
 *     background compaction would have to reclaim right after.</li>
 * </ul>
 * Three further tests pin the halves of the forecast that pair cannot see:
 * <ul>
 *     <li>{@link #testAppendPastATightenedWasteRatioStillFolds} tightens the waste ratio on the very same
 *     APPEND plan, so the shape the fold rule is asked about really is wasteful and the commit must still
 *     fold - a control that stays green only while the APPEND arm reports the plan's real numbers, rather
 *     than suppressing the fold or over-crediting the append;</li>
 *     <li>{@link #testAppendOntoATwoPieceDayFoldsOnThePieceRule} turns the waste rule off entirely and puts
 *     the piece-count rule in play at two pieces - the only shape in which the piece an APPEND leaves
 *     behind changes the answer;</li>
 *     <li>{@link #testAppendOntoAPrefixDominantDayLeavesTheBreachForMoveTail} keeps the MOVE-TAIL escape a
 *     prefix-dominant day is entitled to, which is the one direction counting an APPEND's rows can take
 *     away.</li>
 * </ul>
 */
public class CompositeAppendCompactionForecastTest extends AbstractCairoTest {

    /**
     * The whole partition gets copied when the forecast reports no live rows for an APPEND: the ten
     * incoming rows are charged as 210 physically written rows and the partition moves to a fresh
     * directory (its {@code nameTxn} changes).
     * <p>
     * The waste ratio is pinned at 0.48, which is the one window in which the ten appended rows decide the
     * answer on their own: 100 dead rows are NOT past 0.48 * 210, the live count the plan really leaves,
     * but they ARE past 0.48 * 200, the count the piece held before this commit. So an arm that adds the
     * appended-to piece's rows and forgets {@code action.getO3RowCount()} folds this partition, and this
     * test is what says so - at 1.0 it would stay green for that arm as well.
     */
    @Test
    public void testAppendAboveTailPieceKeepsThePartitionDirectory() throws Exception {
        assertMemoryLeak(() -> {
            configureTightWasteThresholds();
            createDayWithOneTailOwningPiece();
            // Pinned only now, after the buildup: a ratio this tight during the buildup would have folded
            // the dead space away as it was created, leaving nothing for the forecast to weigh.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "0.48");

            final long nameTxnBefore = nameTxnOfDay();
            final long writtenBefore = physicallyWrittenRows();

            // Ten rows ABOVE the piece's tsHi (00:01:39), so the planner extends the tail-owning piece.
            execute("INSERT INTO x SELECT x + 200, timestamp_sequence('2024-01-01T00:01:40', 1_000_000L) FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertEquals(
                    "the append assembled a fresh partition directory instead of extending the tail",
                    nameTxnBefore,
                    nameTxnOfDay()
            );
            Assert.assertEquals(
                    "the append copied the partition's existing rows instead of writing only its own",
                    10,
                    physicallyWrittenRows() - writtenBefore
            );
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\n210\t22155\n");
        });
    }

    /**
     * The direction the other tests cannot see: counting an APPEND's rows in
     * {@code TableWriter.wouldMoveTailSucceed} enlarges the denominator that decides whether the commit
     * hands a breach to MOVE-TAIL instead of folding the partition itself, so a shape whose prefix no
     * longer clears {@code cairo.partition.compaction.prefix.min.percent} against the bigger number loses
     * an escape it used to get. This fixture is the shape that must keep it: a 400-row cold prefix over an
     * 80-row tail piece, so the prefix clears the percentage (400 * 100 >= 490 * 50) even with the
     * appended rows counted.
     * <p>
     * The commit therefore writes only its ten rows, and the compaction that follows moves the 90-row tail
     * out instead of copying all 490 live rows - the cheap path this fixture exists to keep open. Green
     * before and after the fix: it is the control for a fix that declines the escape too eagerly.
     */
    @Test
    public void testAppendOntoAPrefixDominantDayLeavesTheBreachForMoveTail() throws Exception {
        assertMemoryLeak(() -> {
            configureTightWasteThresholds();
            letMoveTailRun();
            letPreSplitCut();
            createDayWithABigPrefixAndASmallTailPiece();
            // The fixture, not the fix: without exactly this shape the assertions below are vacuous.
            assertPrefixDominantFixture();
            // Both only now, after the buildup: the piece divisor is the same knob the pre-split needed,
            // and a ratio this tight during the buildup would have folded the dead space away as it was
            // created. The cap goes to the shipped 20, so the piece rule cannot decide these two pieces
            // and the waste rule is the only one left.
            pinPieceCap(20);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "0.05");

            final long writtenBefore = physicallyWrittenRows();

            // Ten rows ABOVE the last piece's tsHi (00:07:19), so the planner extends the tail-owning piece.
            execute("INSERT INTO x SELECT x + 480, timestamp_sequence('2024-01-01T00:07:20', 1_000_000L) FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertEquals(
                    "the commit folded the whole partition instead of leaving its breach to MOVE-TAIL, which"
                            + " only had to move the tail piece out",
                    100,
                    physicallyWrittenRows() - writtenBefore
            );
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\n490\t120295\n");
        });
    }

    /**
     * The piece half of the forecast. The waste rule is off (a dead-space floor no hundred-row fixture can
     * reach), the piece cap is pinned at one, and the day holds two pieces: a 40-row piece 0 that keeps its
     * place at row offset 0, and a tail-owning piece the incoming ten rows extend. The plan therefore
     * leaves TWO pieces, one over the cap, and the commit must fold.
     * <p>
     * This is the under-forecast direction, so the test is RED before the fix for the opposite reason to
     * {@link #testAppendAboveTailPieceKeepsThePartitionDirectory}: the unaccounted APPEND piece makes the
     * forecast report one piece, exactly at the cap, and the fold the piece rule wants never happens.
     * <p>
     * The piece cap arms the POST-commit compaction pass as well, and that pass rewrites the very shape
     * the commit declined to fold - so the partition ends up folded and dead-space-free either way, and
     * the thing that tells the two apart is who wrote it. A commit that folds writes each of the 450 live
     * rows once. A commit that declines writes its ten rows in place and {@code TableWriter.runCompaction}
     * then rewrites all 450 right after it, for 460.
     * <p>
     * The test also fails for either half of the repair on its own. Without {@code pieceCount++} the
     * forecast still reports one piece and nothing breaches. With {@code pieceCount++} but with
     * {@code TableWriter.wouldMoveTailSucceed} still dropping APPEND from its live-row denominator, that
     * method weighs piece 0's 40 rows against 40 instead of against 450 and promises a MOVE-TAIL, so
     * {@code O3PartitionJob.shouldAssembleFreshPartitionVersion} takes the escape and again does not fold.
     */
    @Test
    public void testAppendOntoATwoPieceDayFoldsOnThePieceRule() throws Exception {
        assertMemoryLeak(() -> {
            configurePieceRuleOnlyThresholds();
            letPreSplitCut();
            createDayWithTwoPiecesTheLastOwningTheTail();
            // The fixture, not the fix: without exactly this shape the assertions below are vacuous.
            assertTwoPieceFixture();
            // Pinned only now, after the buildup: the same knob drives the pre-split that cut the day in
            // two, so pinning it earlier leaves a single-piece fixture instead.
            pinPieceCap(1);

            final long writtenBefore = physicallyWrittenRows();

            // Ten rows ABOVE the last piece's tsHi (00:03:59), so the planner extends the tail-owning piece.
            execute("INSERT INTO x SELECT x + 440, timestamp_sequence('2024-01-01T00:04:00', 1_000_000L) FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertEquals(
                    "the commit left two pieces over a cap of one instead of folding them, so the partition"
                            + " was written twice: ten rows in place and all 450 again by the compaction that"
                            + " followed",
                    450,
                    physicallyWrittenRows() - writtenBefore
            );
            Assert.assertEquals("the fold left dead space behind", 0, deadRowsOfDay());
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\n450\t101475\n");
        });
    }

    /**
     * The preservation control for the arm this change touches: the SAME fixture and the SAME APPEND plan
     * as {@link #testAppendAboveTailPieceKeepsThePartitionDirectory}, with only the waste ratio tightened
     * to 0.4 after the buildup. The plan's real post-commit shape is 210 live rows against 100 dead ones,
     * and 100 > 0.4 * 210, so this append genuinely is past the waste rule and must still fold into a fresh
     * directory.
     * <p>
     * Green both before and after the fix, which is what makes it a control: it stays green only for a fix
     * that reports the APPEND plan's real numbers. It fails for a fix that declines the fold whenever the
     * plan carries an APPEND action, and for one that also cancels the appended-to piece's dead rows.
     */
    @Test
    public void testAppendPastATightenedWasteRatioStillFolds() throws Exception {
        assertMemoryLeak(() -> {
            configureTightWasteThresholds();
            createDayWithOneTailOwningPiece();
            // Tightened only now, after the buildup: with 0.4 in effect throughout, the commit that builds
            // the dead space would have folded it away as it created it and there would be none left.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "0.4");

            final long nameTxnBefore = nameTxnOfDay();
            final long writtenBefore = physicallyWrittenRows();

            // Ten rows ABOVE the piece's tsHi (00:01:39), so the planner extends the tail-owning piece.
            execute("INSERT INTO x SELECT x + 200, timestamp_sequence('2024-01-01T00:01:40', 1_000_000L) FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertNotEquals(
                    "the append's own shape breaches the waste ratio, so the commit had to fold the partition",
                    nameTxnBefore,
                    nameTxnOfDay()
            );
            Assert.assertEquals(
                    "the fold did not copy the partition's live rows into the fresh directory",
                    210,
                    physicallyWrittenRows() - writtenBefore
            );
            Assert.assertEquals("the fold left dead space behind", 0, deadRowsOfDay());
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\n210\t22155\n");
        });
    }

    /**
     * The preservation control for {@link #testAppendAboveTailPieceKeepsThePartitionDirectory}: the same
     * fixture, the same ten rows, but landing INSIDE the piece. That plan really does leave 300 dead rows
     * against 210 live ones, so the commit must still fold the partition into a fresh directory.
     */
    @Test
    public void testMergeOverTailPieceStillFoldsIntoAFreshDirectory() throws Exception {
        assertMemoryLeak(() -> {
            configureTightWasteThresholds();
            createDayWithOneTailOwningPiece();

            final long nameTxnBefore = nameTxnOfDay();
            final long writtenBefore = physicallyWrittenRows();

            // Ten rows INSIDE the piece's range, so the planner re-writes it and abandons its old copy.
            execute("INSERT INTO x SELECT x + 200, timestamp_sequence('2024-01-01T00:00:30', 1_000_000L) FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertNotEquals(
                    "the waste-breaching merge published dead space instead of folding the partition",
                    nameTxnBefore,
                    nameTxnOfDay()
            );
            Assert.assertEquals(
                    "the fold did not copy the partition's live rows into the fresh directory",
                    210,
                    physicallyWrittenRows() - writtenBefore
            );
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\n210\t22155\n");
        });
    }

    /**
     * The fixture {@link #testAppendOntoATwoPieceDayFoldsOnThePieceRule} needs: 2024-01-01 holds exactly
     * two pieces, piece 0 still at row offset 0 (the shape {@code TableWriter.wouldMoveTailSucceed} asks
     * for) and piece 1 owning the shared files' tail (the shape {@code O3CompositeMergeStrategy} emits
     * APPEND for).
     */
    private static void assertPrefixDominantFixture() throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final int partitionIndex = dayPartitionIndex(reader);
            final PartitionGeometry geometry = reader.getGeometry();
            Assert.assertEquals("the pre-split did not cut the day in two", 2, geometry.getPieceCount(partitionIndex));
            Assert.assertEquals("piece 0 moved off the start of the files", 0, geometry.getPieceRowOffset(partitionIndex, 0));
            Assert.assertEquals("piece 0 is not the untouched prefix", 400, geometry.getPieceRowCount(partitionIndex, 0));
            Assert.assertEquals("the tail piece is not the small one", 80, geometry.getPieceRowCount(partitionIndex, 1));
            Assert.assertEquals(
                    "piece 1 does not own the tail, so this commit cannot be an APPEND",
                    geometry.getE(partitionIndex),
                    geometry.getPieceRowOffset(partitionIndex, 1) + geometry.getPieceRowCount(partitionIndex, 1)
            );
        }
    }

    private static void assertTwoPieceFixture() throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final int partitionIndex = dayPartitionIndex(reader);
            final PartitionGeometry geometry = reader.getGeometry();
            Assert.assertEquals("the pre-split did not cut the day in two", 2, geometry.getPieceCount(partitionIndex));
            Assert.assertEquals("piece 0 moved off the start of the files", 0, geometry.getPieceRowOffset(partitionIndex, 0));
            Assert.assertEquals("piece 0 is not the untouched prefix", 40, geometry.getPieceRowCount(partitionIndex, 0));
            Assert.assertEquals(
                    "piece 1 does not own the tail, so this commit cannot be an APPEND",
                    geometry.getE(partitionIndex),
                    geometry.getPieceRowOffset(partitionIndex, 1) + geometry.getPieceRowCount(partitionIndex, 1)
            );
        }
    }

    /**
     * Thresholds that leave the PIECE-COUNT rule as the only one that can fire: a dead-space floor of one
     * gibibyte is beyond anything a few hundred rows reach, so the waste ratio cannot decide these commits,
     * and background compaction's own age and table-pressure rules stay off. The piece rule itself is
     * shared with the post-commit pass and cannot be switched off for one and not the other, which is why
     * the test that uses it counts rows written rather than looking at the directory name.
     */
    private static void configurePieceRuleOnlyThresholds() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, 1_073_741_824L);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");
    }

    /**
     * Thresholds small enough for a hundred-row fixture to reach: the shipped 50 MiB dead-space floor is
     * what keeps a production table this small below the waste rule, and the ratio is already the default.
     * Background compaction's own table-pressure and age rules stay off, so the only thing that can move
     * this partition is the commit itself.
     */
    private static void configureTightWasteThresholds() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");
    }

    /**
     * Leaves 2024-01-01 holding two pieces the other way round from
     * {@link #createDayWithTwoPiecesTheLastOwningTheTail}: 440 rows one second apart, then 40 backdated
     * rows over the top 40 of them. The pre-split cuts the untouched 400-row prefix off into a piece of
     * its own, and merge-append re-writes the last 40 together with the incoming 40 at the shared files'
     * tail, leaving an 80-row tail piece over 40 dead rows.
     */
    private static void createDayWithABigPrefixAndASmallTailPiece() throws Exception {
        execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x SELECT x, timestamp_sequence('2024-01-01T00:00:00', 1_000_000L) FROM long_sequence(440)");
        // A later day, so 2024-01-01 is never the active partition and every further write to it is O3.
        execute("INSERT INTO x VALUES (0, '2024-01-03')");
        drainWalQueue();
        execute("INSERT INTO x SELECT x + 440, timestamp_sequence('2024-01-01T00:06:40', 1_000_000L) FROM long_sequence(40)");
        drainWalQueue();
    }

    /**
     * Leaves 2024-01-01 holding 200 live rows and 100 dead ones in ONE piece that owns the shared files'
     * tail: a hundred rows one second apart, then a second hundred over the same stride, which merge-append
     * re-writes at the tail and abandons the first copy of.
     */
    private static void createDayWithOneTailOwningPiece() throws Exception {
        execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x SELECT x, timestamp_sequence('2024-01-01T00:00:00', 1_000_000L) FROM long_sequence(100)");
        // A later day, so 2024-01-01 is never the active partition and every further write to it is O3.
        execute("INSERT INTO x VALUES (0, '2024-01-03')");
        drainWalQueue();
        execute("INSERT INTO x SELECT x + 100, timestamp_sequence('2024-01-01T00:00:00', 1_000_000L) FROM long_sequence(100)");
        drainWalQueue();
    }

    /**
     * Leaves 2024-01-01 holding two pieces: 240 rows one second apart, then 200 backdated rows over the top
     * 200 of them. The pre-split cuts the untouched 40-row prefix off into a piece of its own, and
     * merge-append re-writes the rest at the shared files' tail, which it owns from then on.
     */
    private static void createDayWithTwoPiecesTheLastOwningTheTail() throws Exception {
        execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x SELECT x, timestamp_sequence('2024-01-01T00:00:00', 1_000_000L) FROM long_sequence(240)");
        // A later day, so 2024-01-01 is never the active partition and every further write to it is O3.
        execute("INSERT INTO x VALUES (0, '2024-01-03')");
        drainWalQueue();
        execute("INSERT INTO x SELECT x + 240, timestamp_sequence('2024-01-01T00:00:40', 1_000_000L) FROM long_sequence(200)");
        drainWalQueue();
    }

    private static int dayPartitionIndex(TableReader reader) {
        final long dayTs = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
        final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
        Assert.assertTrue("2024-01-01 has no partition", partitionIndex > -1);
        return partitionIndex;
    }

    /**
     * Rows 2024-01-01's files still hold that no piece points at - the waste a fold reclaims.
     */
    private static long deadRowsOfDay() throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final int partitionIndex = dayPartitionIndex(reader);
            return reader.getGeometry().getE(partitionIndex) - reader.getTxFile().getPartitionSize(partitionIndex);
        }
    }

    /**
     * The settled-piece window and the MOVE-TAIL gain floor, off. These fixtures are a handful of commits
     * over a clock that barely advances, so the shipped values call every piece hot and decline the
     * MOVE-TAIL the prefix-dominant test is about. {@code O3PartitionCompactionTest.enableCompaction}
     * turns the same three knobs off for the same reason.
     */
    private static void letMoveTailRun() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_TIME, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MOVE_TAIL_MIN_GAIN, 1);
    }

    /**
     * A 32-row piece floor, so the pre-split cuts this small fixture into several pieces instead of
     * re-writing it whole.
     */
    private static void letPreSplitCut() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
    }

    private static long nameTxnOfDay() throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            return reader.getTxFile().getPartitionNameTxn(dayPartitionIndex(reader));
        }
    }

    private static long physicallyWrittenRows() {
        return node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
    }

    /**
     * Puts the piece-count rule in play at exactly {@code pieces}: the divisor goes above any fixture's row
     * count, so the scaled cap is zero and the flat floor is what fires.
     */
    private static void pinPieceCap(int pieces) {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, pieces);
    }
}
