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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Regression lock for the "can two composite pieces share a boundary timestamp" question raised while
 * auditing {@code TableWriter.moveTailToFreshPartition}. They can, and the answer is load-bearing:
 * {@code O3CompositeMergeStrategy}'s piece range check is inclusive on both ends
 * ({@code tsLo <= ts <= tsHi}), so a value on a shared boundary belongs to two pieces at once, and
 * {@code O3CompositeMergeStrategy.findPieceContaining} - which the replace-range edge cuts in
 * {@code O3PartitionJob.processCompositePartition} used to resolve a REPLACE RANGE commit's own bounds to
 * a piece - always resolves such a tie to the FIRST (earlier) piece in ordinal order, never the second.
 * <p>
 * An earlier version of this javadoc claimed genuine touching was not constructible and declined to
 * attempt the three-distinct-value shape. That claim is wrong.
 * {@link #testReplaceRangeEndingOnATouchedBoundaryEmptiesBothPieces} builds exactly that shape - a piece
 * spanning {@code [.., V]} immediately followed by one spanning {@code [V, V + 3s]} - and HEAD's own code
 * says the same: {@code O3PartitionJob.hasTouchingPieces} exists to detect
 * {@code pieceTsLo == previousPieceTsHi}; {@code TableWriter.coldPrefixPieceCount} ends the cold prefix on
 * {@code getPieceTimestampHi(p) >= getPieceTimestampLo(p + 1)}; {@code PartitionGeometry.addPiece} asserts
 * only STRICTLY ascending {@code tsLo}, which permits {@code prevTsHi == tsLo}; and four tests in
 * {@code O3CompositePartitionTest} assert their fixture HAS touching pieces before proceeding.
 * <p>
 * The miss the old javadoc feared was real. The replace-range edge cuts looked the high edge up by
 * {@code replaceRangeTsHi} while cutting at {@code replaceRangeTsHi + 1}, so when a piece started exactly
 * at the range's upper bound the lookup returned its predecessor instead, that piece was never cut, and
 * its rows inside the declared range survived a commit that declared them deleted - no error, no
 * assertion, no suspended table.
 * <p>
 * The tests here lock both halves of the contract: composite pieces built from wildly out-of-order commits
 * over data with almost no distinct timestamps get a REPLACE RANGE applied exactly, and a range whose edge
 * lands on a genuinely shared boundary empties every piece's rows inside it while leaving the rows outside
 * it - and the pieces holding them - in place.
 */
public class O3PartitionReplaceRangeTouchingPiecesTest extends AbstractCairoTest {

    private static final String DAY = "2020-02-03";
    /**
     * Distance from the shared boundary {@code V} to the tie commit's upper row, i.e. the touching piece
     * spans {@code [V, V + TIE_GAP_MICROS]}.
     */
    private static final long TIE_GAP_MICROS = 3_000_000L;

    @Test
    public void testReplaceOneOfTwoTimestampsAfterOutOfOrderComposite() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "16");

            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('" + DAY + "T00:00:00.000000Z', 0)");
            // A later day, so DAY is never the active partition and every further write to it goes
            // through the O3 path.
            execute("INSERT INTO x VALUES ('2020-02-06T00:00:00.000000Z', 999)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            final long t1 = MicrosTimestampDriver.floor(DAY + "T05:00:00.000000Z");
            final long t2 = MicrosTimestampDriver.floor(DAY + "T15:00:00.000000Z");

            // Six separate O3 commits, scrambled: t2 lands before t1 even though t1 < t2, and the two
            // alternate repeatedly - each commit at t1 or t2 competes with, and typically relocates, an
            // already-composite piece at that exact timestamp. No dedup: duplicate-timestamp rows pile up
            // at each of the two points, which is exactly the shape most likely to expose a tie the piece
            // planner gets wrong.
            long v = 1;
            appendAt(xt, t2, v++);
            appendAt(xt, t1, v++);
            appendAt(xt, t2, v++);
            appendAt(xt, t1, v++);
            appendAt(xt, t2, v++);
            appendAt(xt, t1, v++);
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after out-of-order buildup: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            assertFixturePiecesDoNotTouch("x");

            final long t1CountBefore = countAt("x", t1);
            final long t2CountBefore = countAt("x", t2);
            Assert.assertTrue("fixture put no rows at t1: " + describePieces("x"), t1CountBefore > 0);
            Assert.assertTrue("fixture put no rows at t2: " + describePieces("x"), t2CountBefore > 0);

            // REPLACE RANGE covering exactly t1 (the range must be non-empty: [t1, t1+1) is the narrowest
            // half-open span that still covers the single microsecond t1), removing every row at it and
            // inserting none.
            try (WalWriter ww = engine.getWalWriter(xt)) {
                ww.commitWithParams(t1, t1 + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after replace: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            assertFixturePiecesDoNotTouch("x");

            Assert.assertEquals(
                    "rows at t1 survived the replace: " + describePieces("x"),
                    0,
                    countAt("x", t1)
            );
            Assert.assertEquals(
                    "rows at t2 were disturbed by a replace range that never covered t2: " + describePieces("x"),
                    t2CountBefore,
                    countAt("x", t2)
            );
            // The anchor row and the later day's row must survive an unrelated replace untouched too.
            Assert.assertEquals(1, countAt("x", MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z")));
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize()
                    .returns("c\n" + (2 + t2CountBefore) + "\n");
        });
    }

    /**
     * The far more common production shape than a pure delete: the REPLACE RANGE commit also appends new
     * rows at the replaced timestamp in the same commit. Same out-of-order composite buildup as
     * {@link #testReplaceOneOfTwoTimestampsAfterOutOfOrderComposite}, but t1's old rows are replaced with
     * two brand new ones instead of being removed outright.
     */
    @Test
    public void testReplaceOneOfTwoTimestampsWithNewRowsAfterOutOfOrderComposite() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "16");

            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('" + DAY + "T00:00:00.000000Z', 0)");
            // A later day, so DAY is never the active partition and every further write to it goes
            // through the O3 path.
            execute("INSERT INTO x VALUES ('2020-02-06T00:00:00.000000Z', 999)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            final long t1 = MicrosTimestampDriver.floor(DAY + "T05:00:00.000000Z");
            final long t2 = MicrosTimestampDriver.floor(DAY + "T15:00:00.000000Z");

            // Six separate O3 commits, scrambled: t2 lands before t1 even though t1 < t2, and the two
            // alternate repeatedly - see the buildup comment on the sibling test for why this shape.
            long v = 1;
            appendAt(xt, t2, v++);
            appendAt(xt, t1, v++);
            appendAt(xt, t2, v++);
            appendAt(xt, t1, v++);
            appendAt(xt, t2, v++);
            appendAt(xt, t1, v++);
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after out-of-order buildup: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            assertFixturePiecesDoNotTouch("x");

            final long t2CountBefore = countAt("x", t2);
            Assert.assertTrue("fixture put no rows at t1: " + describePieces("x"), countAt("x", t1) > 0);
            Assert.assertTrue("fixture put no rows at t2: " + describePieces("x"), t2CountBefore > 0);

            // REPLACE RANGE covering exactly t1, replacing every old row there with two brand new ones in
            // the same commit.
            try (WalWriter ww = engine.getWalWriter(xt)) {
                ww.commitWithParams(t1, t1 + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
                TableWriter.Row row = ww.newRow(t1);
                row.putLong(1, 9001);
                row.append();
                row = ww.newRow(t1);
                row.putLong(1, 9002);
                row.append();
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after replace: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            assertFixturePiecesDoNotTouch("x");

            Assert.assertEquals(
                    "t1 does not hold exactly the two replacement rows: " + describePieces("x"),
                    2,
                    countAt("x", t1)
            );
            assertQuery("SELECT v FROM x WHERE ts = " + t1 + " ORDER BY v")
                    .returns("v\n9001\n9002\n");
            Assert.assertEquals(
                    "rows at t2 were disturbed by a replace range that never covered t2: " + describePieces("x"),
                    t2CountBefore,
                    countAt("x", t2)
            );
            // The anchor row and the later day's row must survive an unrelated replace untouched too.
            Assert.assertEquals(1, countAt("x", MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z")));
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize()
                    .returns("c\n" + (4 + t2CountBefore) + "\n");
        });
    }

    /**
     * A REPLACE RANGE commit whose declared range ENDS on the shared boundary {@code V} of
     * {@link #buildTouchingPiecesFixture()} must empty {@code [V, V]} - the contract is "the declared
     * range is emptied, then the commit's own rows are inserted" - so BOTH touching pieces lose their
     * rows at {@code V}, while the row at {@code V + TIE_GAP_MICROS}, outside the range, stays.
     * <p>
     * This is the case the high edge missed: it looked its piece up by {@code V} - matching the head
     * piece, or the single-row fragment the low-edge cut had just split off it - while cutting at
     * {@code V + 1}, so the piece STARTING at {@code V} was never considered and kept its row.
     */
    @Test
    public void testReplaceRangeEndingOnATouchedBoundaryEmptiesBothPieces() throws Exception {
        assertMemoryLeak(() -> {
            final long v = buildTouchingPiecesFixture();
            final long vUpper = v + TIE_GAP_MICROS;
            final TableToken xt = engine.verifyTableName("x");

            final long extentBefore;
            try (TableReader reader = engine.getReader(xt)) {
                extentBefore = reader.getGeometry().getE(dayPartitionIndex(reader.getTxFile()));
            }

            // REPLACE RANGE [v, v + 1) - the narrowest range covering exactly v, ENDING on the shared bound.
            try (WalWriter ww = engine.getWalWriter(xt)) {
                ww.commitWithParams(v, v + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after replace: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            Assert.assertEquals(
                    "rows at the shared boundary survived the replace: " + describePieces("x"),
                    0,
                    countAt("x", v)
            );
            // The row just above the range is untouched, and so is everything below it.
            assertQuery("SELECT i FROM x WHERE ts = " + vUpper + "::TIMESTAMP").returns("i\n-2\n");
            // 1 row on 2020-02-06 + 5_760 base rows + 20 backfill rows + 2 tie-commit rows, less the two
            // rows the replace range covers.
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n5781\n");

            // The commit must CUT and DROP, not rewrite. A fix that assembled a fresh partition version
            // instead would satisfy every row assertion above while silently recopying the whole day, so
            // the day must still be composite and its physical extent must be byte-for-byte what it was.
            try (TableReader reader = engine.getReader(xt)) {
                final TxReader txReader = reader.getTxFile();
                final int partitionIndex = dayPartitionIndex(txReader);
                Assert.assertTrue(
                        "the day stopped being composite: " + describePieces("x"),
                        txReader.isPartitionComposite(partitionIndex)
                );
                Assert.assertEquals(
                        "the partition was rewritten rather than cut: " + describePieces("x"),
                        extentBefore,
                        reader.getGeometry().getE(partitionIndex)
                );
            }
        });
    }

    /**
     * The mat-view refresh shape on touching pieces: the REPLACE RANGE commit ending on the shared
     * boundary {@code V} also carries its own rows at {@code V}. Exactly those rows must remain there.
     * <p>
     * This is the one case where the fix changes more than which rows survive. The high-edge cut leaves a
     * SMALL upper half ({@code [V + TIE_GAP_MICROS, V + TIE_GAP_MICROS]}, one row), and
     * {@code O3CompositeMergeStrategy.computeActions} folds gap rows below a piece that small into a
     * MERGE of it instead of letting them found their own piece. So the commit's new rows at {@code V}
     * and that surviving row get written out together: one row of write amplification that the
     * unfixed code did not pay, in exchange for the deletion actually happening.
     */
    @Test
    public void testReplaceRangeEndingOnATouchedBoundaryWithNewRowsKeepsOnlyTheNewRows() throws Exception {
        assertMemoryLeak(() -> {
            final long v = buildTouchingPiecesFixture();
            final long vUpper = v + TIE_GAP_MICROS;
            final TableToken xt = engine.verifyTableName("x");

            // Rows first, then commitWithParams: that is what makes them part of the REPLACE RANGE commit
            // rather than a separate one after it.
            try (WalWriter ww = engine.getWalWriter(xt)) {
                TableWriter.Row row = ww.newRow(v);
                row.putInt(0, -101);
                row.append();
                row = ww.newRow(v);
                row.putInt(0, -102);
                row.append();
                ww.commitWithParams(v, v + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after replace: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            // Both old rows at v - the head piece's and the touching piece's - are gone, and only the
            // commit's own two rows are there.
            assertQuery("SELECT i FROM x WHERE ts = " + v + "::TIMESTAMP ORDER BY i")
                    .returns("i\n-102\n-101\n");
            // The touching piece's row above the range survives, wherever the merge put it.
            assertQuery("SELECT i FROM x WHERE ts = " + vUpper + "::TIMESTAMP").returns("i\n-2\n");
            // 5_783 fixture rows, less the two old rows at v, plus the two new ones.
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n5783\n");
        });
    }

    /**
     * The symmetric low edge: a REPLACE RANGE commit that STARTS on the shared boundary {@code V} and
     * reaches above the touching piece's own {@code tsHi}. The earlier piece must be cut at {@code V} so
     * its rows BELOW the range survive, while everything from {@code V} up to the range's end goes -
     * including the whole touching piece, which sits wholly inside the range.
     * <p>
     * The low edge was already correct (it looks pieces up by the same {@code replaceRangeTsLo} it cuts
     * at, and the later candidate at a tie starts exactly on the key, so it needs no cut), so this is a
     * preservation lock on the edge the fix rewrote as a loop, not a second reproducer.
     */
    @Test
    public void testReplaceRangeStartingOnATouchedBoundaryKeepsRowsBelowIt() throws Exception {
        assertMemoryLeak(() -> {
            final long v = buildTouchingPiecesFixture();
            final long vUpper = v + TIE_GAP_MICROS;
            final TableToken xt = engine.verifyTableName("x");

            final long belowCountBefore = scalar("SELECT count() c FROM x WHERE ts < " + v + "::TIMESTAMP");
            Assert.assertTrue(
                    "fixture left no rows below the shared boundary: " + describePieces("x"),
                    belowCountBefore > 0
            );

            // REPLACE RANGE [v, vUpper + 1): starts ON the shared bound and swallows the touching piece whole.
            try (WalWriter ww = engine.getWalWriter(xt)) {
                ww.commitWithParams(v, vUpper + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after replace: " + describePieces("x"),
                    engine.getTableSequencerAPI().isSuspended(xt)
            );
            Assert.assertEquals(
                    "rows at the shared boundary survived the replace: " + describePieces("x"),
                    0,
                    countAt("x", v)
            );
            Assert.assertEquals(
                    "the touching piece's row inside the range survived: " + describePieces("x"),
                    0,
                    countAt("x", vUpper)
            );
            Assert.assertEquals(
                    "the earlier piece lost rows below the range: " + describePieces("x"),
                    belowCountBefore,
                    scalar("SELECT count() c FROM x WHERE ts < " + v + "::TIMESTAMP")
            );
            // 5_783 fixture rows, less the two at v and the one at vUpper.
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n5780\n");
        });
    }

    private static void appendAt(TableToken tt, long ts, long v) {
        try (WalWriter ww = engine.getWalWriter(tt)) {
            TableWriter.Row row = ww.newRow(ts);
            row.putLong(1, v);
            row.append();
            ww.commit();
        }
    }

    /**
     * Locks the shape of the out-of-order buildup THIS fixture makes: its pieces, built at only two
     * distinct timestamps, must keep a real gap between every adjacent pair. Touching pieces are legal in
     * general - see this class's javadoc and
     * {@link #testReplaceRangeEndingOnATouchedBoundaryEmptiesBothPieces}, which builds them on purpose -
     * so a failure here means this fixture stopped producing the shape its assertions assume, not that a
     * global invariant broke.
     */
    private static void assertFixturePiecesDoNotTouch(String tableName) throws Exception {
        final TableToken tt = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = dayPartitionIndex(txReader);
            if (partitionIndex < 0) {
                return;
            }
            final PartitionGeometry geometry = reader.getGeometry();
            for (int p = 1, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                final long prevHi = geometry.getPieceTimestampHi(partitionIndex, p - 1);
                final long curLo = geometry.getPieceTimestampLo(partitionIndex, p);
                Assert.assertTrue(
                        "pieces " + (p - 1) + " and " + p + " touch or overlap: " + describePieces(tableName),
                        prevHi < curLo
                );
            }
        }
    }

    /**
     * Builds the touching shape, from the same ingredients as
     * {@code O3CompositePartitionTest#testTieOnAnEarlierPieceFoundsThenMergesASinglePointPiece}: an early
     * backfill cuts a SMALL head piece {@code [.., V]} off the day, and a later commit whose lowest row
     * sits exactly on {@code V} is spared from that piece's claim
     * ({@code O3CompositeMergeStrategy.computeActions}'s {@code spareTie}). Its rows land in the gap
     * between the head piece and the backfilled one, founding a NEW piece that starts at {@code V} and
     * reaches ABOVE it - {@code [.., V]} immediately followed by {@code [V, V + TIE_GAP_MICROS]}. The head
     * piece is deliberately left under {@code 2 * minPieceRows} so the pre-split cannot cut the tie out
     * into a piece of its own first.
     * <p>
     * Asserts the shape it built, so a planner change that stops producing touching pieces fails as a
     * fixture assertion instead of passing the callers silently.
     * <p>
     * Leaves table {@code x} with 5_783 rows: one on 2020-02-06, 5_760 base rows across the day, 20
     * backfill rows, and the tie commit's two rows - {@code i = -1} at {@code V} and {@code i = -2} at
     * {@code V + TIE_GAP_MICROS}.
     *
     * @return {@code V}, the timestamp the two pieces share
     */
    private static long buildTouchingPiecesFixture() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A later day, so DAY is never the active partition and every further write to it goes
        // through the O3 composite path.
        execute("INSERT INTO x SELECT x::INT + 90_000, timestamp_sequence('2020-02-06', 60*1_000_000L) ts FROM long_sequence(1)");
        drainWalQueue();
        execute("INSERT INTO x SELECT x::INT, timestamp_sequence('" + DAY + "', 15*1_000_000L) ts FROM long_sequence(5_760)");
        drainWalQueue();
        // A backfill right after the start of the day: the pre-split carves it out, leaving a head
        // piece of about twenty rows in front of it.
        execute("INSERT INTO x SELECT x::INT + 70_000, timestamp_sequence('" + DAY + "T00:05:07', 5*1_000_000L) ts FROM long_sequence(20)");
        drainWalQueue();

        final TableToken xt = engine.verifyTableName("x");
        final long v;
        final long nextPieceTsLo;
        try (TableReader reader = engine.getReader(xt)) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = dayPartitionIndex(reader.getTxFile());
            Assert.assertTrue("the backfill should have cut the day into pieces: " + describePieces("x"),
                    geometry.getPieceCount(partitionIndex) > 1);
            v = geometry.getPieceTimestampHi(partitionIndex, 0);
            nextPieceTsLo = geometry.getPieceTimestampLo(partitionIndex, 1);
        }
        Assert.assertTrue("the head piece should span a real range, not a single instant: " + describePieces("x"),
                v > MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z"));
        final long vUpper = v + TIE_GAP_MICROS;
        Assert.assertTrue("the gap between the head piece and its neighbour is too narrow: " + describePieces("x"),
                vUpper < nextPieceTsLo);

        // Two rows in ONE commit: the lower one ties the head piece's tsHi, the upper one sits in the
        // gap above it. The tie is spared, so both found a single new piece [v, v + TIE_GAP_MICROS]
        // TOUCHING the head piece at v.
        execute("INSERT INTO x VALUES ((-1)::INT, " + v + "::TIMESTAMP), ((-2)::INT, " + vUpper + "::TIMESTAMP)");
        drainWalQueue();

        long touchingTsHi = Long.MIN_VALUE;
        try (TableReader reader = engine.getReader(xt)) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = dayPartitionIndex(reader.getTxFile());
            for (int p = 1, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                if (geometry.getPieceTimestampLo(partitionIndex, p) == v
                        && geometry.getPieceTimestampHi(partitionIndex, p - 1) == v) {
                    touchingTsHi = geometry.getPieceTimestampHi(partitionIndex, p);
                    break;
                }
            }
        }
        Assert.assertTrue(
                "fixture did not build a piece touching its predecessor at v and reaching above it: " + describePieces("x"),
                touchingTsHi > v
        );
        Assert.assertEquals("fixture put the wrong number of rows at v: " + describePieces("x"), 2, countAt("x", v));
        return v;
    }

    private static long countAt(String tableName, long ts) throws Exception {
        return scalar("select count() c from " + tableName + " where ts = " + ts);
    }

    private static int dayPartitionIndex(TxReader txReader) {
        return txReader.getPartitionIndex(MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z"));
    }

    private static String describePieces(String tableName) throws Exception {
        final TableToken tt = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = dayPartitionIndex(txReader);
            if (partitionIndex < 0) {
                return "no partition for " + DAY;
            }
            final PartitionGeometry geometry = reader.getGeometry();
            final StringBuilder sink = new StringBuilder("pieces=[");
            for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                if (p > 0) {
                    sink.append(", ");
                }
                sink.append(p).append(":[")
                        .append(geometry.getPieceTimestampLo(partitionIndex, p)).append("..")
                        .append(geometry.getPieceTimestampHi(partitionIndex, p)).append("]@")
                        .append(geometry.getPieceRowOffset(partitionIndex, p)).append('+')
                        .append(geometry.getPieceRowCount(partitionIndex, p));
            }
            return sink.append("] E=").append(geometry.getE(partitionIndex)).toString();
        }
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }
}
