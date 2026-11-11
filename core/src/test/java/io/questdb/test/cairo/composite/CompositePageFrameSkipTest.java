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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A composite partition emits ONE page frame per PIECE, so a partition of thousands of pieces hands the
 * skip of {@code LIMIT -n} thousands of frames to walk. Pricing a frame's column addresses probes the aux
 * vector of every var-size column ({@code getDataVectorSizeAt}), faulting in one page of the mmapped aux
 * file per column per frame - which is what made {@code SELECT * FROM t LIMIT -10} take hundreds of
 * milliseconds on a wide composite partition while {@code ORDER BY ts DESC LIMIT 10} returned at once.
 * <p>
 * A frame the caller will discard whole needs no addresses at all, and does not even have to be cut at the
 * boundaries a readable frame is cut at - one skeleton stands in for the whole run. These tests drive the page
 * frame cursor exactly as {@code PageFrameRecordCursorImpl.skipRows()} does and assert both: the skipped frames
 * come back address-less, the skip costs a bounded number of them however many pieces it crosses, and the frame
 * the skip LANDS on still carries real addresses.
 */
public class CompositePageFrameSkipTest extends AbstractCairoTest {

    // ts, sym, txt, price -- txt is the var-size column whose aux probe the skip must not pay for.
    private static final int TXT_COLUMN = 2;

    @Test
    public void testAscendingSkipCollapsesSkippedPieceFrames() throws Exception {
        assertMemoryLeak(() -> {
            final int pieceCount = createManyPieceTable("t_skip_asc");
            assertSkipCollapsesPieceFrames(PartitionFrameCursorFactory.ORDER_ASC, pieceCount);
        });
    }

    @Test
    public void testDescendingSkipCollapsesSkippedPieceFrames() throws Exception {
        assertMemoryLeak(() -> {
            final int pieceCount = createManyPieceTable("t_skip_desc");
            assertSkipCollapsesPieceFrames(PartitionFrameCursorFactory.ORDER_DESC, pieceCount);
        });
    }

    /**
     * The skip must not change what the query returns: the tail rows of a many-piece composite partition
     * plus the following plain partition, in timestamp order.
     */
    @Test
    public void testNegativeLimitOverManyPiecesReturnsTheTail() throws Exception {
        assertMemoryLeak(() -> {
            createManyPieceTable("t_skip_rows");
            assertQuery("SELECT ts, sym, txt FROM t_skip_rows LIMIT -4")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\ttxt
                            2020-02-03T23:59:15.000000Z\tF\tabcdefghij5758
                            2020-02-03T23:59:30.000000Z\tF\tabcdefghij5759
                            2020-02-03T23:59:45.000000Z\tF\tabcdefghij5760
                            2020-02-05T00:00:00.000000Z\tZ\tzz
                            """);
            // The same tail, reached the other way round, must agree row for row.
            assertQuery("SELECT ts, sym, txt FROM t_skip_rows ORDER BY ts DESC LIMIT 4")
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\ttxt
                            2020-02-05T00:00:00.000000Z\tZ\tzz
                            2020-02-03T23:59:45.000000Z\tF\tabcdefghij5760
                            2020-02-03T23:59:30.000000Z\tF\tabcdefghij5759
                            2020-02-03T23:59:15.000000Z\tF\tabcdefghij5758
                            """);
        });
    }

    /**
     * Walks the page frame cursor the way {@code PageFrameRecordCursorImpl.skipRows()} does - handing each
     * {@code next()} the skip still outstanding - and checks both halves of the contract: a discarded frame
     * costs no column addresses, and the whole skip costs a bounded number of frames however many pieces it
     * crosses. Counting frames is what pins the win down: address-less frames alone would still leave the
     * cursor walking one frame per piece.
     */
    private void assertSkipCollapsesPieceFrames(int order, int pieceCount) throws Exception {
        final String suffix = order == PartitionFrameCursorFactory.ORDER_ASC ? "asc" : "desc";
        try (
                RecordCursorFactory factory = select("SELECT ts, sym, txt, price FROM t_skip_" + suffix);
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)
        ) {
            long skipTarget = cursor.size() - 4;
            Assert.assertTrue("test precondition: the table must be big enough to skip", skipTarget > 0);

            int skippedFrames = 0;
            PageFrame frame;
            PageFrame landing = null;
            while ((frame = cursor.next(skipTarget)) != null) {
                final long frameSize = frame.getPartitionHi() - frame.getPartitionLo();
                if (frameSize > skipTarget) {
                    landing = frame;
                    break;
                }
                Assert.assertEquals(
                        "skipped frame " + skippedFrames + " must publish no var-size data address",
                        0,
                        frame.getPageAddress(TXT_COLUMN)
                );
                Assert.assertEquals(
                        "skipped frame " + skippedFrames + " must publish no var-size aux address",
                        0,
                        frame.getAuxPageAddress(TXT_COLUMN)
                );
                skipTarget -= frameSize;
                skippedFrames++;
            }

            // The table is two partitions, so a skip that crosses both can need at most one skeleton each.
            Assert.assertTrue(
                    "a skip over " + pieceCount + " pieces must not cost a frame per piece, saw " + skippedFrames,
                    skippedFrames <= 2
            );
            Assert.assertNotNull("the skip must land on a frame", landing);
            Assert.assertTrue(
                    "the landing frame is read, so it must carry a real aux address",
                    landing.getAuxPageAddress(TXT_COLUMN) != 0
            );

            // Draining the rest must still produce the four rows the skip stopped short of, one piece frame at
            // a time - so the collapse above applies to the skip only, not to the readable scan behind it.
            long rowsAfterSkip = landing.getPartitionHi() - landing.getPartitionLo();
            while ((frame = cursor.next()) != null) {
                rowsAfterSkip += frame.getPartitionHi() - frame.getPartitionLo();
            }
            Assert.assertEquals("the skip must stop exactly four rows short of the end", 4, rowsAfterSkip);
        }
    }

    /**
     * Builds a table whose first partition is composite with dozens of pieces: a day of filler rows, then
     * one backdated single-row commit per WAL drain, each landing in a cold gap of its own, so every commit
     * pre-splits rather than merges. A later plain partition follows, so the skip also has to cross a
     * partition boundary.
     */
    private int createManyPieceTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        // A production-sized partition pre-splits on its own at the 50MB default; shrink the threshold so a
        // fixture small enough to run in a unit test splits the same way.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        // Keep compaction off the pieces, so the count this test relies on is the one the commits produced.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);

        execute("CREATE TABLE " + tableName + " (" +
                "ts TIMESTAMP, sym SYMBOL, txt VARCHAR, price DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName +
                " SELECT timestamp_sequence('2020-02-03', 15 * 1000000L), 'F'::SYMBOL, 'abcdefghij' || x, x * 1.0" +
                " FROM long_sequence(5760)");
        // A later day, so 2020-02-03 is never the active partition and every write below is out of order.
        execute("INSERT INTO " + tableName + " VALUES ('2020-02-05', 'Z', 'zz', 1.0)");
        drainWalQueue();

        for (int i = 0; i < 30; i++) {
            execute("INSERT INTO " + tableName + " VALUES ('2020-02-03T"
                    + String.format("%02d", i % 24) + ":" + String.format("%02d", (i * 7) % 60)
                    + ":07', 'G', 'hello" + i + "', 1.0)");
            drainWalQueue();
        }

        final TableToken tt = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(tt)) {
            Assert.assertTrue(
                    "test precondition: partition 0 must be composite",
                    reader.getTxFile().isPartitionComposite(0)
            );
            final int pieceCount = reader.getGeometry().getPieceCount(0);
            Assert.assertTrue(
                    "test precondition: partition 0 must hold many pieces, has " + pieceCount,
                    pieceCount > 10
            );
            return pieceCount;
        }
    }
}
