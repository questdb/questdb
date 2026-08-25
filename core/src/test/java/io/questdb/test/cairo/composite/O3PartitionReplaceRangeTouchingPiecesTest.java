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
 * auditing {@code TableWriter.moveTailToFreshPartition} (see PARTITION_COMPACTION_state.md). Two adjacent
 * pieces sharing a boundary value would be genuinely ambiguous - {@code O3CompositeMergeStrategy}'s piece
 * range check is inclusive on both ends ({@code tsLo <= ts <= tsHi}) - and {@code findPieceContaining}
 * (used to resolve a REPLACE RANGE commit's own bounds to a piece, {@code O3PartitionJob.java:270-278})
 * always resolves such a tie to the FIRST (earlier) piece in ordinal order, never the second. If pieces
 * could touch, a REPLACE RANGE commit whose bound landed exactly on the shared value could silently miss
 * the later piece - rows that should be removed would survive with no error, no assertion, no suspended
 * table.
 * <p>
 * This class does not reproduce that miss - genuine touching turns out NOT to be constructible with only
 * two distinct timestamp values, no matter how the commits are ordered: {@code O3CompositeMergeStrategy}
 * always routes a row whose timestamp exactly equals an existing piece's {@code tsLo} or {@code tsHi} into
 * a MERGE of that piece rather than a new adjacent one ({@code lastAtOrBelow}/{@code findLastBelow} in
 * {@code computeActions}), and a piece touching a neighbour at value {@code V} while holding only two
 * distinct values collapses into two pieces sharing the SAME {@code tsLo} - which
 * {@code PartitionGeometry.addPiece} already asserts against directly. Reaching genuine touching (a piece
 * spanning {@code [V1, V2]} immediately followed by one spanning {@code [V2, V3]}, sharing exactly
 * {@code V2}) needs a THIRD distinct value and a specific shape; it is not attempted here.
 * <p>
 * What this class DOES verify, and what is still a real, useful regression to lock: composite pieces built
 * from wildly out-of-order commits over data with almost no distinct timestamps (the shape most likely to
 * stress every tie-breaking rule in the piece planner) never end up touching in practice, and a REPLACE
 * RANGE commit that removes every row at exactly one of two surviving timestamps removes precisely those
 * rows and nothing else - regardless of what order the composite pieces were built in.
 */
public class O3PartitionReplaceRangeTouchingPiecesTest extends AbstractCairoTest {

    private static final String DAY = "2020-02-03";

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
            assertPiecesDoNotTouch("x");

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
            assertPiecesDoNotTouch("x");

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
            assertPiecesDoNotTouch("x");

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
            assertPiecesDoNotTouch("x");

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

    private static void appendAt(TableToken tt, long ts, long v) {
        try (WalWriter ww = engine.getWalWriter(tt)) {
            TableWriter.Row row = ww.newRow(ts);
            row.putLong(1, v);
            row.append();
            ww.commit();
        }
    }

    /**
     * Every adjacent pair of pieces in the day's own partition must have a real gap between them - see
     * this class's own javadoc for why a shared boundary value would be ambiguous.
     */
    private static void assertPiecesDoNotTouch(String tableName) throws Exception {
        final TableToken tt = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z"));
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

    private static long countAt(String tableName, long ts) throws Exception {
        return scalar("select count() c from " + tableName + " where ts = " + ts);
    }

    private static String describePieces(String tableName) throws Exception {
        final TableToken tt = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z"));
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
