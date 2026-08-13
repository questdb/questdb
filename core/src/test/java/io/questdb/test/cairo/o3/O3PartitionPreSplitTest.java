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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end tests for the cluster-driven pre-split: a WAL block apply whose transactions cluster into a
 * few narrow time strides of a mid partition cuts that partition at the cold gaps, so the merge rewrites
 * only the hot strides. Each case is validated against an independent union - which never touches the
 * composite machinery - and against the physically-written-rows metric, which is the write-amplification
 * win itself.
 * <p>
 * These scenarios are ported from the earlier split implementation, where a piece was a {@code _txn}
 * record of its own and the assertions read it as one: {@code getPartitionCount()} counted pieces,
 * {@code getPartitionTopByTimestamp()} was a piece's row offset, and two pieces of one directory were
 * recognised by sharing a {@code (dirTimestamp, nameTxn)} pair. None of that holds here. A partition is
 * ONE record whatever its shape, and its pieces live in {@code _geometry}, so every geometry assertion
 * below goes through {@link PartitionGeometry} at a partition index and a piece ordinal. The SCENARIOS
 * are the same; only the way the shape is read has changed.
 */
public class O3PartitionPreSplitTest extends AbstractCairoTest {

    private static final long DAY_03 = MicrosTimestampDriver.floor("2020-02-03T00:00:00.000000Z");

    /**
     * A block apply whose transactions cluster into two narrow strides of a dense mid partition. The cold
     * gaps between them are cut away, so the merge rewrites the two hot strides and leaves the rest of the
     * day where it is.
     */
    @Test
    public void testBlockApplyPreSplitsClusteredMidPartition() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // ~50-row min cold-gap / cut floor for this schema, so the 4h gaps between the clusters
            // qualify easily while minute-scale jitter does not.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // Day 2020-02-03: 5760 rows (one per 15s). Day 2020-02-04: a small tail, so the dense day is
            // a MID partition rather than the one being appended to.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " x::INT i," +
                            " -x j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j, rnd_str(5,16,2) AS str," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();

            // An independent snapshot plus the batches, as plain tables. A union over them never exercises
            // the composite machinery, so a corruption in x cannot cancel out of the comparison.
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");
            final StringBuilder union = new StringBuilder("(SELECT * FROM x0");
            for (int b = 0; b < 6; b++) {
                // Clusters at 04:00 and 20:00, ~17min wide, offset 7s off the 15s grid so the rows
                // interleave without colliding on a timestamp.
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (" +
                                "SELECT" +
                                " x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " -x - " + (1_000_000L * (b + 1)) + " AS j," +
                                " rnd_str(5,16,2) AS str," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts" +
                                " FROM long_sequence(200))"
                );
                union.append(" UNION ALL SELECT * FROM z").append(b);
            }
            union.append(") ORDER BY ts");

            final long physicalBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            // Six pending WAL txns -> one block apply -> the cut is decided ahead of the merge.
            for (int b = 0; b < 6; b++) {
                execute("INSERT INTO x SELECT * FROM z" + b);
            }
            drainWalQueue();
            final long physicalRows = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalBefore;

            // The mid partition was cut into pieces, and the cold ones cost nothing to keep.
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 4);
            assertNoOverlappingPieces("x");
            // The write-amplification win: rewriting the whole day would be 5760 rows plus the batches.
            Assert.assertTrue(
                    "physically written rows too high, the pre-split was ineffective: " + physicalRows,
                    physicalRows < 3000
            );

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union.toString(), "x", LOG);

            // Reopen from disk, so the geometry is read back off _txn and _geometry rather than out of
            // anything still resident.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union.toString(), "x", LOG);

            // Fold everything back and re-verify, with the pieces as squash sources.
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union.toString(), "x", LOG);
        });
    }

    /**
     * A cluster that lands in a DATA GAP of the piece it routes to must not drag that piece through a
     * merge. Nothing of the piece has to move: every incoming row sits strictly ABOVE the piece's last
     * row, so the commit can write the new rows alone at the shared file tail and record them as a piece
     * of their own, leaving the existing piece byte-for-byte where it is.
     * <p>
     * The fixture puts a two-hour hole in the day's data and aims a narrow backdated cluster into it. The
     * clusterer cuts at the hole's edges, but a cut is a ROW: the first row at or above either edge is the
     * same row - the one that opens the upper block - so the two cuts collapse into one and the piece
     * below the hole OWNS the hole (routing is by tsLo, so a piece runs up to the next piece's floor). The
     * cluster therefore routes into a 4800-row piece whose rows all sit hours below it.
     */
    @Test
    public void testClusterInADataGapFoundsItsOwnPieceInsteadOfMerging() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Doubles as the cold-gap admission floor and as the "worth avoiding" floor: the receiving
            // piece holds 4800 rows, an order of magnitude above it.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // 00:00:00 - 19:59:45, then a two-hour hole, then 22:00:00 - 23:59:45.
            final String lower = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(4800)";
            final String upper = "SELECT x::INT + 100000 i," +
                    " timestamp_sequence('2020-02-03T22:00:00', 15*1000000L) ts FROM long_sequence(480)";
            final String nextDay = "SELECT x::INT + 500000 i," +
                    " timestamp_sequence('2020-02-05', 60*1000000L) ts FROM long_sequence(50)";
            // 21:00:00 - 21:09:55, inside the hole and above every row of the piece that owns it.
            final String cluster = "SELECT x::INT + 200000 i," +
                    " timestamp_sequence('2020-02-03T21:00:00', 5*1000000L) ts FROM long_sequence(120)";

            execute("CREATE TABLE x AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + upper);
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();

            execute("INSERT INTO x " + cluster);
            drainWalQueue();
            Assert.assertFalse("the cluster apply suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final long clusterLo = MicrosTimestampDriver.floor("2020-02-03T21:00:00.000000Z");
            final long clusterHi = MicrosTimestampDriver.floor("2020-02-03T21:09:55.000000Z");
            final long lowerLastRow = MicrosTimestampDriver.floor("2020-02-03T19:59:45.000000Z");

            // The cut happened and left the blocks in one directory.
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertNoOverlappingPieces("x");
            // The cluster occupies a piece of its own, whose recorded bounds cover it.
            assertPieceCoversRange("x", 120, clusterLo, clusterHi);
            // The piece the cluster routed into never moved: still at file row 0, still 4800 rows, and its
            // recorded top is still its own last row.
            assertPieceUntouched("x", DAY_03, 0, 4800, lowerLastRow);

            // The win itself: the commit writes the cluster, not the piece it landed in.
            final long written = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore;
            Assert.assertTrue(
                    "the commit rewrote the piece instead of appending the cluster [physicallyWrittenRows="
                            + written + ']',
                    written < 500
            );

            // The oracle: an independent union, asserted as-is and across a cold open.
            execute("CREATE TABLE o AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO o " + upper);
            execute("INSERT INTO o " + nextDay);
            execute("INSERT INTO o " + cluster);
            final String expected = "o ORDER BY ts, i";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts, i", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts, i", LOG);

            // A squash folds the pieces back into one partition; the rows must survive that too. Piece
            // bounds are NOT asserted afterwards - a partition that is no longer composite carries no
            // _geometry record, so its single piece reads its tsHi back as LONG_NULL.
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts, i", LOG);
        });
    }

    /**
     * No two pieces of one partition may claim the same file rows. Each piece reads
     * {@code [rowOffset, rowOffset + rowCount)} of the shared column files, so an overlap means one
     * piece's rows are physically another's - which a var-size column shows as an aux entry pointing at
     * no value, and a fixed-size one hides completely.
     */
    private static void assertNoOverlappingPieces(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = partitionIndexOfDay(reader);
            for (int i = 0, n = geometry.getPieceCount(partitionIndex); i < n; i++) {
                final long loI = geometry.getPieceRowOffset(partitionIndex, i);
                final long hiI = loI + geometry.getPieceRowCount(partitionIndex, i);
                for (int j = 0; j < i; j++) {
                    final long loJ = geometry.getPieceRowOffset(partitionIndex, j);
                    final long hiJ = loJ + geometry.getPieceRowCount(partitionIndex, j);
                    Assert.assertFalse(
                            "two pieces claim the same file rows [pieceA=" + j
                                    + ", rowsA=[" + loJ + ", " + hiJ + "), pieceB=" + i
                                    + ", rowsB=[" + loI + ", " + hiI + ")]",
                            loI < hiJ && loJ < hiI
                    );
                }
            }
        }
    }

    /**
     * Asserts that some piece holds exactly {@code rowCount} rows and that its RECORDED bounds cover
     * {@code [loTs, hiTs]}. Recorded, not derived: the point of the assertion is that the piece's own
     * {@code (tsLo, tsHi)} describe the rows it holds, so a future row in that range routes to it. A tsHi
     * of {@code LONG_NULL} fails - a piece of a composite partition carries a {@code _geometry} record and
     * therefore a real tsHi.
     */
    private static void assertPieceCoversRange(String tableName, long rowCount, long loTs, long hiTs) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = partitionIndexOfDay(reader);
            for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                if (geometry.getPieceRowCount(partitionIndex, p) != rowCount) {
                    continue;
                }
                final long tsLo = geometry.getPieceTimestampLo(partitionIndex, p);
                final long tsHi = geometry.getPieceTimestampHi(partitionIndex, p);
                if (tsLo <= loTs && tsHi != Numbers.LONG_NULL && tsHi >= hiTs) {
                    return;
                }
            }
            Assert.fail("no piece of " + rowCount + " rows records bounds covering ["
                    + loTs + ", " + hiTs + "]; " + describePieces(tableName));
        }
    }

    /**
     * Asserts the piece at {@code tsLo} still starts at file row {@code rowOffset}, still holds
     * {@code rowCount} rows, and still records {@code tsHi} as its top - i.e. the commit neither
     * relocated nor merged it. A relocation moves the row offset; a merge changes the row count.
     */
    private static void assertPieceUntouched(String tableName, long tsLo, long rowOffset, long rowCount, long tsHi) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = partitionIndexOfDay(reader);
            for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                if (geometry.getPieceTimestampLo(partitionIndex, p) != tsLo) {
                    continue;
                }
                Assert.assertEquals("piece was relocated", rowOffset, geometry.getPieceRowOffset(partitionIndex, p));
                Assert.assertEquals("piece was merged", rowCount, geometry.getPieceRowCount(partitionIndex, p));
                Assert.assertEquals("piece top timestamp", tsHi, geometry.getPieceTimestampHi(partitionIndex, p));
                return;
            }
            Assert.fail("no piece at " + tsLo + "; " + describePieces(tableName));
        }
    }

    /**
     * Every piece of 2020-02-03 as {@code ordinal:[tsLo..tsHi]@rowOffset+rowCount}, for failure messages.
     */
    private static String describePieces(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = partitionIndexOfDay(reader);
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
            return sink.append(']').toString();
        }
    }

    private static int partitionIndexOfDay(TableReader reader) {
        final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
        Assert.assertTrue("no partition at 2020-02-03", partitionIndex > -1);
        return partitionIndex;
    }

    /**
     * How many pieces the table holds for 2020-02-03, the day every test here cuts.
     */
    private static int piecesOfDay(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getGeometry().getPieceCount(partitionIndexOfDay(reader));
        }
    }
}
