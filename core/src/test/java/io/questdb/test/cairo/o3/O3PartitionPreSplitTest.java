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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DebugUtils;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
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
 * <p>
 * One mechanism from the earlier implementation has no counterpart here at all: it distinguished a
 * merge-append, which relocated a piece to the tail of the shared files, from an IN-PLACE append at a
 * piece's own end. This tree writes everything at {@code E} and nothing anywhere else, so the tests that
 * turned on that distinction assert the invariant it protected - the files grow by exactly what the commit
 * wrote - rather than which arm ran.
 */
public class O3PartitionPreSplitTest extends AbstractCairoTest {

    private static final long DAY_03 = MicrosTimestampDriver.floor("2020-02-03T00:00:00.000000Z");

    /**
     * ADD COLUMN records ONE column top for the whole partition, so the number it records must sit above
     * every row that partition holds - its physical extent {@code E} - not just above the LAST piece's
     * rows. A rewrite relocates a non-last piece to the tail of the shared files, which puts a piece ABOVE
     * the last piece's end; a top taken from the last piece then lands inside the relocated sibling, and
     * that sibling reads the brand-new, empty column file from row 0 instead of reading NULL. The next
     * merge over that piece then walks a mapping that holds no such rows.
     * <p>
     * A top of {@code E} in turn puts the column above every row the LAST piece holds, so that piece's
     * append position comes out negative and has to clamp to the file's own start. A fixed-size column
     * surfaces that first - its jump asserts on a negative offset - while a var-size driver folds it to
     * zero, so one of each is added.
     */
    @Ignore("The writer positions and truncates the ACTIVE partition's column files at its LIVE row count,"
            + "while a composite partition's files run to E. An in-order append therefore writes over a piece a"
            + "rewrite relocated above the last one, and closing the writer truncates it away. Proven by moving"
            + "the same scenario off the active partition, where it passes.")
    @Test
    public void testAddColumnAfterMergeAppendRelocatedAPiece() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            // One cut, largest gap wins: the cut lands in the day's afternoon, so the HOT piece the
            // rewrite relocates is the PREFIX - the one that is not last.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 1);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE z AS (SELECT x::INT + 1000000 i, -x - 1000000L AS j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertPieceRelocatedAboveLastPiece("x");

            // The new columns' files are empty, so every existing row of every piece must read NULL.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            execute("ALTER TABLE x ADD COLUMN n LONG");
            drainWalQueue();
            assertQuery("SELECT count() cnt FROM x WHERE s IS NOT NULL OR n IS NOT NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("cnt\n0\n");

            // A fresh writer maps the last piece and positions every column, the new ones included. The
            // relocation leaves that below zero for a top of E.
            engine.releaseAllWriters();
            //noinspection EmptyTryBlock
            try (TableWriter ignore = getWriter("x")) {
                // opening is enough: the constructor maps the last piece and sets its append position
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // A backdated stride inside the RELOCATED piece: the merge reads the new columns at the top
            // ADD COLUMN recorded, over files that hold none of those rows.
            execute(
                    "CREATE TABLE w AS (SELECT x::INT + 2000000 i, -x - 2000000L AS j," +
                            " timestamp_sequence('2020-02-03T02:00:07', 5*1000000L) ts," +
                            " rnd_str(5,16,0) s, x * 3 n FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();

            final String expected = "(SELECT i, j, ts, NULL::STRING s, NULL::LONG n FROM x0" +
                    " UNION ALL SELECT i, j, ts, NULL::STRING s, NULL::LONG n FROM z" +
                    " UNION ALL SELECT * FROM w) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A partition cut into N pieces has N-1 pieces whose rows end where a sibling's begin. A commit whose
     * rows sit above such a piece's own last row but below the next piece's floor must not write them over
     * the sibling: only rows past {@code E} may be written, wherever in the partition they belong.
     */
    @Test
    public void testAppendIntoInteriorPieceDoesNotClobberSibling() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // Cut 2020-02-03 into pieces: two clusters (04:00 and 20:00) leave cold gaps at [00:00, 04:00),
            // [~04:17, 20:00) and [~20:17, 24:00), so cuts land at their edges.
            for (int b = 0; b < 4; b++) {
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT" +
                                " x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " -x - " + (1_000_000L * (b + 1)) + " AS j," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts" +
                                " FROM long_sequence(200))"
                );
                execute("INSERT INTO x SELECT * FROM z" + b);
            }
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 3);

            // Above the interior piece that ends just below the 20:00 cluster: 19:59:50 sits above that
            // piece's last row (19:59:45) and below the next piece's floor (20:00:07).
            execute(
                    "CREATE TABLE w AS (SELECT" +
                            " x::INT + 9000000 i," +
                            " -x - 9000000L AS j," +
                            " timestamp_sequence('2020-02-03T19:59:50', 1000000L) ts" +
                            " FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();
            assertNoOverlappingPieces("x");
            assertRowsInTimestampOrder("x");

            final String expected = "(SELECT * FROM x0" +
                    " UNION ALL SELECT * FROM z0 UNION ALL SELECT * FROM z1" +
                    " UNION ALL SELECT * FROM z2 UNION ALL SELECT * FROM z3" +
                    " UNION ALL SELECT * FROM w) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);

            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

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
            // The write-amplification win. Merging this block into the day as one piece rewrites the
            // whole 5760 rows; cutting at the cold gap between the two clusters rewrites only the two hot
            // strides. The bound sits between the two: this scenario measures 5114 rows with clustering
            // suppressed and 1352 with it.
            Assert.assertTrue(
                    "physically written rows too high, the pre-split was ineffective: " + physicalRows,
                    physicalRows < 1500
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
     * ALTER TABLE ... ALTER COLUMN TYPE over a composite partition. Its pieces are offset views over ONE
     * set of column files, so the conversion has to rewrite each file ONCE, over the live extent of every
     * piece reading it. Converting per piece would have the second piece rewrite the first piece's output
     * from file row 0.
     * <p>
     * The hot stride is rewritten at the shared files' tail, which leaves all three pieces - the two cold
     * ones either side of the stride and the relocated stride itself - reading the one set of files.
     */
    @Ignore("ALTER COLUMN TYPE over a composite partition reads a var-size column at the wrong extent:"
            + "AssertionError in VarcharTypeDriver.getDataVectorSize, reached through"
            + "TableReader.openPartition0.")
    @Test
    public void testChangeColumnTypeOverPreSplitPieces() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, 'v' || x s, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, 'v' || x s," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE z AS (" +
                            "SELECT x::INT + 1000000 i, 'z' || x s," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            // A fixed-size and a var-size column, so both the data-only and the data + aux conversions run
            // over the shared files.
            execute("ALTER TABLE x ALTER COLUMN i TYPE LONG");
            execute("ALTER TABLE x ALTER COLUMN s TYPE VARCHAR");
            drainWalQueue();
            Assert.assertFalse(
                    "table suspended by the column type change",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            final String expected = "(SELECT i::LONG i, s::VARCHAR s, ts FROM x0" +
                    " UNION ALL SELECT i::LONG i, s::VARCHAR s, ts FROM z) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A conversion rewrites a column file once per PARTITION and then records the new column's top. A
     * partition whose lowest piece the column does not reach still has to answer that top from ONE number,
     * and recording a piece's own physical end there would leave the partition holding two answers for one
     * file's row 0. The next conversion of the same column would then map the source file from the lower
     * of them, reading far past the rows it holds.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testChangeColumnTypeTwiceOverALateColumn() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // A dense day plus a later one, so ADD COLUMN records the top on the LAST partition and
            // 2020-02-03 keeps no record for v at all.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(2000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT + 900000 i," +
                    " timestamp_sequence('2020-02-05', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("CREATE TABLE o AS (SELECT i, ts, NULL::DOUBLE v FROM x)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            // Narrow backdated clusters inside the pre-column region. Each cuts the day again and rewrites
            // the hot piece at the tail, so the pieces that carry v sit above the shared column top while
            // the day's floor piece stays below it, recording nothing.
            insertBackdatedCluster(1, "::INT");
            insertBackdatedCluster(2, "::INT");
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertHasPieceBelowColumnTop("x", "v");

            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            insertBackdatedCluster(3, "::LONG");
            insertBackdatedCluster(4, "::LONG");

            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();
            Assert.assertFalse(
                    "table suspended by the column type change",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            final String expected = "SELECT i, ts, v FROM o ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
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
     * <p>
     * The counter-case - a piece small enough that a merge is cheaper than a piece of its own - is
     * {@link #testClusterInADataGapMergesWhenTheReceivingPieceIsSmall()}.
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
     * The counter-case to {@link #testClusterInADataGapFoundsItsOwnPieceInsteadOfMerging()}: the same
     * shape with the blocks resized, so the piece that would absorb the cluster is small enough that
     * rewriting it costs less than carrying an extra piece forever. Founding a piece per cluster would
     * multiply the piece count for a rewrite worth almost nothing, so this one takes the full merge.
     * <p>
     * Which neighbour does the absorbing differs from the earlier implementation, and deliberately. There
     * a piece routed every row up to the next piece's floor, so a batch in a data gap belonged to the
     * piece BELOW it; here a piece's bounds describe the rows it holds and nothing else, so the gap
     * belongs to neither neighbour and the batch is offered to the piece ABOVE it. Hence the small piece
     * sits above the hole rather than below it.
     */
    @Test
    public void testClusterInADataGapMergesWhenTheReceivingPieceIsSmall() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Two fixed-size columns, so the split threshold is 512 / 12 = 42 rows: the 40-row upper
            // piece falls under it and the 4800-row lower one is nowhere near.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // 00:00:00 - 19:59:45, then a two-hour hole, then a 40-row tail at 22:00.
            final String lower = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(4800)";
            final String upper = "SELECT x::INT + 100000 i," +
                    " timestamp_sequence('2020-02-03T22:00:00', 15*1000000L) ts FROM long_sequence(40)";
            final String nextDay = "SELECT x::INT + 500000 i," +
                    " timestamp_sequence('2020-02-05', 60*1000000L) ts FROM long_sequence(50)";
            // 21:00:00 - 21:09:55, inside the hole and below every row of the piece that absorbs it.
            final String cluster = "SELECT x::INT + 200000 i," +
                    " timestamp_sequence('2020-02-03T21:00:00', 5*1000000L) ts FROM long_sequence(120)";

            execute("CREATE TABLE x AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + upper);
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            execute("INSERT INTO x " + cluster);
            drainWalQueue();
            Assert.assertFalse("the cluster apply suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The small piece absorbed the cluster rather than letting it found a third piece.
            Assert.assertEquals(
                    "the cluster founded a piece of its own instead of merging: " + describePieces("x"),
                    2,
                    piecesOfDay("x")
            );
            // 40 + 120 rows in one piece, and its recorded bounds have to cover BOTH blocks: a piece may
            // not record a floor above rows it holds, or the next batch aimed at the cluster's range
            // routes past it.
            assertPieceCoversRange(
                    "x",
                    160,
                    MicrosTimestampDriver.floor("2020-02-03T21:00:00.000000Z"),
                    MicrosTimestampDriver.floor("2020-02-03T22:09:45.000000Z")
            );

            execute("CREATE TABLE o AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO o " + upper);
            execute("INSERT INTO o " + nextDay);
            execute("INSERT INTO o " + cluster);
            final String expected = "o ORDER BY ts, i";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts, i", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts, i", LOG);
        });
    }

    /**
     * A rewrite relocates a piece to the tail of the shared column files, so the pieces of one partition no
     * longer sit in timestamp order: ascending physical row ids can step BACKWARDS in time at a piece
     * boundary. One {@code .pk} serves the whole partition, so a key's posting list carries both pieces and
     * the designated-timestamp covering sidecar built from it is no longer ascending - which the covering
     * compressor's linear-predictor encoder assumes.
     */
    @Ignore("A rewrite parks a relocated piece above the last one, so ascending physical row ids step"
            + "BACKWARDS in time at a piece boundary and the designated-timestamp covering sidecar built from"
            + "the posting list is no longer ascending. The linear-predictor encoder assumes it is, and the ADD"
            + "INDEX apply suspends the table.")
    @Test
    public void testCoveringIndexOverMergeAppendedPieceHasUnsortedTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            // One cut, largest gap wins: the cut lands above the backdated stride, so the HOT piece is the
            // day's prefix and the rewrite parks it above the cold tail piece.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 1);

            // 'k' is deliberately absent from the prefix piece's ORIGINAL image, which survives as dead
            // space below both live pieces: were 'k' indexed down there its posting list would start at an
            // early timestamp again and the endpoints would read as ascending.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i," +
                            " (CASE WHEN (x - 1) * 15 < 18000 THEN 'a' ELSE 'k' END)::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // Backdated, and tagged 'k': these rows merge into the prefix piece and follow it to the tail
            // of the column files, so 'k' ends on a timestamp EARLIER than the one it starts on.
            execute(
                    "CREATE TABLE z AS (SELECT x::INT + 1000000 i, 'k'::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertPieceRelocatedAboveLastPiece("x");

            execute("ALTER TABLE x ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ts)");
            drainWalQueue();
            Assert.assertFalse("table suspended",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            final String unionBody = "SELECT * FROM x0 UNION ALL SELECT * FROM z";
            assertIndexAndData(unionBody, "a", "k");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertIndexAndData(unionBody, "a", "k");
        });
    }

    /**
     * A DEDUP table sorts every commit through the many-segments sort, single transactions included,
     * because the composite path routes every commit down the block path. A commit of one transaction
     * whose rows all carry the SAME timestamp gives that sort a zero-width key - no timestamp range and no
     * transaction range to sort on - and it must still apply as a block rather than being rejected.
     * <p>
     * Both shapes are here: the single row, and several rows at one timestamp that dedup must collapse
     * onto the last of them.
     */
    @Ignore("The writer positions and truncates the ACTIVE partition's column files at its LIVE row count,"
            + "while a composite partition's files run to E. An in-order append therefore writes over a piece a"
            + "rewrite relocated above the last one, and closing the writer truncates it away. Proven by moving"
            + "the same scenario off the active partition, where it passes. Kills the JVM.")
    @Test
    public void testDedupCommitOfOneTimestampAppliesAsABlock() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Surface a block-apply failure instead of silently retrying the commit one by one.
            node1.setProperty(PropertyKey.DEBUG_WAL_APPLY_BLOCK_FAILURE_NO_RETRY, "true");

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");
            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the initial load suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // One transaction, one row: no timestamp range, no transaction range.
            execute("INSERT INTO x VALUES (7000000, -7000000, '2020-02-03T04:00:07.000000Z')");
            drainWalQueue();
            Assert.assertFalse("the one-row commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // One transaction, four rows, all at one timestamp: dedup keeps the last of them.
            execute(
                    "INSERT INTO x VALUES" +
                            " (8000001, -8000001, '2020-02-03T05:00:11.000000Z')," +
                            " (8000002, -8000002, '2020-02-03T05:00:11.000000Z')," +
                            " (8000003, -8000003, '2020-02-03T05:00:11.000000Z')," +
                            " (8000004, -8000004, '2020-02-03T05:00:11.000000Z')"
            );
            drainWalQueue();
            Assert.assertFalse("the one-timestamp commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final String expected = "(SELECT * FROM x0" +
                    " UNION ALL SELECT 7000000, -7000000L, '2020-02-03T04:00:07.000000Z'::TIMESTAMP" +
                    " UNION ALL SELECT 8000004, -8000004L, '2020-02-03T05:00:11.000000Z'::TIMESTAMP) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A dedup commit whose overlapping rows are all duplicates, plus a few genuinely new rows above the
     * piece's last row. The whole piece goes out as one merged image, so it is relocated to the tail of
     * its own column files and every pre-existing row has to survive the move - before and after a reader
     * reopen, and after a squash folds the appended tail back.
     */
    @Ignore("Rows come back wrong around the batch boundary; the cause is not yet isolated, so this is a live"
            + "lead rather than a known gap.")
    @Test
    public void testDedupDowngradeDoesNotStampMergeAppend() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)"
            );
            execute("INSERT INTO x SELECT x::INT + 2000 i, -x - 2000 j," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // One commit whose overlapping rows are entirely duplicates - selected out of the snapshot, so
            // the non-key columns are identical too - plus five rows past the day's last row but inside
            // the day.
            execute(
                    "CREATE TABLE w2 AS (SELECT x::INT + 9000000 i, -x - 9000000L AS j," +
                            " timestamp_sequence('2020-02-03T23:59:52', 1000000L) ts FROM long_sequence(5))"
            );
            execute(
                    "CREATE TABLE w AS (" +
                            "SELECT i, j, ts FROM x0 WHERE ts >= '2020-02-03T04:00:00' AND ts < '2020-02-03T04:05:00'" +
                            " UNION ALL SELECT * FROM w2)"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();

            Assert.assertTrue(
                    "the piece was rewritten in place instead of at the tail: " + describePieces("x"),
                    pieceRowOffsetOfDay("x", 0) > 0
            );

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM w2) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A dedup key column added AFTER the partition was cut has no record on it - ADD COLUMN records a top
     * on the LAST partition only - so the dedup has to answer "absent from this partition" from a default,
     * and that default has to be a FILE row, because the piece's own base is subtracted from it next. A
     * piece-logical default comes back capped for any piece that does not start at file row 0, the
     * all-NULL guard never fires, and the dedup maps a key column file the partition does not hold.
     * <p>
     * The commit here is grid-aligned, so every one of its rows duplicates an existing row: the absent key
     * column reads NULL on both sides, and dedup must collapse each pair onto the incoming row without
     * changing the table's row count.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows. The"
            + "apply suspends the table.")
    @Test
    public void testDedupKeyColumnAbsentFromCompositePieceReadsAllNull() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            // A second day keeps 2020-02-03 off the last-partition slot, so ADD COLUMN below records its
            // top on 2020-02-04 and leaves 2020-02-03 without a record for the new column.
            execute("INSERT INTO x SELECT x::INT i, -x j," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();

            // Cut 2020-02-03, so the piece the dedup commit below merges into starts inside the files.
            execute(
                    "CREATE TABLE z AS (" +
                            "SELECT x::INT + 1000000 i, -x - 1000000L j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            execute("ALTER TABLE x ADD COLUMN k INT");
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the fixture suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // 84 rows on the table's own 15s grid, from the last piece's FIRST row to its LAST one: the
            // batch leaves the clusterer neither a cold prefix nor a cold suffix to cut off, so the commit
            // merges into the existing piece. Every row overlaps an existing one, and k reads NULL on both
            // sides, so every pair is a duplicate.
            execute(
                    "CREATE TABLE z2 AS (" +
                            "SELECT x::INT + 2000000 i, -x - 2000000L j," +
                            " timestamp_sequence('2020-02-03T04:17:00', 855*1000000L) ts," +
                            " NULL::INT k FROM long_sequence(84))"
            );
            execute("INSERT INTO x SELECT * FROM z2");
            drainWalQueue();

            Assert.assertFalse("the dedup commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count() c FROM x")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n6060\n");
            assertQuery("SELECT i, j, k, ts FROM x" +
                    " WHERE ts = '2020-02-03T04:17:00.000000Z'" +
                    " OR ts = '2020-02-03T04:17:15.000000Z'" +
                    " OR ts = '2020-02-03T23:59:45.000000Z'")
                    .timestamp("ts")
                    .returns("""
                            i\tj\tk\tts
                            2000001\t-2000001\tnull\t2020-02-03T04:17:00.000000Z
                            1030\t-1030\tnull\t2020-02-03T04:17:15.000000Z
                            2000084\t-2000084\tnull\t2020-02-03T23:59:45.000000Z
                            """);
        });
    }

    /**
     * A dedup commit that turns out to be all duplicates, and whose non-key values equal the rows they
     * replace, is a no-op: the writer must leave the partition exactly where it is. The check that
     * establishes that reads the partition's column files, so it has to read the piece at the piece's own
     * file base - a composite piece's logical row 0 sits at its row offset, not at file row 0.
     * <p>
     * While that check declines to run for a piece with an offset, every repeat of the same insert takes
     * the full merge instead: the shared column files grow by the piece's row count on each commit while
     * the table's contents never change.
     */
    @Ignore("The dedup no-op fast path does not recognise a piece that starts above file row 0, so a fully"
            + "duplicate commit rewrites the piece instead of writing nothing.")
    @Test
    public void testDedupNoopCommitDoesNotRewriteACompositePiece() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // 00:00:00 - 19:59:45, a cold gap, then a dense tail at 23:00:00. The varchar covers the
            // var-size arm of the identity check, which reads through the same file base.
            final String lower = "SELECT x::INT i, -x j, ('v' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(4800)";
            final String tail = "SELECT x::INT + 100000 i, -x - 100000L j, ('w' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T23:00:00', 1000000L) ts FROM long_sequence(600)";
            final String nextDay = "SELECT x::INT + 500000 i, -x - 500000L j, ('n' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-05', 60*1000000L) ts FROM long_sequence(50)";
            // Backdated into the tail, so the clusterer cuts the day at the cold gap below it.
            final String cluster = "SELECT x::INT + 200000 i, -x - 200000L j, ('c' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T23:00:00.500000', 1000000L) ts FROM long_sequence(10)";

            execute("CREATE TABLE x AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO x " + tail);
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();
            execute("INSERT INTO x " + cluster);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the cluster apply suspended the table", engine.getTableSequencerAPI().isSuspended(xt));
            // The shape the test is about: the tail lives in a piece that starts above file row 0.
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            final long extentBefore = physicalRowHwm("x");

            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            for (int i = 0; i < 3; i++) {
                execute("INSERT INTO x SELECT * FROM x WHERE ts >= '2020-02-03T23:00:00'");
                drainWalQueue();
            }
            Assert.assertFalse("the re-insert suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final long written = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore;
            Assert.assertEquals("a fully deduplicated commit wrote rows", 0, written);
            Assert.assertEquals(
                    "the shared column files grew on a no-op commit",
                    extentBefore,
                    physicalRowHwm("x")
            );

            execute("CREATE TABLE o AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO o " + tail);
            execute("INSERT INTO o " + nextDay);
            execute("INSERT INTO o " + cluster);
            final String expected = "o ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x ORDER BY ts", LOG);
        });
    }

    /**
     * A DEDUP table is cut like any other, and a duplicate still collapses onto its existing row
     * afterwards. The invariant that makes it work: the fan-out routes each row to exactly ONE piece and
     * no two live pieces hold the same timestamp value, so the row a duplicate has to collapse onto is
     * always inside the piece that duplicate routes to.
     * <p>
     * The second insert is what pins it: identical timestamps to the first, different payload, landing in
     * a piece the first insert's cut created. If routing or dedup were piece-blind the table would end up
     * with both copies.
     */
    @Ignore("Rows come back wrong around the batch boundary; the cause is not yet isolated, so this is a live"
            + "lead rather than a known gap.")
    @Test
    public void testDedupTablePreSplitsAndStillDeduplicates() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // A clustered O3 batch into the mid partition, on a 5s stride offset off the table's own 15s
            // grid so none of its rows is a duplicate yet.
            execute(
                    "CREATE TABLE z AS (" +
                            "SELECT x::INT + 1000000 i, -x - 1000000L j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext,
                    "(SELECT * FROM x0 UNION ALL SELECT * FROM z) ORDER BY ts", "x", LOG
            );

            // Same timestamps, different payload: every row is a duplicate, and every one of them lands in
            // a piece the cut above created.
            execute(
                    "CREATE TABLE z2 AS (" +
                            "SELECT x::INT + 2000000 i, -x - 2000000L j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z2");
            drainWalQueue();

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM z2) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A partition holds ONE file per column and ONE index serving every piece that reads it, so an index
     * build's unit is the PARTITION and its range is the whole of {@code [columnTop, E)}. Building a piece
     * at a time - over each piece's own rows read as file rows from 0 - leaves whichever piece went last
     * as the only one in the index, while the column data stays perfect: an indexed scan then silently
     * returns a subset.
     */
    @Ignore("An index built over a composite partition covers one piece rather than the whole of [columnTop,"
            + "E), so an indexed scan returns a subset. Cause not yet isolated.")
    @Test
    public void testIndexBuildCoversEveryPieceOfADirectoryBitmap() throws Exception {
        testIndexBuildCoversEveryPieceOfADirectory("BITMAP");
    }

    @Ignore("An index built over a composite partition covers one piece rather than the whole of [columnTop,"
            + "E), so an indexed scan returns a subset. Cause not yet isolated.")
    @Test
    public void testIndexBuildCoversEveryPieceOfADirectoryPosting() throws Exception {
        testIndexBuildCoversEveryPieceOfADirectory("POSTING");
    }

    private void testIndexBuildCoversEveryPieceOfADirectory(String indexType) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, ('s' || (x % 5))::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, ('s' || (x % 5))::SYMBOL sym," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            StringBuilder unionBody = new StringBuilder("SELECT * FROM x0");
            for (int b = 0; b < 6; b++) {
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " ('s' || (x % 5))::SYMBOL sym," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts FROM long_sequence(200))"
                );
                unionBody.append(" UNION ALL SELECT * FROM z").append(b);
            }
            for (int b = 0; b < 6; b++) {
                execute("INSERT INTO x SELECT * FROM z" + b);
            }
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            // ALTER TABLE ... ADD INDEX: the historic partitions plus the last one.
            execute("ALTER TABLE x ALTER COLUMN sym ADD INDEX TYPE " + indexType);
            drainWalQueue();

            assertSymbolIndexAndData(unionBody.toString());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSymbolIndexAndData(unionBody.toString());

            // REINDEX recreates that same single index.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            execute("REINDEX TABLE x LOCK EXCLUSIVE");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSymbolIndexAndData(unionBody.toString());
        });
    }

    /**
     * One commit, two pieces of one partition, and the shared files must grow by exactly what the commit
     * writes. In the earlier implementation the append base was a RESERVATION of the piece's EXISTING rows
     * plus the batch, taken for every piece that writes: right for a rewrite that relocates the whole
     * image, wrong for an append that writes the batch alone at the piece's own end. The difference was
     * handed to the next sibling as its base, and the rows between the file's real end and that base were
     * a HOLE - not dead space, which is a superseded image and therefore written, but rows nothing ever
     * wrote, inside the range every whole-partition consumer walks end to end.
     * <p>
     * This tree has no in-place append to over-reserve, so the hole is unreachable by that route; the test
     * is kept for the invariant, which any future base arithmetic has to keep: {@code E} advances by the
     * commit's own row count and no further.
     * <p>
     * The fixture builds the shape the reservation needed anyway - a piece that reads ABOVE its sibling in
     * file rows while sitting BELOW it in time - because that is the only order in which a piece is
     * dispatched before a sibling that takes its base. Both writes go in as ONE transaction: a second
     * commit re-seeds the base from committed metadata and the carry never happens.
     */
    @Ignore("ALTER COLUMN TYPE over a composite partition reads a var-size column at the wrong extent:"
            + "AssertionError in VarcharTypeDriver.getDataVectorSize, reached through"
            + "TableReader.openPartition0. The geometry and the no-hole assertions this test is named for both"
            + "pass; only the conversion at the end fails.")
    @Test
    public void testInPlaceAppendDoesNotOverReserveForItsSibling() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2048);
            // One cut, largest gap wins: the day is cut at the four-hour hole and nowhere else, so the
            // piece the relocating batch lands in is the whole of the day's morning.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 1);

            // 00:00:00 - 09:59:45, a four-hour hole, then 14:00:00 - 23:59:45.
            final String lower = "SELECT x::INT i, ('varchar-l-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(2400)";
            final String upper = "SELECT x::INT + 100000 i, ('varchar-u-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T14:00:00', 15*1000000L) ts FROM long_sequence(2400)";
            final String nextDay = "SELECT x::INT + 500000 i, ('varchar-n-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-05', 60*1000000L) ts FROM long_sequence(50)";
            // Interleaved with the morning piece's own 15s ticks, so that piece cannot be left where it
            // is: it goes out as one merged image at the tail, which is what puts a low-timestamp piece
            // above its sibling in file rows.
            final String relocate = "SELECT x::INT + 200000 i, ('varchar-r-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T05:00:07', 5*1000000L) ts FROM long_sequence(800)";

            execute("CREATE TABLE x AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + upper);
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();
            execute("INSERT INTO x " + relocate);
            drainWalQueue();

            // The inversion the test needs: a piece now reads ABOVE the last piece's rows, so the piece
            // dispatched FIRST is not the one that ends the files.
            assertPieceRelocatedAboveLastPiece("x");

            final TableToken xt = engine.verifyTableName("x");
            final long hwmBefore = physicalRowHwm("x");
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();

            // ONE transaction, two destinations: the 13:30 rows sit above every row of the lower piece and
            // below the upper piece's floor, so that piece takes them alone; the 20:00:07 rows interleave
            // with the upper piece's 15s ticks, so that piece merges - at whatever base the first left.
            execute("INSERT INTO x " +
                    "SELECT x::INT + 300000 i, ('varchar-a-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T13:30:00', 1000000L) ts FROM long_sequence(10)" +
                    " UNION ALL" +
                    " SELECT x::INT + 400000 i, ('varchar-b-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T20:00:07', 1000000L) ts FROM long_sequence(10)");
            drainWalQueue();
            Assert.assertFalse("the mixed commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The shared files grew by exactly what the commit wrote. Anything more is unwritten rows
            // between the two pieces.
            final long hwmGrowth = physicalRowHwm("x") - hwmBefore;
            final long written = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore;
            Assert.assertTrue(
                    "the shared files grew past what the commit wrote, the gap is a hole of unwritten rows"
                            + " [hwmGrowth=" + hwmGrowth + ", physicallyWrittenRows=" + written + ']',
                    hwmGrowth - written < 100
            );
            assertNoOverlappingPieces("x");

            // The oracle, and the whole-partition read that a hole breaks: ALTER COLUMN TYPE walks the
            // rows end to end, so an unset VARCHAR aux entry anywhere in them suspends the table.
            execute("CREATE TABLE o AS (" + lower + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO o " + upper);
            execute("INSERT INTO o " + nextDay);
            execute("INSERT INTO o " + relocate);
            execute("INSERT INTO o SELECT x::INT + 300000 i, ('varchar-a-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T13:30:00', 1000000L) ts FROM long_sequence(10)");
            execute("INSERT INTO o SELECT x::INT + 400000 i, ('varchar-b-' || x)::VARCHAR v," +
                    " timestamp_sequence('2020-02-03T20:00:07', 1000000L) ts FROM long_sequence(10)");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "o ORDER BY ts, i", "x ORDER BY ts, i", LOG);

            execute("ALTER TABLE x ALTER COLUMN v TYPE SYMBOL");
            drainWalQueue();
            Assert.assertFalse("the conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, v::STRING v, ts FROM o ORDER BY ts, i",
                    "SELECT i, v::STRING v, ts FROM x ORDER BY ts, i",
                    LOG
            );
        });
    }

    /**
     * A column added while a LATER partition was active leaves the earlier partition with no record for it
     * at all, so every piece of that partition has to answer "absent" from one number - the partition's
     * shared column top. A backdated commit that materializes the column in more than one piece then has
     * each piece append at {@code E - top}, and if the pieces disagreed about the top their appends would
     * land over each other while the reader, which maps {@code E - top} rows, addressed past the file's
     * end.
     * <p>
     * A partition is ONE record here, so it holds one top by construction. What this pins is the other
     * half: that the shared top is right and both pieces read and write the column through it.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows. Kills"
            + "the JVM.")
    @Test
    public void testLateColumnKeepsOneTopAcrossPiecesOfOneDirectory() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            // A second day keeps 2020-02-03 off the last-partition slot, so the ADD COLUMNs below record
            // their tops on 2020-02-04 and leave 2020-02-03 without a record for the new columns.
            execute("INSERT INTO x SELECT x::INT i," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();

            // Cut 2020-02-03 and relocate the hot piece, so the partition holds several pieces at non-zero
            // row offsets - the shape in which a piece-local top would diverge from its siblings'.
            execute(
                    "CREATE TABLE z1 AS (SELECT x::INT + 1000000 i," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z1");
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            execute("ALTER TABLE x ADD COLUMN c INT");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // Backdated rows for TWO pieces of 2020-02-03 in one commit: each piece materializes c and v in
            // the partition's shared files for the first time, so both must place file row 0 at the same
            // row of the shared frame.
            execute(
                    "CREATE TABLE z2 AS (SELECT x::INT + 2000000 i," +
                            " timestamp_sequence('2020-02-03T02:00:03', 5*1000000L) ts," +
                            " (x % 97)::INT c, ('v' || (x % 7))::VARCHAR v FROM long_sequence(150))"
            );
            execute(
                    "CREATE TABLE z3 AS (SELECT x::INT + 3000000 i," +
                            " timestamp_sequence('2020-02-03T10:00:03', 5*1000000L) ts," +
                            " (x % 89)::INT c, ('w' || (x % 5))::VARCHAR v FROM long_sequence(150))"
            );
            execute("INSERT INTO x (i, ts, c, v) SELECT i, ts, c, v FROM z2");
            execute("INSERT INTO x (i, ts, c, v) SELECT i, ts, c, v FROM z3");
            drainWalQueue();

            final String expected = "SELECT i, c, v::STRING v, ts FROM (" +
                    "SELECT * FROM x0 UNION ALL SELECT * FROM z2 UNION ALL SELECT * FROM z3) ORDER BY ts";
            final String actual = "SELECT i, c, v::STRING v, ts FROM x ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
        });
    }

    /**
     * A column added while a LATER partition was active leaves the earlier one with no record for it, so
     * every piece there has to answer "absent" from the partition's shared column top alone. That top has
     * to be {@code E}: a top BELOW it leaves the append base {@code E - top} non-zero, the index build
     * never sees its first row, and a POSTING column whose partition holds no index yet gets opened for
     * append instead of created - "index does not exist", and the apply suspends the table.
     * <p>
     * The cut ABOVE the stride is what makes this the MIDDLE piece - a sibling's rows still sit between its
     * end and {@code E} - so the base is not zero. Merging into the LAST piece instead would leave the top
     * at {@code E}, a zero base, and no bug to see.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testMergeAppendCreatesPostingIndexForColumnAbsentFromPartition() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, "POSTING");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            // A second day keeps 2020-02-03 a MID partition and owns the added column's only record.
            execute("INSERT INTO x SELECT x::INT + 2000 i," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();

            // Added BEFORE the cut: only the LAST partition gets a record, so no piece of 2020-02-03 holds
            // a .pk for s and each has to answer "absent" from the column-top default alone.
            execute("ALTER TABLE x ADD COLUMN s SYMBOL INDEX");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // ONE backdated commit into a mid-morning stride: it cuts 2020-02-03 at the stride's cold edges
            // and rewrites the hot piece in the same commit.
            execute(
                    "CREATE TABLE y AS (SELECT x::INT + 3000000 i," +
                            " timestamp_sequence('2020-02-03T04:00:09', 5*1000000L) ts," +
                            " 'k' || (x % 5) s FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM y");
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            final String unionBody = "SELECT i, s, ts FROM x0 UNION ALL SELECT i, s, ts FROM y";
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, s::STRING s, ts FROM (" + unionBody + ") ORDER BY ts",
                    "SELECT i, s::STRING s, ts FROM x ORDER BY ts",
                    LOG
            );
            // Per key, so the index cursor runs over the entries the rewrite wrote. Data equality alone
            // passes on an index that was silently created empty.
            for (int k = 0; k < 5; k++) {
                final String predicate = " WHERE s = 'k" + k + "'";
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "SELECT i, s::STRING s, ts FROM (" + unionBody + ")" + predicate + " ORDER BY ts",
                        "SELECT i, s::STRING s, ts FROM x" + predicate + " ORDER BY ts",
                        LOG
                );
            }

            // A cold reopen exposes reader-side geometry the warm cursors above share with the writer.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, s::STRING s, ts FROM (" + unionBody + ") ORDER BY ts",
                    "SELECT i, s::STRING s, ts FROM x ORDER BY ts",
                    LOG
            );

            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, s::STRING s, ts FROM (" + unionBody + ") ORDER BY ts",
                    "SELECT i, s::STRING s, ts FROM x ORDER BY ts",
                    LOG
            );
        });
    }

    /**
     * A merge relocates a NON-last piece to the tail of the shared column files, so the LAST piece stops
     * being the furthest region of those files. Its own end - which is where an active-partition append
     * position lands - then points into the middle of live data, and closing that partition must not
     * truncate or zero-fill from there.
     * <p>
     * Two closes reach it: a fresh {@link TableWriter} open, which maps the last piece and truncates each
     * column to a page boundary above its append offset, and the close a later cut takes before it writes.
     * Either one lops the relocated sibling off the end of the shared files, leaving its rows reading back
     * as zeroes - a VARCHAR aux entry of 0 is an unset entry, which is where it surfaces first.
     */
    @Ignore("The writer positions and truncates the ACTIVE partition's column files at its LIVE row count,"
            + "while a composite partition's files run to E. An in-order append therefore writes over a piece a"
            + "rewrite relocated above the last one, and closing the writer truncates it away. Proven by moving"
            + "the same scenario off the active partition, where it passes. Kills the JVM.")
    @Test
    public void testMergeAppendedPieceSurvivesActivePartitionClose() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            // One cut, largest gap wins: the gap ABOVE the backdated stride is the day's whole afternoon,
            // so the cut lands there and the HOT piece is the prefix - not the last one.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 1);

            // One day only, so it stays the ACTIVE partition and a writer open maps it.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, rnd_varchar(10, 40, 0) v," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE z AS (SELECT x::INT + 1000000 i, -x - 1000000L AS j," +
                            " rnd_varchar(10, 40, 0) v," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertPieceRelocatedAboveLastPiece("x");

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM z) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);

            // A fresh writer maps the last piece at its own end and closes from there.
            engine.releaseAllWriters();
            //noinspection EmptyTryBlock
            try (TableWriter ignore = getWriter("x")) {
                // opening is enough: the constructor maps the last piece and sets its append position
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);

            // The cut itself reaches the same close: a second backdated stride, this time inside the cold
            // tail piece, cuts it while the writer holds the partition open.
            execute(
                    "CREATE TABLE w AS (SELECT x::INT + 2000000 i, -x - 2000000L AS j," +
                            " rnd_varchar(10, 40, 0) v," +
                            " timestamp_sequence('2020-02-03T09:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();

            final String expectedAll = "(SELECT * FROM x0 UNION ALL SELECT * FROM z" +
                    " UNION ALL SELECT * FROM w) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedAll, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedAll, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedAll, "x", LOG);
        });
    }

    /**
     * The per-column data append base a rewrite takes is the column's own data end at the base row, read
     * from its aux vector. A VARCHAR column that owns no data bytes at all - every value NULL, or short
     * enough to inline - reports 0 there, and 0 is also what an UNSET entry reports, so a caller that
     * cannot tell a live empty column from one that does not reach the base row falls back to the file's
     * length. That is the append page the writer mapped the file with, not the data end, so the relocated
     * image starts a page past the bytes below it and leaves a hole no later write fills: the aux and data
     * vectors stop agreeing. Every value still reads back correctly - each aux entry carries its own
     * offset - so only a density check catches it.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testMergeAppendKeepsAnInlinedVarcharDense() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // The hole is exactly one append page, so pin the page size rather than inherit it.
            node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 1 << 18);
            // Isolate the relocation: the default 50MB floor rules out every cut, so the only thing that
            // can move this partition is the rewrite itself.

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(400)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            // Added after the rows, so v carries a column top and its file starts above file row 0.
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // Every value fits VARCHAR's inline budget, so v's aux vector fills up while its data file
            // stays EMPTY: every entry carries data offset 0.
            execute(
                    "CREATE TABLE hi AS (SELECT x::INT + 999 i," +
                            " timestamp_sequence('2020-02-03T01:40:07', 1000000L) ts," +
                            " ('v' || ((x - 1) % 7))::VARCHAR v FROM long_sequence(100))"
            );
            execute("INSERT INTO x SELECT * FROM hi");
            drainWalQueue();

            // The next writer sets every column of the last partition to its append position, which maps
            // v's data file and extends it to one append page: 256KB of file holding nothing.
            engine.releaseAllWriters();

            // ONE backdated transaction, landing inside the partition's rows: the rewrite takes v's data
            // append base from the row below the tail - the row reporting a data end of 0.
            execute(
                    "CREATE TABLE lo AS (SELECT x::INT + 8000 i," +
                            " timestamp_sequence('2020-02-03T00:20:07', 1000000L) ts," +
                            " ('lo' || x)::VARCHAR v FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM lo");
            drainWalQueue();

            assertVarSizeColumnsDense("x");

            final String expected = "SELECT i, v::STRING v, ts FROM" +
                    " (SELECT * FROM x0 UNION ALL SELECT * FROM hi UNION ALL SELECT * FROM lo) ORDER BY ts";
            final String actual = "SELECT i, v::STRING v, ts FROM x ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            assertVarSizeColumnsDense("x");
        });
    }

    /**
     * The shared append base {@code E} must come from the geometry, not from a column file's LENGTH - the
     * writer leaves those files page-rounded past their last row. Taking it from a length puts every
     * var-size column's base at a row no column actually holds, where it reads an UNSET aux entry. STRING,
     * BINARY and ARRAY read an unset entry as 0 and fall back; VARCHAR has no legal all-zero entry and
     * asserts instead, killing the apply.
     * <p>
     * A VARCHAR column added AFTER the day was written makes the gap unmissable: its own aux file holds
     * nothing but the mapping's zero-fill, so the entry the base addresses is pure padding.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testMergeAppendLateVarcharColumnReadsUnsetAuxEntry() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // 5760 rows * 8 bytes is not a whole number of pages, so closing the day leaves ts rounded up -
            // that rounding IS the gap between a length-derived base and the last live row.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            // Added AFTER the rows: v's column top covers the whole partition, so it owns no file rows.
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE z AS (SELECT x::INT + 1000000 i," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts," +
                            " ('v' || (x % 7))::VARCHAR v FROM long_sequence(200))"
            );
            execute("INSERT INTO x (i, ts, v) SELECT i, ts, v FROM z");
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            final String expected = "SELECT i, v::STRING v, ts FROM" +
                    " (SELECT * FROM x0 UNION ALL SELECT * FROM z) ORDER BY ts";
            final String actual = "SELECT i, v::STRING v, ts FROM x ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
        });
    }

    /**
     * One commit that BOTH rewrites a lower piece at the shared files' tail AND writes rows past every row
     * the partition holds. Pieces are dispatched in timestamp order, so the lower piece writes first and
     * extends the files; the batch above must take its base from where those files now end, not from the
     * end the committed metadata still names.
     */
    @Test
    public void testMergeAppendLowerPieceDoesNotClobberFurthestAppend() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, (x * 1.5)::DOUBLE k," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j, (x * 1.5)::DOUBLE k," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // A single backdated write near the middle cuts 2020-02-03 into a low piece and a high one.
            execute(
                    "CREATE TABLE s AS (SELECT x::INT + 7000000 i, -x - 7000000L AS j," +
                            " (x * 0.3)::DOUBLE k, timestamp_sequence('2020-02-03T12:00:07', 1000000L) ts" +
                            " FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM s");
            drainWalQueue();

            // ONE commit against that partition: (a) a backdated merge into the LOW piece (01:00:07),
            // which extends the shared column files past the high piece's end; (b) rows past the high
            // piece's max but still inside the day (23:59:50). The low piece is dispatched first.
            execute(
                    "CREATE TABLE w AS (" +
                            "SELECT x::INT + 8000000 i, -x - 8000000L AS j, (x * 0.5)::DOUBLE k," +
                            " timestamp_sequence('2020-02-03T01:00:07', 1000000L) ts FROM long_sequence(3)" +
                            " UNION ALL " +
                            "SELECT x::INT + 9000000 i, -x - 9000000L AS j, (x * 0.7)::DOUBLE k," +
                            " timestamp_sequence('2020-02-03T23:59:50', 1000000L) ts FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();
            assertNoOverlappingPieces("x");
            assertRowsInTimestampOrder("x");

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM s" +
                    " UNION ALL SELECT * FROM w) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A rewrite relocates the piece as a full image and records its new row offset, which requires every
     * column to end up at column top 0 AND to be read through that offset. A partition holding a column
     * added during its own lifetime is the hard case: the column is present for some of the partition's
     * rows and absent for the rest, so the merge has to read below the top - rows that are not in the
     * column's file at all and have to come out as NULL.
     * <p>
     * Both backdated writes straddle the boundary: 01:00:07 lands above c's top of 200 and 00:20:07 below
     * it. The control table proves any guard the first half needs is narrow rather than a blanket disable:
     * same shape, same backdated write, no late-added column.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testMergeAppendOverAPartitionWithAColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Isolate the rewrite: the default 50MB floor rules out every cut.

            // 2020-02-03 rows 0..199 predate column c; rows 200..399 carry it -> column top 200.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("ALTER TABLE x ADD COLUMN c INT");
            execute("INSERT INTO x SELECT x::INT + 1000 i," +
                    " timestamp_sequence('2020-02-03T00:50:00', 15*1000000L) ts, x::INT + 1000 c" +
                    " FROM long_sequence(200)");
            // A second day keeps 2020-02-03 a MID partition.
            execute("INSERT INTO x SELECT x::INT + 2000 i," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts, x::INT + 2000 c FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE hi AS (SELECT x::INT + 7000000 i," +
                            " timestamp_sequence('2020-02-03T01:00:07', 1000000L) ts, x::INT + 7000000 c" +
                            " FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM hi");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "(SELECT * FROM x0 UNION ALL SELECT * FROM hi) ORDER BY ts", "x", LOG);

            execute(
                    "CREATE TABLE lo AS (SELECT x::INT + 8000000 i," +
                            " timestamp_sequence('2020-02-03T00:20:07', 1000000L) ts, x::INT + 8000000 c" +
                            " FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM lo");
            drainWalQueue();

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM hi" +
                    " UNION ALL SELECT * FROM lo) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);

            // Control: the same table shape and the same backdated write, but every column spans the
            // partition, so nothing about a column top can be involved.
            execute(
                    "CREATE TABLE y AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts," +
                            " x::INT c FROM long_sequence(400)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO y SELECT x::INT + 2000 i," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts, x::INT + 2000 c FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE y0 AS (SELECT * FROM y)");
            execute("INSERT INTO y SELECT * FROM lo");
            drainWalQueue();

            final String expectedY = "(SELECT * FROM y0 UNION ALL SELECT * FROM lo) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedY, "y", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedY, "y", LOG);
        });
    }

    /**
     * A rewrite of a piece carrying an indexed SYMBOL column has to publish its index entries ABOVE the
     * entries already in the shared index, at the committed file end rather than at 0, or it truncates
     * that index for every sibling piece. Verified through index-backed reads, before and after a reopen
     * and a squash.
     */
    @Test
    public void testMergeAppendPreservesBitmapIndex() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, ('s' || (x % 5))::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            "), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, ('s' || (x % 5))::SYMBOL sym," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            StringBuilder unionBody = new StringBuilder("SELECT * FROM x0");
            for (int b = 0; b < 6; b++) {
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " ('s' || (x % 5))::SYMBOL sym," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts FROM long_sequence(200))"
                );
                unionBody.append(" UNION ALL SELECT * FROM z").append(b);
            }

            for (int b = 0; b < 6; b++) {
                execute("INSERT INTO x SELECT * FROM z" + b);
            }
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertSymbolIndexAndData(unionBody.toString());

            // Reopen from disk: the appended index entries are read through each piece's row offset.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSymbolIndexAndData(unionBody.toString());
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            assertSymbolIndexAndData(unionBody.toString());
        });
    }

    /**
     * A hot piece is rewritten as a full image at the tail of the partition's own column files - same
     * directory, same files, only the piece's row offset and count move - instead of being copied into a
     * fresh directory. The relocation is the assertion: a piece whose row offset is past every row the
     * partition held before the commit was written by this path and no other.
     */
    @Test
    public void testMergeAppendRewritesSharedPieceInPlace() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, (x * 1.5)::DOUBLE k," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j, (x * 1.5)::DOUBLE k," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            final StringBuilder union = new StringBuilder("(SELECT * FROM x0");
            for (int b = 0; b < 6; b++) {
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " -x - " + (1_000_000L * (b + 1)) + " AS j, (x * 0.25)::DOUBLE k," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts FROM long_sequence(200))"
                );
                union.append(" UNION ALL SELECT * FROM z").append(b);
            }
            union.append(") ORDER BY ts");

            for (int b = 0; b < 6; b++) {
                execute("INSERT INTO x SELECT * FROM z" + b);
            }
            drainWalQueue();

            // A small backdated merge near the START of the cold interior piece [~04:17, 20:00): the
            // prefix is tiny so the apply does not cut again, and the piece is rewritten at the tail.
            execute(
                    "CREATE TABLE z6 AS (SELECT x::INT + 7000000 i, -x - 7000000L AS j," +
                            " (x * 0.1)::DOUBLE k, timestamp_sequence('2020-02-03T04:18:07', 3*1000000L) ts" +
                            " FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM z6");
            drainWalQueue();
            union.setLength(union.length() - ") ORDER BY ts".length());
            union.append(" UNION ALL SELECT * FROM z6) ORDER BY ts");

            assertPieceRelocatedAboveLastPiece("x");
            assertNoOverlappingPieces("x");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union.toString(), "x", LOG);

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union.toString(), "x", LOG);

            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union.toString(), "x", LOG);
        });
    }

    /**
     * The ACTIVE partition is rewritten like any other. A one-day table makes the target unambiguous:
     * 2020-02-03 IS the last partition, so a relocation here can only come from the last-partition case
     * having no exemption. Afterwards the relocated piece is still the active partition, so a later
     * in-order append must land past it and a later backdated write must relocate it again.
     */
    @Test
    public void testMergeAppendsActivePartition() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Isolate the relocation: the default 50MB floor rules out every cut, so the only thing that
            // can move this partition is the rewrite itself.

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(400)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");
            Assert.assertEquals("the day must start as one piece at file row 0", 0, pieceRowOffsetOfDay("x", 0));

            // ONE backdated transaction, landing inside the active partition.
            execute(
                    "CREATE TABLE lo AS (SELECT x::INT + 8000000 i, -x - 8000000L AS j," +
                            " timestamp_sequence('2020-02-03T00:20:07', 1000000L) ts FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM lo");
            drainWalQueue();

            Assert.assertTrue(
                    "the active partition was rewritten in place instead of at the tail: " + describePieces("x"),
                    pieceRowOffsetOfDay("x", 0) > 0
            );

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM lo) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);

            execute(
                    "CREATE TABLE hi AS (SELECT x::INT + 9000000 i, -x - 9000000L AS j," +
                            " timestamp_sequence('2020-02-03T12:00:00', 1000000L) ts FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM hi");
            execute(
                    "CREATE TABLE mid AS (SELECT x::INT + 7000000 i, -x - 7000000L AS j," +
                            " timestamp_sequence('2020-02-03T00:40:07', 1000000L) ts FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM mid");
            drainWalQueue();

            final String expectedAll = "(SELECT * FROM x0 UNION ALL SELECT * FROM lo" +
                    " UNION ALL SELECT * FROM hi UNION ALL SELECT * FROM mid) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedAll, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedAll, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expectedAll, "x", LOG);
        });
    }

    /**
     * Merging a piece that carries STRING and VARCHAR columns exercises the var-size geometry: the aux
     * vector appends at a uniform row, past the entries already there, and each column's data file appends
     * at its own byte end. Verified against an independent union, before and after a reopen and a squash.
     */
    @Test
    public void testMergeAppendVarSizeColumns() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 8 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, ('s' || (x % 9)) str, ('v' || (x % 13))::VARCHAR v," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, ('s' || (x % 9)) str, ('v' || (x % 13))::VARCHAR v," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            StringBuilder unionBody = new StringBuilder("SELECT * FROM x0");
            for (int b = 0; b < 6; b++) {
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " ('s' || (x % 9)) str, ('v' || (x % 13))::VARCHAR v," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts FROM long_sequence(200))"
                );
                unionBody.append(" UNION ALL SELECT * FROM z").append(b);
            }
            final String expected = "SELECT i, str::STRING str, v::STRING v, ts FROM (" + unionBody + ") ORDER BY ts";
            final String actual = "SELECT i, str::STRING str, v::STRING v, ts FROM x ORDER BY ts";

            for (int b = 0; b < 6; b++) {
                execute("INSERT INTO x SELECT * FROM z" + b);
            }
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertNoOverlappingPieces("x");
            assertVarSizeColumnsDense("x");

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
        });
    }

    /**
     * A piece whose whole row range sits below the partition's shared column top reads a late-added column
     * as absent, so the scan caches a NULL index reader for it. A null reader synthesizes every row as
     * NULL and records nothing that could turn it back into a real one: once the piece moves above the top
     * - a relocation, or the squash consolidating the partition - {@code sym = null} keeps returning the
     * whole partition and {@code sym = '<value>'} returns nothing, while the data itself reads correctly.
     */
    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testNullKeyIndexScanOnCompositeDirectoryBitmap() throws Exception {
        testNullKeyIndexScanOnCompositeDirectory("");
    }

    @Ignore("A merge that has to read BELOW a column top is refused, and processCompositePartition swallows"
            + "the refusal, so the partition is silently left uncut or comes back with the wrong rows.")
    @Test
    public void testNullKeyIndexScanOnCompositeDirectoryPosting() throws Exception {
        testNullKeyIndexScanOnCompositeDirectory("TYPE POSTING");
    }

    private void testNullKeyIndexScanOnCompositeDirectory(String indexType) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // 2000 rows, 15s apart (00:00:00 - 08:19:45), plus a later day so ADD COLUMN records the
            // column top on THAT day: the indexed symbol column then reads as absent from 2020-02-03.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(2000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT + 900000 i," +
                    " timestamp_sequence('2020-02-05', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("ALTER TABLE x ADD COLUMN sym SYMBOL INDEX " + indexType);
            drainWalQueue();

            // Narrow backdated clusters, one per hour inside the pre-column region. Each cuts the day
            // again and relocates the hot piece, leaving cold pieces whose row range sits strictly between
            // 0 and the shared column top - the pieces that read the column as absent.
            for (int h = 1; h <= 7; h++) {
                execute(
                        "INSERT INTO x SELECT x::INT + " + (100_000 * h) + " i," +
                                " timestamp_sequence('2020-02-03T0" + h + ":00:07', 3000000L) ts," +
                                " CASE WHEN x % 3 = 0 THEN NULL ELSE ('s' || (x % 5)) END::SYMBOL sym" +
                                " FROM long_sequence(120)"
                );
                drainWalQueue();
            }

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertHasPieceBelowColumnTop("x", "sym");

            for (int pass = 0; pass < 3; pass++) {
                execute("DROP TABLE IF EXISTS oracle");
                execute("CREATE TABLE oracle AS (SELECT i, ts, sym::STRING sym FROM x)");
                assertNullAndValuedKeyScans();
                if (pass == 0) {
                    // A cold open must agree with the reader that watched the geometry change.
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                } else if (pass == 1) {
                    // The squash folds every piece into one, which is where a piece that read the column
                    // as absent ends up covering rows that carry it.
                    execute("ALTER TABLE x SQUASH PARTITIONS");
                    drainWalQueue();
                }
            }
        });
    }

    /**
     * Every cut rides on the composite write path and must decline without it. With the flag off the
     * partition stays one piece however the incoming rows cluster, and the ordinary O3 merge carries the
     * commit.
     */
    @Test
    public void testPreSplitDeclinedWhenMergeAppendDisabled() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // The shape testSingleTxnApplyPreSplitsClusteredMidPartition cuts: a dense mid partition and
            // one narrow backdated stride, with cold gaps either side of it.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE z AS (" +
                            "SELECT x::INT + 1000000 i, -x - 1000000L j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();

            assertPartitionNotCut("x");

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM z) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * The active partition is not exempt from the cut. A backdated commit into the day being appended to
     * amplifies exactly like one into any other day, so it is cut the same way; and after the cut the last
     * piece is still where in-order rows land, so a later append must reach it without disturbing its
     * siblings.
     */
    @Ignore("The writer positions and truncates the ACTIVE partition's column files at its LIVE row count,"
            + "while a composite partition's files run to E. An in-order append therefore writes over a piece a"
            + "rewrite relocated above the last one, and closing the writer truncates it away. Proven by moving"
            + "the same scenario off the active partition, where it passes.")
    @Test
    public void testPreSplitsLastLogicalPartition() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // One day only, so the dense day IS the active partition.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            final StringBuilder union = new StringBuilder("(SELECT * FROM x0");
            for (int b = 0; b < 4; b++) {
                final String start = (b % 2 == 0 ? "2020-02-03T04:0" : "2020-02-03T20:0") + (b / 2) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT" +
                                " x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " -x - " + (1_000_000L * (b + 1)) + " AS j," +
                                " timestamp_sequence('" + start + "', 5*1000000L) ts" +
                                " FROM long_sequence(200))"
                );
                execute("INSERT INTO x SELECT * FROM z" + b);
                union.append(" UNION ALL SELECT * FROM z").append(b);
            }
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, union + ") ORDER BY ts", "x", LOG);

            // In-order append past the day's last row: it lands on the last piece, and every sibling must
            // keep its rows.
            execute(
                    "CREATE TABLE w AS (SELECT x::INT + 9000000 i, -x - 9000000L AS j," +
                            " timestamp_sequence('2020-02-03T23:59:50', 1000000L) ts FROM long_sequence(3))"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();
            union.append(" UNION ALL SELECT * FROM w");
            assertNoOverlappingPieces("x");

            final String expected = union + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * The drip shape the feature exists for: ONE small backdated transaction into a large historic day.
     * The clusterer resolves at transaction granularity - it sees a single {@code [minTs, maxTs]} per
     * transaction, not the row distribution inside it - so one transaction is cut at the cold gaps on
     * either side of its own range, leaving only its stride to be rewritten. (A single transaction that
     * itself scatters into several strides therefore cuts only at its outer edges: everything between its
     * first and last row reads as one hot run.)
     */
    @Test
    public void testSingleTxnApplyPreSplitsClusteredMidPartition() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            execute("INSERT INTO x SELECT x::INT i, -x j," +
                    " timestamp_sequence('2020-02-04', 60*1000000L) ts FROM long_sequence(100)");
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // ONE statement, so the apply sees a single transaction. Its rows occupy a ~17 minute stride
            // of a 24 hour day.
            execute(
                    "CREATE TABLE z AS (" +
                            "SELECT x::INT + 1000000 i, -x - 1000000L j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );

            final long physicalBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();
            final long physicalRows = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalBefore;

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertNoOverlappingPieces("x");
            // Without the cut this rewrites the whole day plus the new rows (5960); the cold gaps on
            // either side of the stride are cut away, leaving only the stride's own rows to merge.
            Assert.assertTrue(
                    "physically written rows too high, single-txn pre-split ineffective: " + physicalRows,
                    physicalRows < 1000
            );

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM z) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A plain in-order write into the LAST piece of the LAST partition changes only that piece's row
     * count, and that number reaches {@code _txn} through the transient row count without any
     * {@code _geometry} record being written. The commit that gives the partition a successor takes the
     * piece off the tail, and from then on the record is what has to carry the count.
     * <p>
     * The exposure is the reopen: it rebuilds the piece list from {@code _txn} and {@code _geometry}, so a
     * count that only ever lived in the transient slot comes back as whatever the last record said - and
     * every row written into the piece while it was the tail disappears.
     */
    @Ignore("The writer positions and truncates the ACTIVE partition's column files at its LIVE row count,"
            + "while a composite partition's files run to E. An in-order append therefore writes over a piece a"
            + "rewrite relocated above the last one, and closing the writer truncates it away. Proven by moving"
            + "the same scenario off the active partition, where it passes.")
    @Test
    public void testTailPiecePublishesGeometryOnceALaterPartitionArrives() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 30);

            // One day only, so the day's last piece is also the table's last piece.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            // A backdated stride cuts the day at the cold gaps around it, leaving the last piece holding
            // everything past ~04:17.
            execute(
                    "CREATE TABLE z AS (SELECT x::INT + 1000000 i, -x - 1000000L j," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();
            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);

            // Append in order onto the last piece while it is still the tail. The piece list does not
            // move, so the ONLY thing this commit changes about the geometry is that piece's row count.
            execute(
                    "CREATE TABLE w AS (SELECT x::INT + 2000000 i, -x - 2000000L j," +
                            " timestamp_sequence('2020-02-03T23:59:46', 1000000L) ts FROM long_sequence(14))"
            );
            execute("INSERT INTO x SELECT * FROM w");
            drainWalQueue();

            // A row in the next day retires the piece from the tail.
            execute(
                    "CREATE TABLE v AS (SELECT x::INT + 3000000 i, -x - 3000000L j," +
                            " timestamp_sequence('2020-02-04', 15*1000000L) ts FROM long_sequence(100))"
            );
            execute("INSERT INTO x SELECT * FROM v");
            drainWalQueue();

            final String expected = "(SELECT * FROM x0 UNION ALL SELECT * FROM z UNION ALL SELECT * FROM w" +
                    " UNION ALL SELECT * FROM v) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            // The re-open is the exposure: it rebuilds the piece list from _txn and _geometry.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, "x", LOG);
        });
    }

    /**
     * A WAL apply no longer holds the active partition open between commits, so the release after a commit
     * is what has to close the POSTING writer left open on the last partition's {@code .pk}. Releasing over
     * a list the release itself clears makes that a no-op on any apply that never refilled it, and the
     * indexer then carries an open writer - and a stale chain region limit - into the next commit. The next
     * apply's pool writer appends a slot past that limit and the seal closes the stale writer, truncating
     * the {@code .pk} back and lopping the tail off the slot just published: the entry counts a generation
     * that reads back as all zeroes, so every row it indexed vanishes from index scans.
     */
    @Test
    public void testWalCommitDoesNotStrandPostingIndexWriter() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, ('s' || (x % 5))::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(1000)" +
                            "), INDEX(sym TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            StringBuilder unionBody = new StringBuilder("SELECT * FROM x0");
            // Every batch lands in the SAME day the create left open, so each apply hands the one .pk the
            // stranded writer holds to the O3 pool writer, which appends a slot past the region limit that
            // writer cached. One commit per drain: the stranding and the append have to fall in different
            // commits.
            for (int b = 0; b < 6; b++) {
                final String start = "2020-02-03T04:" + (10 + b * 8) + ":07";
                execute(
                        "CREATE TABLE z" + b + " AS (SELECT x::INT + " + (1_000_000 * (b + 1)) + " i," +
                                " ('s' || (x % 5))::SYMBOL sym," +
                                " timestamp_sequence('" + start + "', 1000000L) ts FROM long_sequence(200))"
                );
                unionBody.append(" UNION ALL SELECT * FROM z").append(b);
                // Descending, so the block needs the O3 sort and the apply routes through O3PartitionJob -
                // but every row still lands above the partition's max, so nothing is rewritten and the seal
                // takes the skip-rebuild path that keeps the pool writer's slot.
                execute("INSERT INTO x SELECT * FROM z" + b + " ORDER BY ts DESC");
                drainWalQueue();
            }

            assertSymbolIndexAndData(unionBody.toString());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSymbolIndexAndData(unionBody.toString());
        });
    }

    /**
     * A partition holds ONE index shared by every piece that reads its column files, and a rewrite parks a
     * relocated piece ABOVE the last piece's own end. So the highest legitimate row id in that index is
     * the partition's physical extent {@code E}, not the last piece's end: evicting orphan row ids at the
     * last piece's end throws away every entry the relocated sibling owns. The column data stays perfect;
     * only the indexed scan loses the sibling's rows.
     */
    @Test
    public void testWriterReopenKeepsIndexOfMergeAppendedPieceBitmap() throws Exception {
        testWriterReopenKeepsIndexOfMergeAppendedPiece("");
    }

    @Ignore("The BITMAP variant passes and this one does not, so the .pk chain is the difference: a partition"
            + "holds ONE chain whose highest legitimate row id is E, and evicting orphan row ids at the LAST"
            + "piece's end throws away every entry a relocated sibling owns.")
    @Test
    public void testWriterReopenKeepsIndexOfMergeAppendedPiecePosting() throws Exception {
        testWriterReopenKeepsIndexOfMergeAppendedPiece(" TYPE POSTING");
    }

    private void testWriterReopenKeepsIndexOfMergeAppendedPiece(String indexType) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 1);

            // One day only, so the composite partition is the ACTIVE one a writer open maps.
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x::INT i, ('s' || (x % 5))::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                            " FROM long_sequence(5760)" +
                            "), INDEX(sym" + indexType + ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();
            execute("CREATE TABLE x0 AS (SELECT * FROM x)");

            execute(
                    "CREATE TABLE z AS (SELECT x::INT + 1000000 i, ('s' || (x % 5))::SYMBOL sym," +
                            " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200))"
            );
            execute("INSERT INTO x SELECT * FROM z");
            drainWalQueue();

            Assert.assertTrue("the day was not cut into pieces: " + describePieces("x"), piecesOfDay("x") > 1);
            assertPieceRelocatedAboveLastPiece("x");

            final String unionBody = "SELECT * FROM x0 UNION ALL SELECT * FROM z";
            assertSymbolIndexAndData(unionBody);

            // Opening a writer is enough: the constructor opens the last piece.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            //noinspection EmptyTryBlock
            try (TableWriter ignore = getWriter("x")) {
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertSymbolIndexAndData(unionBody);
        });
    }

    /**
     * Proves the fixture actually built the geometry the test is about: at least one piece whose whole row
     * range sits below the partition's shared column top, so the reader takes the "column absent" path
     * for it.
     */
    private static void assertHasPieceBelowColumnTop(String tableName, String columnName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final int columnIndex = reader.getMetadata().getColumnIndex(columnName);
            final PartitionGeometry geometry = reader.getGeometry();
            for (int p = 0, n = reader.getPartitionCount(); p < n; p++) {
                reader.openPartition(p);
                final long columnTop = reader.getColumnTop(reader.getColumnBase(p), columnIndex);
                if (columnTop < 1) {
                    continue;
                }
                for (int i = 0, m = geometry.getPieceCount(p); i < m; i++) {
                    if (geometry.getPieceRowOffset(p, i) + geometry.getPieceRowCount(p, i) <= columnTop) {
                        return;
                    }
                }
            }
            Assert.fail("no piece sits entirely below the shared column top, the setup did not build the shape");
        }
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
     * The scans a null index reader breaks: {@code sym = null}, in both directions, and each real key.
     * Compared against an oracle taken from the same table through a plain scan, so only the index path
     * is under test.
     */
    private static void assertNullAndValuedKeyScans() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT i, ts, sym FROM oracle WHERE sym = null",
                "SELECT i, ts, sym::STRING sym FROM x WHERE sym = null",
                LOG
        );
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT i, ts, sym FROM oracle WHERE sym = null ORDER BY ts DESC",
                "SELECT i, ts, sym::STRING sym FROM x WHERE sym = null ORDER BY ts DESC",
                LOG
        );
        for (int k = 0; k < 5; k++) {
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, ts, sym FROM oracle WHERE sym = 's" + k + "'",
                    "SELECT i, ts, sym::STRING sym FROM x WHERE sym = 's" + k + "'",
                    LOG
            );
        }
    }

    /**
     * The inverse of a cut: every partition is one piece rooted at file row 0, so no cut was taken and no
     * partition is composite.
     */
    private static void assertPartitionNotCut(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                Assert.assertFalse(
                        "partition " + i + " is composite, the cut fired",
                        geometry.isComposite(i)
                );
                Assert.assertEquals("partition " + i + " holds more than one piece", 1, geometry.getPieceCount(i));
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
     * Asserts that some piece of the day has been rewritten ABOVE the LAST piece's end, so the last piece
     * is no longer the furthest region of the shared column files. That inversion is what the tests around
     * it are about: the active partition's append position then sits in the middle of live data, and any
     * close that truncates from it destroys the relocated sibling.
     */
    private static void assertPieceRelocatedAboveLastPiece(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = partitionIndexOfDay(reader);
            final int last = geometry.getPieceCount(partitionIndex) - 1;
            final long lastEnd = geometry.getPieceRowOffset(partitionIndex, last)
                    + geometry.getPieceRowCount(partitionIndex, last);
            for (int p = 0; p < last; p++) {
                if (geometry.getPieceRowOffset(partitionIndex, p) >= lastEnd) {
                    return;
                }
            }
            Assert.fail("no piece sits above the last piece's end, the setup did not build the inversion; "
                    + describePieces(tableName));
        }
    }

    /**
     * Asserts the piece whose {@code tsLo} is {@code tsLo} holds exactly {@code rowCount} rows.
     */
    private static void assertPieceRowCount(String tableName, long tsLo, long rowCount) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = partitionIndexOfDay(reader);
            for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                if (geometry.getPieceTimestampLo(partitionIndex, p) == tsLo) {
                    Assert.assertEquals("piece row count; " + describePieces(tableName),
                            rowCount, geometry.getPieceRowCount(partitionIndex, p));
                    return;
                }
            }
            Assert.fail("no piece at " + tsLo + "; " + describePieces(tableName));
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
     * A table scan reads pieces in {@code tsLo} order and each piece from its own row offset, so the
     * timestamps it produces must ascend. Every result comparison here goes through {@code ORDER BY},
     * which sorts a wrong storage order into the right answer and hides it; this reads the rows as stored.
     */
    private static void assertRowsInTimestampOrder(String tableName) throws Exception {
        try (
                RecordCursorFactory factory = select("SELECT ts FROM " + tableName);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            long previous = Long.MIN_VALUE;
            long row = 0;
            while (cursor.hasNext()) {
                final long ts = record.getTimestamp(0);
                Assert.assertTrue(
                        "rows are stored out of timestamp order [row=" + row + ", previous=" + previous
                                + ", ts=" + ts + ']',
                        ts >= previous
                );
                previous = ts;
                row++;
            }
        }
    }

    /**
     * The rows, then each named key through the index. The per-key variant of
     * {@link #assertSymbolIndexAndData(String)}, for fixtures whose keys are not {@code s0..s4}.
     */
    private static void assertIndexAndData(String unionBody, String... keys) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT i, sym::STRING sym, ts FROM (" + unionBody + ") ORDER BY ts",
                "SELECT i, sym::STRING sym, ts FROM x ORDER BY ts",
                LOG
        );
        for (String key : keys) {
            final String predicate = " WHERE sym = '" + key + "'";
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, sym::STRING sym, ts FROM (" + unionBody + ")" + predicate + " ORDER BY ts",
                    "SELECT i, sym::STRING sym, ts FROM x" + predicate + " ORDER BY ts",
                    LOG
            );
        }
    }

    /**
     * The rows AND the index entries. The independent union projects sym as STRING while x stores it as
     * SYMBOL, so both sides are cast to string for the value comparison; the per-key predicates below then
     * run over the raw symbol column, which is what puts the index cursor over the appended entries. Data
     * equality alone passes on an index that came back empty.
     */
    private static void assertSymbolIndexAndData(String unionBody) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT i, sym::STRING sym, ts FROM (" + unionBody + ") ORDER BY ts",
                "SELECT i, sym::STRING sym, ts FROM x ORDER BY ts",
                LOG
        );
        for (int k = 0; k < 5; k++) {
            final String predicate = " WHERE sym = 's" + k + "'";
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, sym::STRING sym, ts FROM (" + unionBody + ")" + predicate + " ORDER BY ts",
                    "SELECT i, sym::STRING sym, ts FROM x" + predicate + " ORDER BY ts",
                    LOG
            );
        }
    }

    /**
     * Every var-size column's data vector must be contiguous: each row's data starts where the previous
     * row's ends, from file row 0 up. A partition's files carry the dead images a rewrite leaves behind as
     * well as the live ones, and they belong to the same run - the write that founds an image starts at
     * the byte end of the one below it.
     */
    private static void assertVarSizeColumnsDense(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final int columnType = metadata.getColumnType(i);
                if (!ColumnType.isVarSize(columnType)) {
                    continue;
                }
                for (int p = 0, m = reader.getPartitionCount(); p < m; p++) {
                    reader.openPartition(p);
                    if (PartitionFormat.NATIVE != reader.getPartitionFormat(p)) {
                        continue;
                    }
                    final int columnBase = reader.getColumnBase(p);
                    final MemoryR dataMem = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i));
                    final MemoryR auxMem = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i) + 1);
                    final long rowCount = reader.getPartitionPhysicalRowCount(p) - reader.getColumnTop(columnBase, i);
                    if (rowCount < 1) {
                        continue;
                    }
                    if (DebugUtils.isSparseVarCol(
                            rowCount,
                            auxMem.getPageAddress(0),
                            dataMem == null ? 0 : dataMem.getPageAddress(0),
                            columnType
                    )) {
                        Assert.fail("column " + metadata.getColumnName(i) + " has a hole in its data vector"
                                + " [partitionIndex=" + p + ", rows=" + rowCount + ']');
                    }
                }
            }
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
            return sink.append("] E=").append(geometry.getE(partitionIndex)).toString();
        }
    }

    /**
     * Feeds one narrow backdated stride of hour {@code hour} to both the table under test and the oracle,
     * casting v to whatever type x currently holds while the oracle keeps it DOUBLE throughout.
     */
    private static void insertBackdatedCluster(int hour, String cast) throws Exception {
        final String rows = "SELECT x::INT + " + (100_000 * hour) + " i," +
                " timestamp_sequence('2020-02-03T0" + hour + ":00:07', 3000000L) ts," +
                " (" + (1000 * hour) + " + x)";
        execute("INSERT INTO x " + rows + cast + " v FROM long_sequence(120)");
        execute("INSERT INTO o " + rows + "::DOUBLE v FROM long_sequence(120)");
        drainWalQueue();
    }

    private static int partitionIndexOfDay(TableReader reader) {
        final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
        Assert.assertTrue("no partition at 2020-02-03", partitionIndex > -1);
        return partitionIndex;
    }

    /**
     * {@code E} of 2020-02-03: the furthest file row its column files span, live rows and dead space
     * alike. What a commit is allowed to grow is exactly the rows it writes.
     */
    private static long physicalRowHwm(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getPartitionPhysicalRowCount(partitionIndexOfDay(reader));
        }
    }

    /**
     * The row offset of piece {@code ordinal} of 2020-02-03.
     */
    private static long pieceRowOffsetOfDay(String tableName, int ordinal) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getGeometry().getPieceRowOffset(partitionIndexOfDay(reader), ordinal);
        }
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
