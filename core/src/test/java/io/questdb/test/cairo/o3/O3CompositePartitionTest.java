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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * End-to-end tests for writing a partition as a COMPOSITE - several pieces over one set of column files,
 * with the incoming rows appended at the tail and the untouched pieces left exactly where they are.
 * <p>
 * The table carries both fixed-width and var-size columns. The var-size ones are there for their AUX
 * VECTORS, which are what a merge has to rebuild: a merged row keeps its bytes but lands at a new offset, so
 * every entry has to be rewritten even though nothing about the value changed.
 * <p>
 * Each case asserts two independent things. The ROWS come from a table built by plain UNION ALL, which
 * never touches the composite machinery - so whatever pieces the plan decided on, the rows read back have
 * to be the same rows in the same order. The GEOMETRY is asserted separately, because a correct result read
 * back from a partition that was quietly rewritten whole would prove nothing about the design.
 */
public class O3CompositePartitionTest extends AbstractCairoTest {

    /**
     * A DOUBLE[], whose data vector holds a header as well as the values, so its entries vary in size for a
     * reason none of the other types share.
     */
    private static final String ARRAY_EXPR = "(CASE WHEN x % 13 = 0 THEN NULL::DOUBLE[]" +
            " WHEN x % 2 = 0 THEN ARRAY[x::double]" +
            " ELSE ARRAY[x::double, -x::double, 1.5] END)";
    /**
     * A BINARY, whose aux vector has STRING's N+1 shape but whose data entries are counted in bytes rather
     * than characters. The base64 text is padded to 4, 8 or 12 digits, so the values decode to 3, 6 or 9
     * bytes and no two neighbouring rows are the same length.
     */
    private static final String BINARY_EXPR = "(CASE WHEN x % 11 = 0 THEN NULL" +
            " ELSE from_base64(lpad(x::string, (4 * ((x % 3) + 1))::INT, '0')) END)";
    /**
     * A STRING, which stores every value in the data vector and describes it with an aux vector of N+1
     * entries - a different shape from VARCHAR's, and the one the merge writes a trailing offset for.
     */
    private static final String STRING_EXPR = "(CASE WHEN x % 5 = 0 THEN NULL ELSE 's-' || x END)::STRING";
    /**
     * An INDEXED SYMBOL. On top of the 32-bit key in the partition it carries a bitmap index, which lists
     * the rows each key appears at - so a merge has to publish index entries for the rows it writes, not
     * just the rows themselves.
     */
    private static final String SYMBOL_INDEXED_EXPR = "('i-' || (x % 3))::SYMBOL";
    /**
     * A SYMBOL. In the partition it is a 32-bit key like any other fixed-width column, but the value behind
     * that key lives in the table's symbol map, and a WAL commit remaps its keys on the way in.
     */
    private static final String SYMBOL_EXPR = "('sym-' || (x % 7))::SYMBOL";
    /**
     * A VARCHAR of values too long to inline, so every one of them is held in the data vector and every aux
     * entry is a pointer that a merge has to rewrite.
     */
    private static final String VARCHAR_LONG_EXPR = "('a-varchar-too-long-to-inline-' || x)::VARCHAR";
    /**
     * A VARCHAR carrying all three shapes an aux entry can take: NULL, inlined, and a pointer.
     */
    private static final String VARCHAR_MIXED_EXPR = "(CASE WHEN x % 7 = 0 THEN NULL" +
            " WHEN x % 3 = 0 THEN 'v' || x" +
            " ELSE 'a-varchar-too-long-to-inline-' || x END)::VARCHAR";
    /**
     * A VARCHAR whose values all fit inside the aux entry itself. Nothing is ever written to this column's
     * DATA VECTOR, which stays empty at zero bytes - a merge with an aux vector to rebuild and no data to
     * move.
     */
    private static final String VARCHAR_SHORT_EXPR = "('v' || x)::VARCHAR";
    /**
     * The oracle's outer projection. UNION ALL widens a SYMBOL to a STRING, so the column has to be put back
     * to the type the table under test declares before the two can be compared.
     */
    private static final String ORACLE_PROJECTION = "i, j, vs, vl, vm, s, b, a, sym::SYMBOL sym, symi::SYMBOL symi, ts";
    /**
     * The projection every one of the three batches shares. The values are functions of {@code x}
     * alone, so they are deterministic: the oracle table runs the same text a second time and has to get the
     * same bytes back.
     */
    private static final String WIDE_COLUMNS = VARCHAR_SHORT_EXPR + " vs, " + VARCHAR_LONG_EXPR + " vl, " +
            VARCHAR_MIXED_EXPR + " vm, " + STRING_EXPR + " s, " + BINARY_EXPR + " b, " + ARRAY_EXPR + " a, " +
            SYMBOL_EXPR + " sym, " + SYMBOL_INDEXED_EXPR + " symi";

    /**
     * The shape the design exists for: a narrow backdated batch landing inside a day. The rows either side
     * of where it lands are KEPT, and only the stride it overlaps is rewritten - so the dead space left
     * behind is the size of that stride, not the size of the partition.
     */
    @Test
    public void testBackdatedInsertMergesOnlyWhereItLands() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // ~43 rows at this table's ~190 bytes a row. The default is 50MB, which is over 250k rows -
            // far more than these partitions hold, so no cut would ever be worth proposing and every batch
            // would rewrite the whole day.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            // One day at 15s, so the partition holds 5760 rows.
            final String base = "SELECT x::INT i, -x j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            // A later day, so 2020-02-03 is never the active partition and the write goes through the O3
            // path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, -x - 90000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + "), INDEX(symi CAPACITY 32) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");

            final String backfill = "SELECT x::INT + 70000 i, -x - 70000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("the partition should have been cut into pieces",
                        reader.getGeometry().getPieceCount(0) > 1);

                // Only the rows the batch was merged with are dead. Every other row of the day stayed
                // exactly where it was, which is the claim the whole design rests on: had the partition
                // been rewritten whole, the dead space would be the 5760 rows it started with.
                final long liveRows = reader.getTxFile().getPartitionSize(0);
                final long deadRows = reader.getPartitionPhysicalRowCount(0) - liveRows;
                Assert.assertEquals(5960, liveRows);
                Assert.assertTrue("rewrote too much of the partition [deadRows=" + deadRows + ']',
                        deadRows > 0 && deadRows < 1500);
            }

            // The var-size columns carry values, and not the same value twice. Without this the row
            // comparison below would pass just as happily on columns of nulls, since the oracle is built
            // from the same expressions.
            assertQuery("SELECT count(vs) vs, count(vl) vl, count(vm) vm, count_distinct(vl) dvl," +
                    " count(s) s, sum(length(b)) b, sum(a[1]) a FROM x WHERE ts IN '2020-02-03'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("vs\tvl\tvm\tdvl\ts\tb\ta\n5960\t5960\t5110\t5760\t4768\t31973\t1.5331722E7\n");

            // The oracle: the same rows, assembled without ever touching the composite machinery.
            execute("CREATE TABLE o AS (SELECT " + ORACLE_PROJECTION + " FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + backfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertSameRows();

            // ...and again with no reader or writer cached, so the read comes off _txn and _geometry as
            // they are on disk rather than out of anything still resident.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSameRows();
        });
    }

    /**
     * Rows that sort above everything the partition holds, but still inside its day. Every existing piece
     * is KEPT and the batch becomes a piece of its own - but the KEPT piece and the new one land back to
     * back with no gap between them, so together they TILE {@code [0, physicalRows)}. A tiled partition
     * needs no {@code _geometry} record at all: its boundaries carry nothing a plain row count doesn't
     * already say (see {@code O3PartitionJob.processCompositePartition}'s {@code isComposite} check and
     * "Non-composite is not a special case" in {@code COMPOSITE_PARTITION_STATE.md}), so the commit
     * abandons the update it built and the partition reads back as an ordinary, non-composite one - one
     * piece, not composite - even though internally it took two actions to get there.
     */
    @Test
    public void testChronologicalAppendRewritesNothing() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // ~43 rows at this table's ~190 bytes a row. The default is 50MB, which is over 250k rows -
            // far more than these partitions hold, so no cut would ever be worth proposing and every batch
            // would rewrite the whole day.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            // 10s apart, so 5760 rows reach 15:59:50 and leave the rest of the day empty for the tail.
            final String base = "SELECT x::INT i, -x j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03', 10*1000000L) ts" +
                    " FROM long_sequence(5760)";
            final String nextDay = "SELECT x::INT + 90000 i, -x - 90000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + "), INDEX(symi CAPACITY 32) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final String tail = "SELECT x::INT + 80000 i, -x - 80000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03T20:00:00', 1000000L) ts FROM long_sequence(100)";
            execute("INSERT INTO x " + tail);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            try (TableReader reader = engine.getReader(xt)) {
                // The KEPT piece and the new one tile [0, physicalRows) with no gap, so the commit
                // abandoned the geometry it built instead of publishing it - the partition reads back as
                // an ordinary, single-piece one, which is the correct and cheaper outcome for a shape a
                // plain row count already describes in full.
                Assert.assertFalse("a tiled partition should not be composite",
                        reader.getTxFile().isPartitionComposite(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
                // Nothing was rewritten, so there is no dead space: every file row is a live row.
                Assert.assertEquals(5860, reader.getTxFile().getPartitionSize(0));
                Assert.assertEquals(5860, reader.getPartitionPhysicalRowCount(0));
            }

            execute("CREATE TABLE o AS (SELECT " + ORACLE_PROJECTION + " FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + tail +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertSameRows();

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSameRows();
        });
    }

    /**
     * A writer that rounds its timestamps to the second (or the minute) sends many rows sharing one
     * timestamp, one commit at a time. Every commit after the first TIES the partition's own {@code tsHi}
     * instead of landing strictly above it, and with no DEDUP key on the table there is nothing to compare
     * a tie against - so each commit should extend the one existing piece in place, exactly as a strictly
     * chronological batch already does in {@link #testChronologicalAppendRewritesNothing}. Regression test
     * for the composite planner instead forcing every such commit through a MERGE, rewriting the whole
     * partition again for each one: seconds' worth of ties compounded into a piece count and a dead-row
     * count that both grew without bound.
     */
    @Test
    public void testChronologicalTiesAppendInsteadOfMerging() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later day, so 2020-02-03 is never the active partition and every insert into it - even the
            // very first one - goes through the composite dispatch, exactly like the rows chronologically
            // arrive from a real writer rather than through the plain in-order append fast path.
            execute("INSERT INTO x SELECT x::INT + 90000, timestamp_sequence('2020-02-06', 60*1000000L)" +
                    " ts FROM long_sequence(1)");
            drainWalQueue();

            // Three seconds, three separate commits each, five rows a commit: within a second every commit
            // TIES the one before it; across seconds each commit still lands strictly above the last.
            int i = 0;
            for (int second = 0; second < 3; second++) {
                for (int commit = 0; commit < 3; commit++) {
                    execute("INSERT INTO x SELECT " + i + " + x::INT," +
                            " timestamp_sequence('2020-02-03T10:00:0" + second + "', 0) ts FROM long_sequence(5)");
                    i += 5;
                    drainWalQueue();
                }
            }
            final int totalRows = i;

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertFalse("same-second commits with no dedup key should never leave the partition composite",
                        reader.getTxFile().isPartitionComposite(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
                // Every commit tiled onto the one piece, so there is no dead space: every file row is live.
                Assert.assertEquals(totalRows, reader.getTxFile().getPartitionSize(0));
                Assert.assertEquals(totalRows, reader.getPartitionPhysicalRowCount(0));
            }

            assertQuery("SELECT count() c FROM x WHERE ts < '2020-02-04'")
                    .noRandomAccess().expectSize().returns("c\n" + totalRows + "\n");
            assertQuery("SELECT count() c FROM x WHERE ts = '2020-02-03T10:00:00.000000'")
                    .noRandomAccess().expectSize().returns("c\n15\n");
            assertQuery("SELECT count() c FROM x WHERE ts = '2020-02-03T10:00:02.000000'")
                    .noRandomAccess().expectSize().returns("c\n15\n");
        });
    }

    /**
     * The same tie-avoids-merge idea as {@link #testChronologicalTiesAppendInsteadOfMerging}, but the tie
     * lands on an EARLIER piece instead of the tail's. That piece owns none of the files' tail, so there is
     * nowhere to append to in place - but outside dedup the tie still needs no key comparison, so it should
     * be spared from the piece's own claim rather than force a full rewrite of it. A first tie founds its
     * own single-point piece in the gap; a second tie at the very same instant must NOT found a second one
     * at that instant too - it merges into the one the first tie already founded.
     */
    @Test
    public void testTieOnAnEarlierPieceFoundsThenMergesASinglePointPiece() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later day, so 2020-02-03 is never the active partition and every insert into it goes
            // through the composite dispatch.
            execute("INSERT INTO x SELECT x::INT + 90000, timestamp_sequence('2020-02-06', 60*1000000L)" +
                    " ts FROM long_sequence(1)");
            drainWalQueue();

            // A wide chronological base, followed by a narrow backfill landing in the middle: this carves
            // the day into a KEPT head piece, a MERGED piece where the backfill landed, and a KEPT tail
            // piece - the same shape testBackdatedInsertMergesOnlyWhereItLands builds and verifies in full.
            execute("INSERT INTO x SELECT x::INT, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)");
            drainWalQueue();
            execute("INSERT INTO x SELECT x::INT + 70000, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(200)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            final long tieTs;
            final int pieceCountBefore;
            final long deadRowsBefore;
            try (TableReader reader = engine.getReader(xt)) {
                final PartitionGeometry geometry = reader.getGeometry();
                pieceCountBefore = geometry.getPieceCount(0);
                Assert.assertTrue("the backfill should have cut the day into pieces", pieceCountBefore > 1);
                // The KEPT head piece: untouched by the backfill, so it should span real hours, not a
                // single instant.
                final long headTsLo = geometry.getPieceTimestampLo(0, 0);
                final long headTsHi = geometry.getPieceTimestampHi(0, 0);
                Assert.assertTrue("the head piece should span a real range, not a single instant", headTsLo < headTsHi);
                tieTs = headTsHi;
                deadRowsBefore = reader.getPartitionPhysicalRowCount(0) - reader.getTxFile().getPartitionSize(0);
            }

            // A commit that ties the HEAD piece's own tsHi.
            execute("INSERT INTO x SELECT (-1)::INT, " + tieTs + "::TIMESTAMP FROM long_sequence(1)");
            drainWalQueue();

            final int pieceCountAfterFirstTie;
            try (TableReader reader = engine.getReader(xt)) {
                final PartitionGeometry geometry = reader.getGeometry();
                pieceCountAfterFirstTie = geometry.getPieceCount(0);
                Assert.assertEquals("the tie should found exactly one new piece",
                        pieceCountBefore + 1, pieceCountAfterFirstTie);
                final long deadRowsAfter = reader.getPartitionPhysicalRowCount(0) - reader.getTxFile().getPartitionSize(0);
                Assert.assertEquals("the head piece should not have been rewritten", deadRowsBefore, deadRowsAfter);
            }

            // A second commit ties the SAME instant again.
            execute("INSERT INTO x SELECT (-2)::INT, " + tieTs + "::TIMESTAMP FROM long_sequence(1)");
            drainWalQueue();

            try (TableReader reader = engine.getReader(xt)) {
                final PartitionGeometry geometry = reader.getGeometry();
                Assert.assertEquals(
                        "a repeat tie should merge into the existing single-point piece, not found another",
                        pieceCountAfterFirstTie, geometry.getPieceCount(0));
                final long deadRowsAfter = reader.getPartitionPhysicalRowCount(0) - reader.getTxFile().getPartitionSize(0);
                // The single-point piece held exactly the first tie's one row; merging the second tie into
                // it makes that one row dead and nothing else - the head piece is still untouched.
                Assert.assertEquals(deadRowsBefore + 1, deadRowsAfter);

                assertNoPieceSharesBoundsWithAnother(geometry, 0);
            }

            assertQuery("SELECT count() c FROM x WHERE ts = " + tieTs + "::TIMESTAMP")
                    .noRandomAccess().expectSize().returns("c\n3\n");
        });
    }

    /**
     * Reproduces (and, via {@code TableWriter}'s degraded-geometry compaction, verifies the fix for) a
     * correctness gap left by the tie-avoids-merge optimisation
     * ({@link #testTieOnAnEarlierPieceFoundsThenMergesASinglePointPiece}): a directory can end up with two
     * TOUCHING pieces - a real piece ending at {@code V} and a single-point piece {@code [V,V]} right after
     * it - built while DEDUP was off. Turning DEDUP on afterwards and writing a row at {@code V} must still
     * compare against BOTH of them.
     * <p>
     * {@link io.questdb.cairo.O3CompositeMergeStrategy#computeActions} walks pieces in order and gives each
     * O3 row to the FIRST piece whose range claims it. A row at {@code V} is claimed by the earlier, real
     * piece (its own {@code tsHi}), which is placed at the tail as one {@code MERGE} action. That merge's
     * dedup pass ({@code O3PartitionJob#executeCompositePlan}'s {@code MERGE} case, {@code getDedupRows})
     * compares the incoming row only against THAT piece's own {@code [pieceLo, pieceHi)} rows - it never
     * reads the second, single-point piece at the very same instant, because {@code computeActions} already
     * advanced its O3 cursor past every row at {@code V} before that piece's own turn in the loop. Left
     * unfixed, the single-point piece stays an untouched {@code KEEP} and its row keeps its STALE value
     * forever, since every later commit at {@code V} keeps resolving to the same earlier piece first, never
     * the sibling sitting right behind it.
     * <p>
     * DEDUP does not collapse pre-existing duplicate keys - that is expected, not a bug: two rows can
     * already share a timestamp from before DEDUP was ever enabled, and enabling it is not retroactive.
     * What DEDUP does promise is that a commit whose row matches an EXISTING key updates every row holding
     * that key to the incoming value, never leaving one behind stale. That is what this test checks: not
     * that the two rows at {@code V} collapse to one, but that neither survives with its old value.
     */
    @Test
    public void testDedupUpdatesEveryRowInATouchingSinglePointPiece() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later day, so 2020-02-03 is never the active partition and every insert into it goes
            // through the composite dispatch.
            execute("INSERT INTO x SELECT x::INT + 90000, timestamp_sequence('2020-02-06', 60*1000000L)" +
                    " ts FROM long_sequence(1)");
            drainWalQueue();

            // A wide chronological base, followed by a narrow backfill landing in the middle: this carves
            // the day into a KEPT head piece, a MERGED piece where the backfill landed, and a KEPT tail
            // piece - the same shape testBackdatedInsertMergesOnlyWhereItLands builds and verifies in full.
            execute("INSERT INTO x SELECT x::INT, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)");
            drainWalQueue();
            execute("INSERT INTO x SELECT x::INT + 70000, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(200)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            final long tieTs;
            try (TableReader reader = engine.getReader(xt)) {
                final PartitionGeometry geometry = reader.getGeometry();
                Assert.assertTrue("the backfill should have cut the day into pieces: " + describePieces(reader, 0),
                        geometry.getPieceCount(0) > 1);
                final long headTsLo = geometry.getPieceTimestampLo(0, 0);
                final long headTsHi = geometry.getPieceTimestampHi(0, 0);
                Assert.assertTrue("the head piece should span a real range, not a single instant: " + describePieces(reader, 0),
                        headTsLo < headTsHi);
                tieTs = headTsHi;
            }

            // Still no DEDUP key: this tie is spared from the head piece's claim (see
            // testTieOnAnEarlierPieceFoundsThenMergesASinglePointPiece) and founds a TOUCHING single-point
            // piece [tieTs, tieTs] right after the head piece - both now hold one row apiece at tieTs.
            execute("INSERT INTO x SELECT (-1)::INT, " + tieTs + "::TIMESTAMP FROM long_sequence(1)");
            drainWalQueue();

            assertQuery("SELECT count() c FROM x WHERE ts = " + tieTs + "::TIMESTAMP")
                    .noRandomAccess().expectSize().returns("c\n2\n");

            // Turn DEDUP on: from here on, at most one row per ts should ever survive a commit that
            // touches an existing one.
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts)");
            drainWalQueue();

            // A dedup commit landing exactly on tieTs. It must be compared against BOTH existing rows at
            // tieTs (the head piece's own last row and the single-point piece's row) and update both to
            // the incoming value - DEDUP does not collapse them to one row, and this test does not expect
            // it to.
            execute("INSERT INTO x SELECT (-2)::INT, " + tieTs + "::TIMESTAMP FROM long_sequence(1)");
            drainWalQueue();

            final TableToken xtAfterDedup = engine.verifyTableName("x");
            Assert.assertFalse("the dedup commit suspended the table",
                    engine.getTableSequencerAPI().isSuspended(xtAfterDedup));

            // Still 2 rows - DEDUP never collapses pre-existing duplicate keys - but BOTH must now carry
            // the incoming value. Before the fix, the single-point piece's row was never compared and
            // kept its stale -1.
            assertQuery("SELECT count() c FROM x WHERE ts = " + tieTs + "::TIMESTAMP")
                    .noRandomAccess().expectSize().returns("c\n2\n");
            assertQuery("SELECT i FROM x WHERE ts = " + tieTs + "::TIMESTAMP ORDER BY i")
                    .returns("i\n-2\n-2\n");
        });
    }

    /**
     * A day is built into a composite partition, the table is TRUNCATEd, and the same day is built into a
     * composite partition again from nothing - each round's backdated batches submitted without a drain
     * between them, so {@code ApplyWal2TableJob} replays them as ONE bundled WAL transaction block, letting
     * transaction clustering cut the fresh partition into several pieces from a single commit.
     * <p>
     * Minimised from a WAL fuzz failure ({@code WalWriterFuzzTest#testAddDropColumnDropPartition}) that
     * lost exactly one row at a piece boundary the first time a truncated table's partition went composite
     * again. Root cause not yet isolated: {@code TxWriter.removeAllPartitions()} clears
     * {@code attachedPartitions} cleanly and {@link PartitionGeometry#resolveInternal} re-derives its
     * {@code (partitionTimestamp, nameTxn)} cache key from the live {@code TxReader} on every call, so a
     * stale resolver-cache entry surviving the truncate looks ruled out on inspection - the loss happens
     * somewhere else in the truncate-then-rebuild path.
     */
    @Test
    public void testTruncateThenRebuildComposite() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // This table is narrower than the WIDE_COLUMNS ones the rest of this class uses, so the split
            // threshold - in rows derived from an average record size - needs a proportionally smaller
            // setting before a cut is worth proposing at all.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            final String base = "SELECT x::INT i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(5760)";
            // A later day, so 2020-02-03 is never the active partition and the backdated batches go
            // through the O3 path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            // Three backdated batches, each in its own hour with cold gaps either side, so transaction
            // clustering has separate hot strides to cut around rather than one contiguous stride.
            final String batch1 = "SELECT x::INT + 70000 i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-03T02:00:07', 5*1000000L) ts FROM long_sequence(120)";
            final String batch2 = "SELECT x::INT + 80000 i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-03T08:00:11', 5*1000000L) ts FROM long_sequence(120)";
            final String batch3 = "SELECT x::INT + 60000 i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-03T14:00:23', 5*1000000L) ts FROM long_sequence(120)";

            // First round: build 2020-02-03 into a composite partition. The three backdated batches are
            // submitted without draining between them, so ApplyWal2TableJob replays them as ONE bundled WAL
            // transaction block - exactly how the fuzz failure's own replay batched several original
            // commits into a single o3 partition task.
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();
            execute("INSERT INTO x " + batch1);
            execute("INSERT INTO x " + batch2);
            execute("INSERT INTO x " + batch3);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("the first round should have gone composite",
                        reader.getGeometry().getPieceCount(0) > 1);
            }

            // Wipe the table and build the SAME calendar day into a composite partition again, from
            // nothing - a brand new directory, a brand new _geometry file - via the same bundled-replay
            // shape.
            execute("TRUNCATE TABLE x");
            drainWalQueue();

            execute("INSERT INTO x " + base);
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();
            execute("INSERT INTO x " + batch1);
            execute("INSERT INTO x " + batch2);
            execute("INSERT INTO x " + batch3);
            drainWalQueue();

            Assert.assertFalse("rebuilding a composite partition after truncate suspended the table",
                    engine.getTableSequencerAPI().isSuspended(xt));

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("the rebuilt partition should have gone composite too",
                        reader.getGeometry().getPieceCount(0) > 1);
            }

            // The oracle needs only the POST-truncate rows - that is all TRUNCATE TABLE leaves standing.
            execute("CREATE TABLE o AS (SELECT i, s, vs, ts FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + batch1 +
                    " UNION ALL " + batch2 + " UNION ALL " + batch3 +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM o ORDER BY ts, i",
                    "SELECT * FROM x ORDER BY ts, i",
                    LOG
            );

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM o ORDER BY ts, i",
                    "SELECT * FROM x ORDER BY ts, i",
                    LOG
            );
        });
    }

    /**
     * {@code ALTER TABLE ... ALTER COLUMN TYPE} over a directory that already has DEAD pieces - a merge
     * relocated one to the tail, leaving its old copy behind, so every column's file spans more rows than
     * are live. A conversion rewrites a FILE, not a query result, and it has to span that whole extent
     * without either crashing on the dead bytes or letting them leak into a live row's value.
     * <p>
     * Three shapes at once: a plain fixed-to-fixed cast (walks the directory's own pieces and pads the
     * gaps, never reading them), a var-to-fixed cast and a fixed-to-var cast (both still read the flat
     * {@code [columnTop, E)} range as a whole, dead space included - this is the case the fixed-column
     * rework does not yet cover).
     */
    @Test
    public void testConvertColumnTypeAcrossDeadPieces() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // This table is narrower than the WIDE_COLUMNS ones the rest of this class uses, so the split
            // threshold - in rows derived from an average record size - needs a proportionally smaller
            // setting before a cut is worth proposing at all.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            // One day, three columns present from row 0: a FIXED one (i), a STRING one (s), a VARCHAR one
            // (vs) - one of each shape the conversion has to handle.
            final String base = "SELECT x::INT i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(5760)";
            // A later day, so 2020-02-03 is never the active partition and the backfill below goes through
            // the O3 path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            // A backdated batch relocates the piece it lands in to the tail, leaving the old copy behind
            // as dead space.
            final String backfill = "SELECT x::INT + 70000 i, " + STRING_EXPR + " s, " + VARCHAR_SHORT_EXPR + " vs," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("the partition should have dead pieces",
                        reader.getGeometry().getPieceCount(0) > 1);
            }

            execute("ALTER TABLE x ALTER COLUMN i TYPE LONG");
            execute("ALTER TABLE x ALTER COLUMN s TYPE VARCHAR");
            execute("ALTER TABLE x ALTER COLUMN vs TYPE STRING");
            drainWalQueue();
            Assert.assertFalse("column type conversion over dead pieces suspended the table",
                    engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n6010\n");
            // Every row's s/vs value is deterministic in x - a conversion that let dead bytes leak into a
            // live row, instead of the row's own real value, would not match this count.
            assertQuery("SELECT count() c FROM x WHERE s IS NOT NULL AND vs IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n4808\n");

            // The oracle: the same rows, assembled without ever touching the composite machinery, already
            // typed the way the real table ends up after conversion.
            execute("CREATE TABLE o AS (SELECT i::LONG i, s::VARCHAR s, vs::STRING vs, ts FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + backfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM o ORDER BY ts, i",
                    "SELECT * FROM x ORDER BY ts, i",
                    LOG
            );

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM o ORDER BY ts, i",
                    "SELECT * FROM x ORDER BY ts, i",
                    LOG
            );
        });
    }

    /**
     * The byte-level proof behind the padding rule, over dead pieces, across every direction the rule
     * applies to: FIXED (INT -> LONG), VAR (STRING <-> VARCHAR) and MIXED (LONG -> VARCHAR,
     * VARCHAR -> LONG).
     * <p>
     * A FIXED destination pads its {@code .d} file with nulls for every dead row - the row still occupies
     * a fixed-width slot whether it was converted or not, so the file always reaches the full physical
     * extent. A VAR destination pads its {@code .i} (aux) file the same way, but keeps its {@code .d} file
     * exactly as dense as it would be with no dead space at all: VARCHAR's null costs zero data bytes, so
     * its {@code .d} file matches an oracle built from the live rows alone, byte for byte; STRING's null
     * costs exactly {@code Integer.BYTES} (the length prefix), so its {@code .d} file matches the oracle
     * plus one marker per dead row - never the dead row's own (unconverted) value.
     * <p>
     * {@code cLong -> VARCHAR} and {@code cVarNum -> LONG} exercise the MIXED (fixed source/var
     * destination and var source/fixed destination) directions, piece-walked the same way as the pure
     * fixed and pure var cases.
     */
    @Test
    public void testColumnConversionKeepsVarDataDenseAcrossDeadSpace() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            // cLong's values are all the same digit width (15, comfortably over VARCHAR's 9-byte inline
            // ceiling), so a correct conversion's .d size is a plain multiplication rather than a sum over
            // per-row lengths - and a naive one that converts dead rows too is off by a whole multiple of
            // deadRows, not just a few stray bytes.
            final String base = "SELECT x::INT i, x::INT cFix, ('long-string-' || x)::STRING cS," +
                    " ('varchar-string-' || x)::VARCHAR cV, (700_000_000_000_000L + x) cLong, x::VARCHAR cVarNum," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(5760)";
            final String nextDay = "SELECT x::INT + 90000 i, x::INT + 90000 cFix, ('long-string-' || x)::STRING cS," +
                    " ('varchar-string-' || x)::VARCHAR cV, (700_000_000_000_000L + x) cLong, x::VARCHAR cVarNum," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final String backfill = "SELECT x::INT + 70000 i, x::INT + 70000 cFix, ('long-string-' || x)::STRING cS," +
                    " ('varchar-string-' || x)::VARCHAR cV, (700_000_000_000_000L + x) cLong, x::VARCHAR cVarNum," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final long liveRows;
            final long physicalRows;
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("the partition should have dead pieces", reader.getGeometry().getPieceCount(0) > 1);
                liveRows = reader.getTxFile().getPartitionSize(0);
                physicalRows = reader.getPartitionPhysicalRowCount(0);
            }
            final long deadRows = physicalRows - liveRows;
            Assert.assertEquals(5960, liveRows);
            Assert.assertTrue("no dead space to prove the rule against [deadRows=" + deadRows + ']', deadRows > 0);

            // The oracle: the same live rows, converted the same way, with no composite machinery and
            // therefore no dead space at all - the baseline every .d size below is measured against.
            execute("CREATE TABLE o AS (SELECT i, cFix, cS, cV, cLong, cVarNum, ts FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + backfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            execute("ALTER TABLE x ALTER COLUMN cFix TYPE LONG");
            execute("ALTER TABLE x ALTER COLUMN cS TYPE VARCHAR");
            execute("ALTER TABLE x ALTER COLUMN cV TYPE STRING");
            execute("ALTER TABLE x ALTER COLUMN cLong TYPE VARCHAR");
            execute("ALTER TABLE x ALTER COLUMN cVarNum TYPE LONG");
            drainWalQueue();
            Assert.assertFalse("column type conversion over dead pieces suspended the table",
                    engine.getTableSequencerAPI().isSuspended(xt));

            execute("ALTER TABLE o ALTER COLUMN cFix TYPE LONG");
            execute("ALTER TABLE o ALTER COLUMN cS TYPE VARCHAR");
            execute("ALTER TABLE o ALTER COLUMN cV TYPE STRING");
            execute("ALTER TABLE o ALTER COLUMN cLong TYPE VARCHAR");
            execute("ALTER TABLE o ALTER COLUMN cVarNum TYPE LONG");

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader xr = engine.getReader(xt);
                 TableReader or = engine.getReader(engine.verifyTableName("o"))) {

                // FIXED destination: every dead row still occupies its 8-byte slot, converted or not - the
                // .d file always reaches the full physical extent, never just the live one.
                Assert.assertTrue("cFix: .d file must reach the full physical extent",
                        columnFileSize(xr, 0, "cFix") >= physicalRows * Long.BYTES);
                // MIXED, var source into a fixed destination: same fixed-slot rule, exercised from a
                // var-size source this time.
                Assert.assertTrue("cVarNum: .d file must reach the full physical extent",
                        columnFileSize(xr, 0, "cVarNum") >= physicalRows * Long.BYTES);

                // VAR destination, VARCHAR: a null costs nothing in the data vector, so a correctly
                // piece-walked conversion leaves .d exactly as large as it would be with no dead rows at
                // all - measured off the aux vector's own offsets, which are immune to page rounding in
                // the underlying file.
                Assert.assertEquals("cS: dead space must not grow the VARCHAR .d file",
                        dataVectorSizeAt(or, 0, "cS", liveRows - 1), dataVectorSizeAt(xr, 0, "cS", physicalRows - 1));
                Assert.assertEquals("cLong: dead space must not grow the VARCHAR .d file",
                        dataVectorSizeAt(or, 0, "cLong", liveRows - 1), dataVectorSizeAt(xr, 0, "cLong", physicalRows - 1));
                // Closed form for cLong, since every value converts to the same 15-byte string: proves the
                // oracle comparison above isn't passing by accident.
                Assert.assertEquals(liveRows * 15, dataVectorSizeAt(xr, 0, "cLong", physicalRows - 1));

                // VAR destination, STRING: a null costs exactly Integer.BYTES (the length prefix), one per
                // dead row - never the dead row's own (unconverted) value.
                Assert.assertEquals("cV: dead space must cost exactly one null marker per row in the STRING .d file",
                        dataVectorSizeAt(or, 0, "cV", liveRows - 1) + deadRows * Integer.BYTES,
                        dataVectorSizeAt(xr, 0, "cV", physicalRows - 1));
            }

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT i, cFix, cS, cV, cLong, cVarNum, ts FROM o ORDER BY ts, i",
                    "SELECT i, cFix, cS, cV, cLong, cVarNum, ts FROM x ORDER BY ts, i",
                    LOG
            );
        });
    }

    /**
     * A directory of many pieces, read through the INTERVAL cursor. Resolving a {@code WHERE ts BETWEEN}
     * to a row range is a search over the timestamp column, and a composite directory breaks the two things
     * an ordinary partition lets that search assume: that the rows are one contiguous run of file rows, and
     * that file order is timestamp order.
     * <p>
     * A partition can hold thousands of pieces once a fine cut floor has been in use for a while, so the
     * per-piece lookups are asserted here as well as the rows. They are binary searches, and a binary search
     * that agrees with a linear one on a handful of pieces can still disagree on the boundary cases - hence
     * a window at every piece edge, and one either side of it.
     */
    @Test
    public void testIntervalScanAcrossManyPieces() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            final String base = "SELECT x::INT i, -x j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(5760)";
            // A later day, so 2020-02-03 is never the active partition and every batch below goes through
            // the O3 path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, -x - 90000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + "), INDEX(symi CAPACITY 32) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            // Eleven backdated batches, two hours apart, each landing between rows the batch before it left
            // alone. The day accumulates pieces instead of being rewritten whole.
            StringBuilder backfillUnion = new StringBuilder();
            for (int hour = 1; hour < 23; hour += 2) {
                String backfill = "SELECT x::INT + " + (100_000 * hour) + " i," +
                        " -x - " + (100_000L * hour) + " j, " + WIDE_COLUMNS + "," +
                        " timestamp_sequence('2020-02-03T" + String.format("%02d", hour) + ":07:03', 5*1000000L) ts" +
                        " FROM long_sequence(40)";
                execute("INSERT INTO x " + backfill);
                drainWalQueue();
                backfillUnion.append(" UNION ALL ").append(backfill);
            }

            final TableToken xt = engine.verifyTableName("x");
            final LongList boundaries = new LongList();
            try (TableReader reader = engine.getReader(xt)) {
                final PartitionGeometry geometry = reader.getGeometry();
                final int pieceCount = geometry.getPieceCount(0);
                Assert.assertTrue("expected a many-piece directory, got " + pieceCount, pieceCount >= 8);

                // The lookups are binary searches, so assert them against the definitions they stand for:
                // cumulativeLo is the running sum of the row counts before a piece, the shift is the gap
                // between that and where the piece actually sits in the files, and a piece owns its own
                // first row, its own last row and its own tsLo.
                long cumulative = 0;
                for (int p = 0; p < pieceCount; p++) {
                    final long rows = geometry.getPieceRowCount(0, p);
                    Assert.assertEquals("cumulativeLo of piece " + p, cumulative, geometry.getPieceCumulativeLo(0, p));
                    Assert.assertEquals("shift of piece " + p,
                            geometry.getPieceRowOffset(0, p) - cumulative, geometry.getPieceShift(0, p));
                    Assert.assertEquals("piece owning its first row", p, geometry.findPieceByRow(0, cumulative));
                    Assert.assertEquals("piece owning its last row", p, geometry.findPieceByRow(0, cumulative + rows - 1));
                    Assert.assertEquals("piece owning its tsLo", p, geometry.findPiece(0, geometry.getPieceTimestampLo(0, p)));
                    boundaries.add(geometry.getPieceTimestampLo(0, p));
                    cumulative += rows;
                }
                Assert.assertEquals("pieces must sum to the live rows", cumulative, geometry.getLiveRows(0));
            }

            // The oracle reaches the same rows by a FULL scan, which is a different cursor from the interval
            // one under test. It is written in one ordered pass, so it stays a plain partition.
            execute("CREATE TABLE o AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // A window straddling every piece boundary, where an off-by-one in the row range would show.
            for (int i = 0, n = boundaries.size(); i < n; i++) {
                final long boundary = boundaries.getQuick(i);
                assertSameWindow(boundary - 60 * 1000000L, boundary + 60 * 1000000L);
                assertSameWindow(boundary, boundary + 1);
                assertSameWindow(boundary - 1, boundary);
            }

            // A full, unfiltered scan is a different cursor path from the interval windows above - it is
            // FwdTableReaderPageFrameCursor cutting frames at piece boundaries rather than
            // CompositeTimestampFinder searching within one - so it needs its own check. The oracle here is
            // built from the raw generating SQL rather than "SELECT * FROM x": an oracle read back through
            // x's own (possibly broken) full scan would inherit the same corruption and the comparison
            // would pass either way.
            execute("CREATE TABLE o2 AS (SELECT " + ORACLE_PROJECTION + " FROM (" +
                    base + " UNION ALL " + nextDay + backfillUnion + ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // No ORDER BY: a bare table scan, exactly what the fuzz suite's own _nonwal-vs-_wal comparison
            // runs (FuzzRunner.runFuzz: "String limit = \"\"; assertSqlCursors(tableNameNoWal + limit,
            // tableNameWal + limit, ...)"). It trusts the reader to already hand back ts order on its own,
            // which is the exact claim FwdTableReaderPageFrameCursor's piece-boundary stitching makes.
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "o2", "x", LOG);
        });
    }

    /**
     * A merge whose DATA side carries a column top. A column added part way through a partition's life has
     * no entry in its file for the rows written before it, so a merge that reaches below that top cannot
     * read them - they are NULL, and nothing on disk says so.
     * <p>
     * Both regimes are here, because they take different arithmetic. {@code mid_*} are added while
     * 2020-02-03 is still being written, so their top lands in the MIDDLE of the day: the merge reads real
     * values above it and NULLs below. {@code all_*} are added once a later day is active, so 2020-02-03
     * never gets a record for them at all and every row of the merge's data side is NULL.
     * <p>
     * Only the data side can have a top. The O3 side is the batch this commit is writing, so every row of
     * it exists - which is what lets the top-aware kernels take the top as a single number instead of the
     * caller building a nulls-and-data image of the column first.
     */
    @Test
    public void testMergeReadsBelowAColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            // The day's FIRST half, written before the mid_* columns exist.
            final String lower = "SELECT x::INT i, -x j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(2880)";
            execute("CREATE TABLE x AS (" + lower + "), INDEX(symi CAPACITY 32) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Added while 2020-02-03 is the last partition, so the top is recorded ON it, at row 2880 -
            // half way through the day the batch below merges into.
            execute("ALTER TABLE x ADD COLUMN mid_i INT");
            execute("ALTER TABLE x ADD COLUMN mid_vs VARCHAR");
            execute("ALTER TABLE x ADD COLUMN mid_vl VARCHAR");
            execute("ALTER TABLE x ADD COLUMN mid_s STRING");
            execute("ALTER TABLE x ADD COLUMN mid_b BINARY");
            execute("ALTER TABLE x ADD COLUMN mid_a DOUBLE[]");
            drainWalQueue();

            // The day's SECOND half, carrying the new columns: the rows ABOVE the top.
            final String midColumns = "(x + 500000)::INT mid_i, " + VARCHAR_SHORT_EXPR + " mid_vs, " +
                    VARCHAR_LONG_EXPR + " mid_vl, " + STRING_EXPR + " mid_s, " + BINARY_EXPR + " mid_b, " +
                    ARRAY_EXPR + " mid_a";
            final String upper = "SELECT x::INT + 20000 i, -x - 20000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03T12:00:00', 15*1000000L) ts, " + midColumns +
                    " FROM long_sequence(2880)";
            execute("INSERT INTO x " + upper);
            // A later day, so 2020-02-03 stops being the active partition and the batch below goes through
            // the O3 path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, -x - 90000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts, " + midColumns +
                    " FROM long_sequence(50)";
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            assertQuery("SELECT count(mid_i) above FROM x WHERE ts >= '2020-02-03T12:00:00' AND ts < '2020-02-04'")
                    .noRandomAccess().expectSize().returns("above\n2880\n");

            // Added with 2020-02-06 active, so 2020-02-03 carries no record for these at all and reads
            // every one of its rows as absent.
            execute("ALTER TABLE x ADD COLUMN all_i LONG");
            execute("ALTER TABLE x ADD COLUMN all_v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN all_s STRING");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");

            // The batch. It lands at 04:00, BELOW both tops, so the merge has to produce NULLs for the
            // data side's mid_* and all_* rows while writing real values for its own.
            final String allColumns = "(x + 600000)::LONG all_i, " + VARCHAR_MIXED_EXPR + " all_v, " +
                    STRING_EXPR + " all_s";
            final String backfill = "SELECT x::INT + 70000 i, -x - 70000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts, " + midColumns + ", " +
                    allColumns + " FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();
            Assert.assertFalse("the merge below a column top suspended the table",
                    engine.getTableSequencerAPI().isSuspended(xt));

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("the partition should have been cut into pieces",
                        reader.getGeometry().getPieceCount(0) > 1);
            }

            // The merge really did span the top: rows below it read NULL, rows above it read values, and
            // the batch's own rows read values wherever it lands. Without this the comparison below would
            // pass on a column the merge had filled with nulls throughout.
            assertQuery("SELECT count(mid_i) below, count(mid_vs) below_vs, count(mid_s) below_s" +
                    " FROM x WHERE ts < '2020-02-03T04:00:00'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("below\tbelow_vs\tbelow_s\n0\t0\t0\n");
            assertQuery("SELECT count(mid_i) c, count(all_i) a, count(all_v) v FROM x" +
                    " WHERE ts >= '2020-02-03T04:00:00' AND ts < '2020-02-03T04:20:00'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ta\tv\n200\t200\t172\n");
            assertQuery("SELECT count(mid_i) above, count(all_i) absent FROM x" +
                    " WHERE ts >= '2020-02-03T12:00:00' AND ts < '2020-02-04'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("above\tabsent\n2880\t0\n");
            assertQuery("SELECT count(mid_i) c, count(all_i) a, count(all_v) v FROM x" +
                    " WHERE ts >= '2020-02-03T04:00:00' AND ts < '2020-02-03T04:20:00'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ta\tv\n200\t200\t172\n");

            // The oracle: the same rows, assembled without ever touching the composite machinery. Each
            // batch supplies NULL for whatever it predates, exactly as the table should read it back.
            final String nulls = ", NULL::INT mid_i, NULL::VARCHAR mid_vs, NULL::VARCHAR mid_vl," +
                    " NULL::STRING mid_s, NULL::BINARY mid_b, NULL::DOUBLE[] mid_a";
            final String allNulls = ", NULL::LONG all_i, NULL::VARCHAR all_v, NULL::STRING all_s";
            execute("CREATE TABLE o AS (SELECT " + ORACLE_PROJECTION +
                    ", mid_i, mid_vs, mid_vl, mid_s, mid_b, mid_a, all_i, all_v, all_s FROM (" +
                    "SELECT x::INT i, -x j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts" + nulls + allNulls +
                    " FROM long_sequence(2880)" +
                    " UNION ALL SELECT x::INT + 20000 i, -x - 20000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-03T12:00:00', 15*1000000L) ts, " + midColumns + allNulls +
                    " FROM long_sequence(2880)" +
                    " UNION ALL SELECT x::INT + 90000 i, -x - 90000L j, " + WIDE_COLUMNS + "," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts, " + midColumns + allNulls +
                    " FROM long_sequence(50)" +
                    " UNION ALL " + backfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertSameRows();

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSameRows();
        });
    }

    /**
     * A REPLACE RANGE commit whose lower bound falls inside a partition this table has already committed,
     * but that carries no O3 rows of its own for that partition - the one row this transaction inserts
     * lands entirely in the day above. The composite path has no replace-range deletion logic at all, so
     * the partition is left exactly as it was; that omission is a documented gap, not what this test
     * checks.
     * <p>
     * What it checks is what gets REPORTED: the sink used to carry the caller's raw incoming timestamp,
     * which under a replace commit is the replace range's own lower bound rather than any timestamp this
     * partition holds. Since the partition is the table's first, {@code o3ConsumePartitionUpdateSink} took
     * that value as the table's new floor outright, moving it to a timestamp that is neither the
     * partition's true floor nor the incoming transaction's own minimum - which is exactly what {@code
     * TableWriter.processWalCommit}'s post-replace assertion checks for.
     */
    @Test
    public void testReplaceRangeWithNoOwnRowsDoesNotMoveThePartitionFloor() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");

            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2022-02-24T00:00:00', 3600L*1000000L) ts, x v" +
                    " FROM long_sequence(10)");
            // A later day, so 2022-02-24 is no longer the active partition when the replace commit lands.
            execute("INSERT INTO x SELECT timestamp_sequence('2022-02-26T00:00:00', 3600L*1000000L) ts, x + 100 v" +
                    " FROM long_sequence(10)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");

            // The range's lower bound (05:00) falls inside 2022-02-24's already-committed data and its
            // upper bound falls inside 2022-02-25 - but the row this transaction inserts lands in
            // 2022-02-25, so 2022-02-24 gets no O3 rows of its own from this commit.
            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T05:00:00.000000Z");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-25T01:00:00.000000Z");
            try (WalWriter ww = engine.getWalWriter(xt)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-25T00:30:00.000000Z"));
                row.putLong(1, 999L);
                row.append();
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            Assert.assertFalse("the replace commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));
            assertQuery("SELECT min(ts) mn FROM x").expectSize().timestamp("mn")
                    .returns("mn\n2022-02-24T00:00:00.000000Z\n");
        });
    }

    /**
     * A commit that BOTH merge-appends the still-active last partition into a composite one AND lands rows
     * on a brand-new later partition - a day rollover crossed by the very same commit that promotes the
     * day it rolls off. {@code txWriter}'s last-partition pointer moves to the new day before {@code
     * columns[]} is ever told the old one went composite, so a plain reuse-via-close of {@code columns[]}
     * (the next {@code openPartition}, repointing the same {@code MemoryMA} objects at the new day) would
     * truncate the old day's files down to {@code columns[]}'s stale, pre-promotion append offset -
     * discarding every row the composite frame executor appended since, silently, because the geometry it
     * published is never consulted by that close. The corruption itself throws nothing; only a LATER
     * commit or read that maps the partition against its (unaffected) geometry notices the file fell
     * short.
     * <p>
     * Minimised from a {@code WalWriterFuzzTest#testWalWriteManyTablesInOrder} failure: "composite
     * timestamp column file too short".
     */
    @Test
    public void testMergeAppendAcrossDayRolloverInSameCommit() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // This table is narrower than the WIDE_COLUMNS ones the rest of this class uses, so the split
            // threshold - in rows derived from an average record size - needs a proportionally smaller
            // setting before a cut is worth proposing at all.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            // 2020-02-03 alone, in order - the table's only partition, so it is still the writer's active
            // last partition when the next commit lands.
            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // ONE commit: a backdated batch that merge-appends into 2020-02-03 (promoting it to
            // composite), UNIONed with rows that land on 2020-02-04 - a brand-new partition this SAME
            // commit creates.
            // Big enough that the gap between the stale pre-promotion row count (5760) and E after the
            // merge clears a whole OS page (512 rows for an 8-byte column) - otherwise page-rounding both
            // sides up to the same page hides the corruption by sheer luck.
            final String backfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(2000)";
            final String nextDay = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-04', 60*1000000L) ts" +
                    " FROM long_sequence(50)";
            execute("INSERT INTO x " + backfill + " UNION ALL " + nextDay);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("2020-02-03 should have gone composite",
                        reader.getGeometry().getPieceCount(0) > 1);
                // The direct check: the ts column FILE has to reach at least E rows, independently of
                // whether anything has read it back yet - a reuse-via-close that truncated it to a stale
                // pre-promotion offset would still leave the geometry claiming E, and only a LATER commit
                // or read that maps against that claim would notice.
                final long requiredBytes = reader.getGeometry().getE(0) * Long.BYTES;
                Assert.assertTrue("ts column file [" + columnFileSize(reader, 0, "ts") + "] shorter than E requires [" + requiredBytes + ']',
                        columnFileSize(reader, 0, "ts") >= requiredBytes);
            }

            // The real failure needed a SECOND commit landing back on 2020-02-03 - now composite and no
            // longer last - to surface the corruption: the truncate already happened silently above.
            final String again = "SELECT x::INT + 80000 i, timestamp_sequence('2020-02-03T10:00:00', 5*1000000L) ts" +
                    " FROM long_sequence(50)";
            execute("INSERT INTO x " + again);
            drainWalQueue();
            Assert.assertFalse("the follow-up commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            execute("CREATE TABLE o AS (SELECT i, ts FROM (" +
                    base + " UNION ALL " + backfill + " UNION ALL " + nextDay + " UNION ALL " + again +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM o ORDER BY ts, i",
                    "SELECT * FROM x ORDER BY ts, i",
                    LOG
            );
        });
    }

    /**
     * Dropping the partition below a composite one has to recompute the table's new min timestamp from
     * what remains on disk - and file row 0 of a composite directory is not that value once a
     * merge-append has relocated the piece that used to own it to the tail. The relocated piece keeps its
     * timestamp range, so file row 0 goes on holding the superseded, now-dead value.
     */
    @Test
    public void testDroppingFirstPartitionReadsTrueMinAcrossPieces() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Narrower than the WIDE_COLUMNS tables the rest of this class uses, so the split threshold -
            // in rows derived from an average record size - needs a proportionally smaller setting.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            // Dropped once 2020-02-03 is composite, promoting 2020-02-03 to the table's first partition.
            final String day1 = "SELECT x::INT i, timestamp_sequence('2020-02-01', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            // Starts at noon, so its own min timestamp - the one sitting at file row 0 - leaves the
            // morning free for a later backdated batch to relocate into.
            final String day2 = "SELECT x::INT + 60000 i, timestamp_sequence('2020-02-03T12:00:00', 15*1000000L) ts" +
                    " FROM long_sequence(2880)";
            // Keeps 2020-02-03 from being the writer's active last partition when the backdated batch
            // lands, so the write goes through the O3 path rather than an append to the open partition.
            final String day3 = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-06', 60*1000000L) ts" +
                    " FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + day1 + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + day2);
            execute("INSERT INTO x " + day3);
            drainWalQueue();

            // Lands entirely before 2020-02-03's current min of noon: the merge writes the merged piece
            // at the tail, so file row 0 keeps the stale noon value while the directory's true min drops
            // to midnight.
            final String backfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T00:00:00', 5*1000000L) ts" +
                    " FROM long_sequence(2000)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("2020-02-03 should have gone composite", reader.getGeometry().getPieceCount(1) > 1);
            }

            execute("ALTER TABLE x DROP PARTITION LIST '2020-02-01'");
            drainWalQueue();
            Assert.assertFalse("the drop suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The QUERY reads the composite geometry directly and gets this right either way - the field
            // the bug corrupts is the table's CACHED min timestamp, which nothing re-derives from the
            // geometry unless asked to, so it has to be checked on its own rather than through a query.
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertEquals(
                        "cached table min timestamp must match the composite directory's true min",
                        "2020-02-03T00:00:00.000000Z",
                        Micros.toUSecString(reader.getMinTimestamp())
                );
            }

            assertQuery("select ts from x order by ts limit 1")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\n2020-02-03T00:00:00.000000Z\n");
        });
    }

    /**
     * The mirror image of {@link #testDroppingFirstPartitionReadsTrueMinAcrossPieces}: dropping the
     * ACTIVE (last) partition promotes its predecessor to last and has to recompute the table's new max
     * timestamp from what remains on disk. The old code read that off byte offset
     * {@code (liveRows - 1) * 8} in the ts column - the live row count's own offset, not the physical
     * one - which lands inside a composite directory's dead space or a relocated piece instead of the
     * physically-last live row once a merge-append has moved rows around.
     */
    @Test
    public void testDroppingLastPartitionReadsTrueMaxAcrossPieces() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            // Promoted to last once 2020-02-06 is dropped below it, so its own max - 23:59:45, the last
            // row of a day filled end to end - has to survive the promotion.
            final String day1 = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            // The partition that gets dropped: keeps 2020-02-03 from being the writer's active last
            // partition when the backdated batch lands, so that write goes through the O3 path.
            final String day3 = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-06', 60*1000000L) ts" +
                    " FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + day1 + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + day3);
            drainWalQueue();

            // Lands inside 2020-02-03, well short of its 23:59:45 close: the merge relocates the pieces it
            // touches to the tail, so the file row that used to sit at physical offset (liveRows - 1) is
            // no longer the row holding the day's unmoved, still-true max.
            final String backfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(2000)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("2020-02-03 should have gone composite", reader.getGeometry().getPieceCount(0) > 1);
            }

            execute("ALTER TABLE x DROP PARTITION LIST '2020-02-06'");
            drainWalQueue();
            Assert.assertFalse("the drop suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The QUERY reads the composite geometry directly and gets this right either way - the field
            // the bug corrupts is the table's CACHED max timestamp, which nothing re-derives from the
            // geometry unless asked to, so it has to be checked on its own rather than through a query.
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertEquals(
                        "cached table max timestamp must match the composite directory's true max",
                        "2020-02-03T23:59:45.000000Z",
                        Micros.toUSecString(reader.getMaxTimestamp())
                );
            }

            assertQuery("select ts from x order by ts limit -1")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\n2020-02-03T23:59:45.000000Z\n");
        });
    }

    /**
     * Regression test for a fixed bug: {@code CONVERT PARTITION TO PARQUET} used to call
     * {@code TableWriter.getPartitionSize} (live rows) and map each column file as one flat range from
     * byte 0 for that many rows - correct for an ordinary partition, but wrong for a composite one, whose
     * live rows sit out of order behind dead space and relocated pieces. The fix compacts a composite
     * partition ahead of conversion so the parquet encoder always reads an ordinary, contiguous directory.
     * Minimised from a natural {@code WalWriterFuzzTest#testConvertPartitionToParquet} failure - the fuzz
     * suite's own {@code _nonwal}-vs-{@code _wal} comparison reported "wrong row" at an arbitrary offset,
     * which is what a dropped row does to every comparison after it, not a hint that the row itself was
     * corrupted.
     */
    @Test
    public void testConvertingCompositePartitionToParquetKeepsAllRows() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Narrower than the WIDE_COLUMNS tables the rest of this class uses, so the split threshold -
            // in rows derived from an average record size - needs a proportionally smaller setting.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            // Keeps 2020-02-03 from being the writer's active last partition when the backdated batch
            // lands, so that write goes through the O3 path and can be cut into pieces.
            final String nextDay = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-06', 60*1000000L) ts" +
                    " FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            // Lands inside 2020-02-03: the merge relocates the pieces it touches to the tail, cutting the
            // day into several pieces instead of rewriting it whole.
            final String backfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(2000)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("2020-02-03 should have gone composite", reader.getGeometry().getPieceCount(0) > 1);
            }

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-02-03'");
            drainWalQueue();
            Assert.assertFalse("the conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The oracle: the same rows, assembled without ever touching the composite machinery, so any
            // row the conversion dropped shows up as a row-count/content mismatch here.
            execute("CREATE TABLE o AS (SELECT i, ts FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + backfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "o", "x", LOG);
        });
    }

    @Test
    public void testConvertingCompositePartitionToParquetWithColumnAddedAfterPartitionRolledOver() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            // Keeps 2020-02-03 from being the writer's active last partition when the backdated batch
            // lands, so that write goes through the O3 path and can be cut into pieces.
            final String nextDay = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-06', 60*1000000L) ts" +
                    " FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            // Lands inside 2020-02-03: the merge relocates the pieces it touches to the tail, cutting the
            // day into several pieces instead of rewriting it whole.
            final String backfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(2000)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertTrue("2020-02-03 should have gone composite", reader.getGeometry().getPieceCount(0) > 1);
            }

            // Added once the writer has already rolled onto 2020-02-06: 2020-02-03 - still composite,
            // still holding its own pieces - never gets an explicit column-version record for this column
            // at all, unlike the ordinary case of adding a column to the still-active last partition.
            execute("ALTER TABLE x ADD COLUMN new_col INT");
            drainWalQueue();
            final String moreOnNextDay = "SELECT x::INT + 95000 i, timestamp_sequence('2020-02-06T01', 60*1000000L) ts," +
                    " (x + 95000)::INT new_col FROM long_sequence(20)";
            execute("INSERT INTO x " + moreOnNextDay);
            drainWalQueue();
            Assert.assertFalse("adding the column suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-02-03'");
            drainWalQueue();
            Assert.assertFalse("the conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The oracle: the same rows, assembled without ever touching the composite machinery, so any
            // row the conversion dropped - or any wrong value it read for a column that never had real
            // data in this directory - shows up as a row-count/content mismatch here.
            execute("CREATE TABLE o AS (SELECT i, ts, new_col FROM (" +
                    "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts, NULL::INT new_col" +
                    " FROM long_sequence(5760)" +
                    " UNION ALL SELECT x::INT + 90000 i, timestamp_sequence('2020-02-06', 60*1000000L) ts," +
                    " NULL::INT new_col FROM long_sequence(50)" +
                    " UNION ALL SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts," +
                    " NULL::INT new_col FROM long_sequence(2000)" +
                    " UNION ALL " + moreOnNextDay +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "o", "x", LOG);
        });
    }

    /**
     * The .d file's own logical size at the given (0-based) row, read off the .i (aux) vector's own
     * offsets rather than the .d file's raw length - which can be page-rounded larger than what was
     * actually written, and would make an exact-size assertion fragile for reasons that have nothing to
     * do with the padding rule under test.
     */
    private static long dataVectorSizeAt(TableReader reader, int partitionIndex, String columnName, long row) {
        final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
        final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
        final int columnIndex = reader.getMetadata().getColumnIndex(columnName);
        final int columnType = reader.getMetadata().getColumnType(columnIndex);
        // The column-version file is keyed by the column's WRITER index, not its positional index in the
        // metadata - changeColumnType gives a converted column a brand new writer index (columnCount at
        // conversion time), even though it keeps the same positional slot getColumnIndex() answers with.
        final long columnNameTxn = reader.getColumnVersionReader()
                .getColumnNameTxn(partitionTimestamp, reader.getMetadata().getWriterIndex(columnIndex));
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(reader.getTableToken());
        TableUtils.setPathForNativePartition(
                path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), partitionTimestamp, partitionNameTxn
        );
        long fd = TableUtils.openRO(ff, TableUtils.iFile(path, columnName, columnNameTxn), LOG);
        try {
            return ColumnType.getDriver(columnType).getDataVectorSizeAtFromFd(ff, fd, row);
        } finally {
            ff.close(fd);
        }
    }

    /**
     * The raw .d file length for a fixed-size column. Fixed columns have no separate aux vector to read
     * an exact logical size off, so this is used only for a "reaches at least the full extent" check, not
     * for an exact one - the file can be legitimately page-rounded larger than {@code rowCount * width}.
     */
    private static long columnFileSize(TableReader reader, int partitionIndex, String columnName) {
        final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
        final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
        final int columnIndex = reader.getMetadata().getColumnIndex(columnName);
        final long columnNameTxn = reader.getColumnVersionReader()
                .getColumnNameTxn(partitionTimestamp, reader.getMetadata().getWriterIndex(columnIndex));
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(reader.getTableToken());
        TableUtils.setPathForNativePartition(
                path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), partitionTimestamp, partitionNameTxn
        );
        return ff.length(TableUtils.dFile(path, columnName, columnNameTxn));
    }

    private static void assertSameRows() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT * FROM o ORDER BY ts, i",
                "SELECT * FROM x ORDER BY ts, i",
                LOG
        );
        // The same rows again, this time reached through the BITMAP INDEX on symi. The oracle has no index
        // on that column, so it answers by scanning; a merge that wrote its rows but not their index entries
        // would show up here and nowhere else.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT * FROM o WHERE symi = 'i-1' ORDER BY ts, i",
                "SELECT * FROM x WHERE symi = 'i-1' ORDER BY ts, i",
                LOG
        );
    }

    /**
     * The same half-open window over both tables. The oracle is a plain partition, so its interval scan
     * takes the single-search path, and the table under test takes the per-piece one.
     */
    private static void assertSameWindow(long lo, long hi) throws Exception {
        final String where = " WHERE ts >= " + lo + "::TIMESTAMP AND ts < " + hi + "::TIMESTAMP ORDER BY ts, i";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT * FROM o" + where,
                "SELECT * FROM x" + where,
                LOG
        );
    }

    /**
     * No two pieces of one directory should ever describe the exact same instant - that would make them
     * indistinguishable to a caller resolving a timestamp to a piece, and defeats the point of sparing a
     * tie in the first place: it exists so a REPEAT tie merges into the piece the first one founded rather
     * than founding a competing one right next to it.
     */
    private static String describePieces(TableReader reader, int partitionIndex) {
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

    private static void assertNoPieceSharesBoundsWithAnother(PartitionGeometry geometry, int partitionIndex) {
        final int pieceCount = geometry.getPieceCount(partitionIndex);
        for (int a = 0; a < pieceCount; a++) {
            final long aLo = geometry.getPieceTimestampLo(partitionIndex, a);
            final long aHi = geometry.getPieceTimestampHi(partitionIndex, a);
            for (int b = a + 1; b < pieceCount; b++) {
                final long bLo = geometry.getPieceTimestampLo(partitionIndex, b);
                final long bHi = geometry.getPieceTimestampHi(partitionIndex, b);
                Assert.assertFalse(
                        "pieces " + a + " and " + b + " share identical bounds [" + aLo + "," + aHi + "]",
                        aLo == bLo && aHi == bHi
                );
            }
        }
    }
}
