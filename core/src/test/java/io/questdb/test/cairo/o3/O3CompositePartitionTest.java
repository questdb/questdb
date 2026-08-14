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
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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
     * is KEPT and the batch becomes a piece of its own, so the commit writes only the rows it brought and
     * leaves no dead space at all.
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
                Assert.assertEquals("the batch should have become a piece of its own",
                        2, reader.getGeometry().getPieceCount(0));
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
}
