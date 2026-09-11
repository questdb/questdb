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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The OPPORTUNISTIC squash - the one housekeeping runs after every commit to keep the split population
 * under {@code o3.*.partition.max.splits} - folding COMPOSITE partitions, as source, as target, and as
 * both. It appends piece by piece with {@code FrameAlgebra}, starting at the target's physical extent
 * rather than at its live row count, and republishes the target's geometry with one more piece.
 * <p>
 * MOVE-TAIL leaves a sibling behind on every fire, so on a real-time ingest this is the only thing
 * keeping the split count bounded - and by then every partition in reach is composite.
 * <p>
 * Each test snapshots the day's rows IN CURSOR ORDER before the fold and compares afterwards, so a piece
 * appended at the wrong offset, in the wrong order, or with dead space left in shows up as a difference
 * rather than only as a row-count change.
 */
public class CompositePartitionSquashTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";

    @Before
    public void setUpSplits() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        // Hold the siblings still while the fixture builds; each test lowers this to let the fold run.
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1000);
    }

    @Test
    public void testAColumnAddedMidDaySurvivesTheFold() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            // A column added now has a top on every existing partition, so both sides of the fold carry
            // one and the target's share of the source's leading NULLs has to be written out.
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();
            execute("INSERT INTO x (i, s, ts, v) SELECT cast(x AS int) + 400_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + "T02:00:00', 1_000_000L) ts, x v FROM long_sequence(200)");
            drainWalQueue();
            makeComposite("T01:00:00");
            makeComposite("T11:00:00");

            assertFoldPreservesTheDay();
        });
    }

    /**
     * The column exists only in the SOURCES, so the composite target carries no data for it at all and the
     * fold has to write the target's share of it as NULLs.
     */
    @Test
    public void testAColumnOnlyTheSourcesHaveSurvivesTheFold() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();
            // Rows carrying v land in the LAST sibling only, well past anything the target holds.
            execute("INSERT INTO x (i, s, ts, v) SELECT cast(x AS int) + 400_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + "T11:00:00', 1_000_000L) ts, x v FROM long_sequence(200)");
            drainWalQueue();
            makeComposite("T01:00:00");

            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            final long vRows = scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "' AND v IS NOT NULL");
            final long vSum = scalar("SELECT sum(v) FROM x WHERE ts IN '" + DAY + "'");
            assertFoldPreservesTheDay();
            Assert.assertEquals("the fold lost rows of the added column", vRows,
                    scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "' AND v IS NOT NULL"));
            Assert.assertEquals("the fold changed the added column's values", vSum,
                    scalar("SELECT sum(v) FROM x WHERE ts IN '" + DAY + "'"));
        });
    }

    @Test
    public void testCompositeSourceIntoCompositeTarget() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            makeComposite("T01:00:00");   // the front sibling: the fold's target
            makeComposite("T11:00:00");   // the last sibling: one of its sources

            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            assertFoldPreservesTheDay();
        });
    }

    @Test
    public void testCompositeSourceIntoPlainTarget() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            // Only a later sibling is made composite, so the front one - the fold's target - stays plain
            // and the result collapses back to the ordinary single-piece shape.
            makeComposite("T11:00:00");

            Assert.assertFalse("fixture made the target composite", isComposite(DAY));
            assertFoldPreservesTheDay();
            Assert.assertFalse("folding into a plain target left it composite", isComposite(DAY));
        });
    }

    /**
     * The merged partition's stamp. {@code squashSplitPartitions} folds max(sources' seqTxn) into the
     * target, and a composite source keeps its stamp in {@code _geometry} rather than in the offset-3
     * word. {@code Math.max(0, ...)} floors a -1 read away, so a source whose stamp does not come back
     * would leave the merged partition reporting a seqTxn LOWER than one that wrote rows now inside it,
     * and unchanged across a fold that did change its bytes - both of which
     * {@link io.questdb.cairo.TxReader#getNativePartitionSeqTxn} promises never happens.
     * <p>
     * This holds today, and held before {@code PartitionGeometry.getSeqTxn} started resolving, but only
     * by an accident of ordering: the {@code isComposite} call a few lines above each read has already
     * pulled the slot into the resolved cache, so the old cache-only lookup happened to find it. Moving
     * or dropping that call would have silently dropped the stamp. Pin the outcome, not the ordering.
     */
    @Test
    public void testFoldCarriesACompositeSourcesSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            // Only the LAST sibling goes composite, so it is a source and the fold's target stays plain -
            // the one shape where the merged stamp is written into the offset-3 word and is observable.
            makeComposite("T11:00:00");

            Assert.assertFalse("fixture made the target composite", isComposite(DAY));
            final long targetSeqTxnBefore = seqTxnOfSibling(0);
            final long sourceSeqTxn = seqTxnOfSibling(2);
            Assert.assertTrue("fixture left the composite source unstamped", sourceSeqTxn > 0);
            Assert.assertTrue("the composite source must out-rank the target or the fold cannot show the drop",
                    sourceSeqTxn > targetSeqTxnBefore);

            assertFoldPreservesTheDay();

            Assert.assertFalse("folding into a plain target left it composite", isComposite(DAY));
            Assert.assertEquals("the fold dropped the composite source's stamp", sourceSeqTxn, seqTxnOfSibling(0));
        });
    }

    @Test
    public void testPlainSourceIntoCompositeTarget() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            makeComposite("T01:00:00");

            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            assertFoldPreservesTheDay();
            // The target keeps its dead space, so it stays composite: collapsing it to the plain shape
            // here would declare the dead rows live.
            Assert.assertTrue("folding into a composite target dropped its geometry", isComposite(DAY));
        });
    }

    /**
     * The fold refuses a composite LAST partition as a source - its file carries {@code lagRowCount} rows
     * past the live ones, belonging to no piece. When that refusal is the only thing on offer the pass
     * appends nothing, and it must then leave the target exactly as it found it. Publishing the plain
     * shape here would declare a composite target's dead rows live.
     */
    @Test
    public void testRefusedFoldLeavesTheTargetsGeometryAlone() throws Exception {
        assertMemoryLeak(() -> {
            createLastDaySplit();
            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            Assert.assertTrue("fixture lost the split", partitionCountOfDay() > 1);
            final String before = fingerprintOfDay();
            final long rows = rowsOfDay();

            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);
            execute("INSERT INTO x (i, s, ts) SELECT cast(x AS int) + 500_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + "T23:30:00', 1_000_000L) ts FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertTrue("the refused fold dropped the target's geometry", isComposite(DAY));
            Assert.assertEquals("the refused fold changed the day's row count", rows + 10, rowsOfDay());
            Assert.assertNotEquals("the trigger commit did not reach the day", before, fingerprintOfDay());
            assertDayReadsBack();
        });
    }

    /**
     * Reads the day back two ways that disagree the moment a piece lands at the wrong offset: the row
     * count against a fully ordered read, and the partition catalogue's timestamp bounds against the
     * data's own.
     */
    private static void assertDayReadsBack() throws Exception {
        Assert.assertEquals(
                "the day's timestamps came back unordered",
                rowsOfDay(),
                scalar("SELECT count() FROM (SELECT ts FROM x WHERE ts IN '" + DAY + "' ORDER BY ts)")
        );
        Assert.assertEquals(
                "the partition catalogue disagrees with the data on the day's first timestamp",
                scalar("SELECT min(ts)::long FROM x WHERE ts IN '" + DAY + "'"),
                scalar("SELECT min(minTimestamp)::long FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' AND NOT name LIKE '%.detached'")
        );
        Assert.assertEquals(
                "the partition catalogue disagrees with the data on the day's last timestamp",
                scalar("SELECT max(ts)::long FROM x WHERE ts IN '" + DAY + "'"),
                scalar("SELECT max(maxTimestamp)::long FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' AND NOT name LIKE '%.detached'")
        );
        Assert.assertEquals(
                "the partition catalogue disagrees with the data on the day's row count",
                rowsOfDay(),
                scalar("SELECT sum(numRows) FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' AND NOT name LIKE '%.detached'")
        );
    }

    /**
     * Lowers the split limit, drives one more commit so housekeeping folds, then asserts the day came
     * back with fewer directories and the same rows in the same order.
     */
    private static void assertFoldPreservesTheDay() throws Exception {
        final long siblingsBefore = partitionCountOfDay();
        Assert.assertTrue("fixture produced nothing to fold", siblingsBefore > 1);
        final String before = fingerprintOfDay();
        final long rowsBefore = rowsOfDay();

        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1);
        // A row in a LATER day, so the commit touches nothing in the day under test and only its
        // housekeeping can account for what changes there.
        execute("INSERT INTO x (i, s, ts) VALUES (999, 'z', '2024-01-05T00:00:00.000000Z')");
        drainWalQueue();

        // All the way down to the limit, not merely fewer: stopping short would mean the pass refused a
        // sibling, and refusing a composite one is exactly the bug this suite is here for.
        Assert.assertEquals(
                "housekeeping did not fold the day to the split limit, from " + siblingsBefore + " siblings",
                1,
                partitionCountOfDay()
        );
        Assert.assertEquals("the fold changed the day's row count", rowsBefore, rowsOfDay());
        Assert.assertEquals("the fold changed the day's rows or their order", before, fingerprintOfDay());
        assertDayReadsBack();
    }

    /**
     * A day whose split siblings are the LAST partitions of the table, with the final one composite -
     * the shape the fold has to refuse.
     */
    private static void createLastDaySplit() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        splitTheDayAt("T05:00:00");
        Assert.assertEquals("fixture did not split the day in two", 2, partitionCountOfDay());
        makeComposite("T01:00:00");
        makeComposite("T05:10:00");
    }

    /**
     * A day cut into three siblings, pushed off the end of the table so none of them is the last
     * partition and every one of them is foldable.
     */
    private static void createSplitDay() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        // Only the LAST partition ever splits, and only when the rows ahead of the O3 batch outnumber
        // everything behind it two to one - so each cut goes near the end of what the day holds so far,
        // and the day grows by an in-order append before the next one.
        splitTheDayAt("T05:00:00");
        execute("INSERT INTO x SELECT cast(x AS int) + 20_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "T05:33:21', 1_000_000L) ts FROM long_sequence(20_000)");
        drainWalQueue();
        splitTheDayAt("T10:30:00");
        Assert.assertEquals("fixture did not split the day into three", 3, partitionCountOfDay());

        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();
    }

    /**
     * Content fingerprint of one day IN CURSOR ORDER, so a piece appended at the wrong offset moves it
     * even when the row count holds.
     */
    private static String fingerprintOfDay() throws Exception {
        long count = 0;
        long hash = 0;
        try (RecordCursorFactory f = select("SELECT ts, i, s FROM x WHERE ts IN '" + DAY + "'")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    count++;
                    hash = hash * 1_000_003L + c.getRecord().getLong(0);
                    hash = hash * 1_000_003L + c.getRecord().getInt(1);
                    final CharSequence s = c.getRecord().getStrA(2);
                    hash = hash * 1_000_003L + (s == null ? -1 : s.hashCode());
                }
            }
        }
        return count + "/" + hash;
    }

    /**
     * Whether the day's OWN (front) partition is flagged composite.
     */
    private static boolean isComposite(String day) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    /**
     * Two narrow backdated strides over the same rows: merge-append rewrites the owning piece at the
     * shared file tail and abandons the previous copy, which is what leaves dead space behind.
     */
    private static void makeComposite(String timeOfDay) throws Exception {
        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x (i, s, ts) SELECT cast(x AS int) + 300_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + timeOfDay + "', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
    }

    /**
     * Attached partitions of the day under test; a detached directory still shows up here, so exclude it.
     */
    private static long partitionCountOfDay() throws Exception {
        return scalar("SELECT count() FROM table_partitions('x') WHERE name LIKE '" + DAY + "%'" +
                " AND NOT name LIKE '%.detached'");
    }

    /**
     * The resolved seqTxn of the day's sibling at {@code ordinal}: a composite one keeps its stamp in
     * {@code _geometry}, a plain one in the offset-3 word. Resolves the geometry first, so what the
     * fixture reads back does not itself depend on the accessor under test.
     */
    private static long seqTxnOfSibling(int ordinal) throws Exception {
        final long dayLo = MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            int seen = 0;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getLogicalPartitionTimestamp(txReader.getPartitionTimestampByIndex(i)) != dayLo) {
                    continue;
                }
                if (seen++ != ordinal) {
                    continue;
                }
                if (!txReader.isPartitionComposite(i)) {
                    return txReader.getNativePartitionSeqTxn(i);
                }
                reader.getGeometry().getPieceCount(i);
                return reader.getGeometry().getSeqTxn(i);
            }
        }
        return -1;
    }

    private static long rowsOfDay() throws Exception {
        return scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "'");
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }

    /**
     * Cuts a sibling off the tail of the day with an O3 write landing well inside it. Merge-append would
     * rewrite the piece in place instead, so it is off for the duration of the cut.
     */
    private static void splitTheDayAt(String timeOfDay) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        execute("INSERT INTO x (i, s, ts) SELECT cast(x AS int) + 200_000, rnd_str(5, 16, 2)," +
                " timestamp_sequence('" + DAY + timeOfDay + "', 1_000L) ts FROM long_sequence(200)");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
    }
}
