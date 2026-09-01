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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionSpec;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The PER-CELL storage-policy commit entry points:
 * {@code markPartitionParquetReady(ts, cellKey)} and
 * {@code switchNativePartitionWithParquet(ts, cellKey, size)}.
 * <p>
 * These exist so the enterprise storage-policy pipeline can drive a composite day one cell at a time.
 * The by-timestamp forms are refused on a composite table (see {@code CompositeStoragePolicyTest})
 * because a day is N cell partitions and a timestamp does not identify one.
 * <p>
 * The parquet files are PLANTED rather than encoded, exactly as the plain-table tests for these
 * methods do ({@code NativePartitionSeqTxnTest#plantEmptyDataParquet}): neither method reads the file,
 * they check existence and hard-link it. Producing real parquet is the enterprise job's half and is
 * tested there; what is under test here is the per-cell resolution, path building and {@code _txn}
 * bookkeeping.
 */
public class CompositeStoragePolicyPerCellTest extends AbstractCompositeTwinTest {

    /**
     * {@code preparePartitionForParquetConversion} must handle EVERY cell of the day.
     * <p>
     * It is the first step the enterprise policy job takes, and it does two mutating things per
     * partition: force-squash, and delete any stale parquet left by an earlier conversion. Resolved by
     * timestamp alone -- as it was -- both land on cellKey 0, leaving every sibling's stale parquet in
     * place for the encoder to trip over later.
     * <p>
     * NON-VACUITY: a stale parquet file is planted in BOTH cells and both must be gone afterwards. A
     * cellKey-0 implementation removes one and passes every assertion that only counts.
     */
    @Test
    public void testPrepareForParquetConversionHandlesEveryCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // day 2 keeps day 1 out of the active slot -- prepare skips the active partition
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableWriter w = getWriter("c")) {
                final TxWriter tx = w.getTxWriter();
                final long day1 = tx.getPartitionTimestampByIndex(0);
                final IntList cells = cellsOfDay(w, day1);
                Assert.assertEquals("the day must have two cells or the per-cell claim is untestable",
                        2, cells.size());

                // plant a STALE parquet in every cell -- prepare must clear them all
                for (int i = 0; i < cells.size(); i++) {
                    plantCellParquet(w, "c", day1, cells.getQuick(i));
                }

                Assert.assertEquals("prepare must accept a composite day and return its timestamp",
                        day1, w.preparePartitionForParquetConversion(day1));

                for (int i = 0; i < cells.size(); i++) {
                    Assert.assertFalse(
                            "stale parquet must be gone from cell " + cells.getQuick(i),
                            cellParquetExists(w, "c", day1, cells.getQuick(i)));
                }
            }
        });
    }

    /**
     * The cell segment must render in HIVE form ON DEMAND, whatever the table's own {@code LAYOUT}.
     * <p>
     * The enterprise cold-storage bucket key is self-describing storage that other tools read, so it
     * carries {@code exch=BTC} even for a table stored locally as {@code LAYOUT PLAIN} (which renders
     * {@code BTC}). The 2-arg renderer follows the table, so the tiering path needs a form that takes
     * the mode explicitly.
     * <p>
     * NON-VACUITY: the table is created {@code LAYOUT PLAIN} precisely so the two renderings DIFFER.
     * On a HIVE table both forms agree and the test could not fail.
     */
    @Test
    public void testCellSegmentRendersHiveOnDemandRegardlessOfLayout() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T01:00:00.000000Z','BTC',1.0)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableWriter w = getWriter("p")) {
                final int cellKey = w.getTxWriter().getPartitionCellKey(0);

                final StringSink asStored = new StringSink();
                w.renderCellSegment(asStored, cellKey);
                TestUtils.assertEquals("BTC", asStored);

                final StringSink asBucketKey = new StringSink();
                w.renderCellSegment(asBucketKey, cellKey, PartitionSpec.MODE_HIVE);
                TestUtils.assertEquals("exch=BTC", asBucketKey);

                // and the explicit-mode form must still be able to render the table's own shape
                final StringSink asPlain = new StringSink();
                w.renderCellSegment(asPlain, cellKey, PartitionSpec.MODE_PLAIN);
                TestUtils.assertEquals("BTC", asPlain);
            }
        });
    }

    /**
     * Both cells of a day switch to parquet independently.
     * <p>
     * NON-VACUITY: the day deliberately has TWO cells and the test asserts BOTH end up parquet. A
     * cellKey-0 implementation -- the defect family this whole branch exists to fix -- would switch
     * cell 0 and leave its sibling native, which is what the second assertion catches. The test also
     * asserts the cells carry DIFFERENT directories, so "both parquet" cannot be satisfied by one
     * shared day container.
     */
    @Test(timeout = 120_000)
    public void testEachCellOfADaySwitchesToParquetIndependently() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // Day 2 exists so day 1 is not the ACTIVE partition -- the switch refuses the active one.
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableWriter w = getWriter("c")) {
                final TxWriter tx = w.getTxWriter();
                final long day1 = tx.getPartitionTimestampByIndex(0);
                final IntList cells = cellsOfDay(w, day1);
                Assert.assertEquals("the day must have two cells or the per-cell claim is untestable",
                        2, cells.size());

                for (int i = 0; i < cells.size(); i++) {
                    final int cellKey = cells.getQuick(i);
                    plantCellParquet(w, "c", day1, cellKey);
                    Assert.assertTrue("mark-ready must accept cell " + cellKey,
                            w.markPartitionParquetReady(day1, cellKey));
                    Assert.assertEquals("switch must succeed for cell " + cellKey,
                            TableWriter.SWITCH_OK, w.switchNativePartitionWithParquet(day1, cellKey, 0L));
                }

                // EVERY cell of the day must now be parquet, not just cell 0.
                for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                    if (tx.getPartitionTimestampByIndex(i) == day1) {
                        Assert.assertTrue(
                                "cell " + tx.getPartitionCellKey(i) + " of the day must be parquet;"
                                        + " a cellKey-0 implementation leaves the sibling native",
                                tx.isPartitionParquet(i));
                    }
                }
            }
        });
    }

    /**
     * Marking ONE cell ready must flag THAT cell and not its sibling.
     * <p>
     * Both cells get a parquet file planted, and only the SECOND is marked. That ordering is what makes
     * the test load-bearing: a cellKey-0 implementation resolves the mark to cell 0 regardless of the
     * argument, so it would flag the FIRST cell and leave the second unflagged -- the exact inversion
     * the two assertions below check.
     * <p>
     * An earlier version planted only one file and asserted the sibling returned false. MEASURED by
     * mutation (resolve the index by timestamp instead of by cell): that version still PASSED, because
     * the mutant's path was built from the requested cell's segment and simply missed the file. Only
     * the sibling-flag inversion below actually distinguishes the two implementations.
     */
    @Test(timeout = 120_000)
    public void testMarkingOneCellDoesNotFlagItsSibling() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableWriter w = getWriter("c")) {
                final TxWriter tx = w.getTxWriter();
                final long day1 = tx.getPartitionTimestampByIndex(0);
                final IntList cells = cellsOfDay(w, day1);
                Assert.assertEquals(2, cells.size());

                final int first = cells.getQuick(0);
                final int second = cells.getQuick(1);

                // BOTH cells get a file, so the mark cannot succeed merely by finding one.
                plantCellParquet(w, "c", day1, first);
                plantCellParquet(w, "c", day1, second);

                // Mark the SECOND cell only.
                Assert.assertTrue("mark-ready must accept the requested cell",
                        w.markPartitionParquetReady(day1, second));

                for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                    if (tx.getPartitionTimestampByIndex(i) != day1) {
                        continue;
                    }
                    final int cellKey = tx.getPartitionCellKey(i);
                    if (cellKey == second) {
                        Assert.assertTrue("the REQUESTED cell must be flagged", tx.isPartitionParquetGenerated(i));
                    } else {
                        Assert.assertFalse(
                                "the cell that was NOT requested must stay unflagged; flagging it is what"
                                        + " a cellKey-0 resolution does", tx.isPartitionParquetGenerated(i));
                    }
                }
            }
        });
    }

    /**
     * THE CONTROL: on a PLAIN table the same cell-aware entry points delegate to the by-timestamp
     * implementation and still work. Without this, the assertions above would hold equally if the new
     * overloads were broken for everyone.
     */
    @Test(timeout = 120_000)
    public void testPlainTableStillWorksThroughTheCellAwareOverload() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableWriter w = getWriter("p")) {
                final TxWriter tx = w.getTxWriter();
                final long day1 = tx.getPartitionTimestampByIndex(0);

                plantPlainParquet("p", day1, tx.getPartitionNameTxn(0), tx.getTimestampType());
                // cellKey is ignored on a plain table.
                Assert.assertTrue("the cell-aware overload must delegate on a plain table",
                        w.markPartitionParquetReady(day1, 0));
                Assert.assertTrue("day1 must be flagged parquet-generated", tx.isPartitionParquetGenerated(0));
            }
        });
    }

    /**
     * {@code linkPartitionIndexFiles(ts, cellKey, oldNameTxn, newNameTxn)} -- the entry point the
     * enterprise cold-switch uses to carry ONE cell's symbol index into a new partition version.
     * <p>
     * NON-VACUITY comes from the second cell being the one linked while the FIRST is left alone: the
     * by-timestamp form resolves both directories at cellKey 0, so a cell-blind implementation reads
     * cell 0's directory (and cell 0's row count and column tops from `_cv`) and writes the index of
     * the wrong cell -- or nothing at all, cell 0's new-version dir not existing. Asserting that the
     * files appear under cell 1's new dir, and that cell 0's dir did NOT gain any, distinguishes them.
     * <p>
     * WHAT THIS DOES NOT PIN: the inner helper's use of the cell's own row count, column tops and
     * column name-txns. Both cells here have no tops and name-txn 0, so passing cell 0's would produce
     * the same links; that resolution belongs to the private helper the parquet switch already drives.
     * Measured, not assumed -- mutating the inner cellKey to 0 leaves this test green.
     */
    @Test
    public void testIndexFilesLinkForTheNamedCellOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1','B',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0','A',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableWriter w = getWriter("c")) {
                final TxWriter tx = w.getTxWriter();
                final long day1 = tx.getPartitionTimestampByIndex(0);
                final IntList cells = cellsOfDay(w, day1);
                Assert.assertEquals("the day must have two cells or the per-cell claim is untestable",
                        2, cells.size());
                final int firstCell = cells.getQuick(0);
                final int targetCell = cells.getQuick(1);
                final long oldNameTxn = tx.getPartitionNameTxn(tx.getPartitionIndex(day1));
                final long newNameTxn = w.getTxn() + 1;

                final FilesFacade ff = configuration.getFilesFacade();
                final int indexFilesBefore = indexFileCount(ff, w, "c", day1, targetCell, newNameTxn);
                Assert.assertEquals("the new version dir must start empty or the link proves nothing",
                        0, indexFilesBefore);

                try (Path dir = new Path()) {
                    cellPartitionPath(dir, "c", day1, targetCell, newNameTxn, w);
                    Assert.assertEquals("could not create the target version dir",
                            0, ff.mkdirs(dir.slash(), configuration.getMkDirMode()));
                }
                w.linkPartitionIndexFiles(day1, targetCell, oldNameTxn, newNameTxn);

                Assert.assertTrue("the named cell's index files must be linked into the new version",
                        indexFileCount(ff, w, "c", day1, targetCell, newNameTxn) > 0);
                Assert.assertEquals("the sibling cell must be untouched -- a cell-blind link writes here",
                        0, indexFileCount(ff, w, "c", day1, firstCell, newNameTxn));
            }
        });
    }

    /**
     * {@code .k}/{@code .v} files present in one cell's version directory.
     */
    private int indexFileCount(FilesFacade ff, TableWriter w, String table, long dayTs, int cellKey, long nameTxn) {
        int found = 0;
        try (Path path = new Path()) {
            for (String suffix : new String[]{".k", ".v"}) {
                cellPartitionPath(path, table, dayTs, cellKey, nameTxn, w);
                path.concat("sym").put(suffix);
                if (ff.exists(path.$())) {
                    found++;
                }
            }
        }
        return found;
    }

    /**
     * {@code <root>/<table>/<day>/<cell>.<nameTxn>} -- the directory one cell of a day lives in.
     */
    private void cellPartitionPath(Path path, String table, long dayTs, int cellKey, long nameTxn, TableWriter w) {
        final StringSink segment = new StringSink();
        w.renderCellSegment(segment, cellKey);
        final TableToken tt = engine.verifyTableName(table);
        path.of(configuration.getDbRoot()).concat(tt);
        TableUtils.setPathForNativePartition(path, w.getTimestampType(), PartitionBy.DAY, dayTs, nameTxn, segment);
    }

    private IntList cellsOfDay(TableWriter w, long dayTs) {
        final TxWriter tx = w.getTxWriter();
        final IntList out = new IntList();
        for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
            if (tx.getPartitionTimestampByIndex(i) == dayTs) {
                out.add(tx.getPartitionCellKey(i));
            }
        }
        return out;
    }

    /**
     * Plants {@code data.parquet} plus its {@code _pm} sidecar inside ONE CELL's directory. The switch
     * needs both: it hard-links the data file, then the sidecar, and reports SWITCH_NO_PARQUET if the
     * sidecar is missing.
     */
    private boolean cellParquetExists(TableWriter w, String table, long dayTs, int cellKey) {
        final TxWriter tx = w.getTxWriter();
        long nameTxn = -1;
        for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
            if (tx.getPartitionTimestampByIndex(i) == dayTs && tx.getPartitionCellKey(i) == cellKey) {
                nameTxn = tx.getPartitionNameTxn(i);
                break;
            }
        }
        final StringSink segment = new StringSink();
        w.renderCellSegment(segment, cellKey);
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(engine.verifyTableName(table));
            TableUtils.setPathForParquetPartition(p, tx.getTimestampType(), PartitionBy.DAY, dayTs, nameTxn, segment);
            return configuration.getFilesFacade().exists(p.$());
        }
    }

    private void plantCellParquet(TableWriter w, String table, long dayTs, int cellKey) {
        final TxWriter tx = w.getTxWriter();
        long nameTxn = 0;
        boolean found = false;
        for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
            if (tx.getPartitionTimestampByIndex(i) == dayTs && tx.getPartitionCellKey(i) == cellKey) {
                // -1 is a legitimate name-txn (the initial sentinel), so track presence separately
                // rather than using the value as its own found-flag.
                nameTxn = tx.getPartitionNameTxn(i);
                found = true;
                break;
            }
        }
        Assert.assertTrue("cell " + cellKey + " must exist in the day", found);

        final StringSink segment = new StringSink();
        w.renderCellSegment(segment, cellKey);
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            final TableToken tt = engine.verifyTableName(table);
            p.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(p, tx.getTimestampType(), PartitionBy.DAY, dayTs, nameTxn, segment);
            final int dirLen = p.size();
            p.concat(TableUtils.PARQUET_PARTITION_NAME).$();
            Assert.assertTrue("plant cell data.parquet at " + p, ff.touch(p.$()));

            p.trimTo(dirLen);
            TableUtils.setPathForParquetPartitionMetadata(
                    p.of(configuration.getDbRoot()).concat(tt),
                    tx.getTimestampType(), PartitionBy.DAY, dayTs, nameTxn, segment
            );
            Assert.assertTrue("plant cell _pm sidecar at " + p, ff.touch(p.$()));
        }
    }

    private void plantPlainParquet(String table, long dayTs, long nameTxn, int tsType) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            final TableToken tt = engine.verifyTableName(table);
            p.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(p, tsType, PartitionBy.DAY, dayTs, nameTxn);
            p.concat(TableUtils.PARQUET_PARTITION_NAME).$();
            Assert.assertTrue("plant data.parquet", ff.touch(p.$()));
        }
    }
}
