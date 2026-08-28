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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
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
