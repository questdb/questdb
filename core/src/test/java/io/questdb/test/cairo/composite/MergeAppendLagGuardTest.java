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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * The WAL LAG guard in {@code TableWriter.processWalCommit} and the in-order block fast append in
 * {@code TableWriter.tryFastAppendInOrderBlock} both refuse a merge-append table, because a commit that turns the
 * last partition COMPOSITE relocates a piece onto exactly the file rows those two mechanisms park data in.
 * <p>
 * Both guards ask {@code isMergeAppendTable()}, which is {@code walEnabled && flagOn && isPartitioned}, rather than
 * the raw {@code isO3PartitionMergeAppendEnabled()} flag. Narrowing to PARTITIONED tables is free, because two
 * independent things keep a non-partitioned table away from a composite partition:
 * <ol>
 *     <li>A WAL table is always partitioned. {@code SqlParser} refuses {@code PARTITION BY NONE ... WAL} and
 *     resolves the WAL-by-default setting to false for it; {@code SqlCompilerImpl.alterTableSetType} refuses to
 *     convert a non-partitioned table, and it is the only writer of the {@code _convert} marker;
 *     {@code CairoEngine.createTable} asserts the invariant at the engine's one creation chokepoint. Both guards
 *     sit on the WAL apply path, so the shape they would newly admit cannot be created in the first place.</li>
 *     <li>A non-partitioned table refuses out-of-order rows outright - {@code TableWriter.newRow} throws
 *     "cannot insert rows out of order to non-partitioned table" under {@code ROW_ACTION_NO_PARTITION}. Composite
 *     partitions are what an O3 commit leaves behind when it tiles a partition into pieces, so with no O3 commit
 *     there is nothing to make one.</li>
 * </ol>
 * Note that the first reason, not the second, is what the guards actually rest on, and that a non-partitioned table
 * DOES have a partition: {@code TxReader.initPartitionBy} seeds one at {@code DEFAULT_PARTITION_TIMESTAMP}, so
 * {@code getPartitionCount()} is 1 and {@code getPartitionIndex()} returns 0 for it. That matters because
 * {@code O3PartitionJob.processPartition} gates composite promotion on {@code isWalEnabled()} and
 * {@code compositeIndex > -1} WITHOUT an {@code isPartitioned} conjunct - both of which a non-partitioned WAL table
 * would satisfy. "There is no partition for a composite to exist on" is NOT what makes this safe.
 */
public class MergeAppendLagGuardTest extends AbstractCairoTest {

    /**
     * The live behaviour the guard exists for: a merge-append WAL table parks no LAG, not even on the FIRST commit
     * into an empty table. That first commit is the only one the {@code noLag} merge-append clause decides on its
     * own - from the second commit on, {@code isLastPartitionAppendBlocked()} already refuses the in-place append.
     */
    @Test
    public void testMergeAppendTableTakesNoLagFromTheFirstCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // In order, then out of order, then in order again - the shape that makes a table without merge-append
            // park one LAG row per transaction (see WalTableSqlTest#testSavedDataInTxnFile).
            execute("INSERT INTO x VALUES (1, '2022-02-24T01')");
            execute("INSERT INTO x VALUES (2, '2022-02-24T00')");
            execute("INSERT INTO x VALUES (3, '2022-02-24T02')");

            final TableToken token = engine.verifyTableName("x");
            final int timestampType;
            final int partitionBy;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                timestampType = m.getTimestampType();
                partitionBy = m.getPartitionBy();
            }

            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                for (int txn = 1; txn <= 3; txn++) {
                    runApplyOnce(token);
                    Assert.assertFalse("merge-append suspended the table at txn " + txn,
                            engine.getTableSequencerAPI().isSuspended(token));
                    txReader.unsafeLoadAll();
                    Assert.assertEquals("merge-append parked LAG rows at txn " + txn, 0, txReader.getLagRowCount());
                    Assert.assertEquals("merge-append parked LAG txns at txn " + txn, 0, txReader.getLagTxnCount());
                    // Every row is committed the moment it is applied, none of it withheld in a LAG.
                    Assert.assertEquals("rows withheld from the table at txn " + txn, txn, txReader.getRowCount());
                }
            }

            assertQuery("SELECT x FROM x")
                    .expectSize()
                    .returns("x\n2\n1\n3\n");
        });
    }

    /**
     * The same guard in {@code tryFastAppendInOrderBlock}: a merge-append table refuses the in-order block fast
     * append, so a multi-transaction block still lands through the unchanged O3 path, with no LAG and no data loss.
     */
    @Test
    public void testMergeAppendTableTakesNoLagOnBlockApply() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Several in-order transactions applied as one block - the shape tryFastAppendInOrderBlock exists for.
            for (int i = 0; i < 6; i++) {
                execute("INSERT INTO x SELECT x + " + (i * 100L) + " x," +
                        " timestamp_sequence('2022-02-24T0" + i + "', 1000000L) ts FROM long_sequence(100)");
            }
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse("block apply suspended the table", engine.getTableSequencerAPI().isSuspended(token));

            final int timestampType;
            final int partitionBy;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                timestampType = m.getTimestampType();
                partitionBy = m.getPartitionBy();
            }
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                txReader.unsafeLoadAll();
                Assert.assertEquals("block apply parked LAG rows", 0, txReader.getLagRowCount());
                Assert.assertEquals("block apply parked LAG txns", 0, txReader.getLagTxnCount());
                Assert.assertEquals(600, txReader.getRowCount());
            }

            assertQuery("SELECT count(*) c, min(ts) lo, max(ts) hi FROM x")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tlo\thi\n600\t2022-02-24T00:00:00.000000Z\t2022-02-24T05:01:39.000000Z\n");
        });
    }

    private void runApplyOnce(TableToken token) {
        engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl().setMaxBlockRowCount(1);
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
            walApplyJob.run();
        }
    }
}
