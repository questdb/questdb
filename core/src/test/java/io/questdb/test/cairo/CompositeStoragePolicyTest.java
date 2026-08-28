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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * STORAGE POLICY (the {@code TO PARQUET} tier) against a composite table.
 * <p>
 * Storage policy is an ENTERPRISE feature with no OSS production caller, which is why none of the
 * methods it drives are exercised by OSS SQL. Its OSS-facing surface is four {@code TableWriter}
 * entry points, called in this order by {@code StoragePolicyWriterCommand} (verified against
 * questdb-enterprise {@code e87f5971d}):
 * <ol>
 *   <li>{@code preparePartitionForParquetConversion(ts)} -- TASK_SQUASH; force-squashes the partition
 *       and deletes any stale parquet file, returning the timestamp to generate for;</li>
 *   <li>the job then generates {@code data.parquet} into that partition's directory;</li>
 *   <li>{@code markPartitionParquetReady(ts)} -- flags the partition parquet-generated;</li>
 *   <li>{@code switchNativePartitionWithParquet(ts, size)} -- flips the partition to parquet format.</li>
 * </ol>
 * <b>Every one of them is keyed by PARTITION TIMESTAMP alone.</b> There is no cellKey in the
 * enterprise API at any point. On a composite table a timestamp does not identify a partition -- a day
 * is N cell partitions -- so each of these resolves through the cellKey-0 lookup
 * {@code txWriter.getPartitionIndex(partitionTimestamp)} and builds its paths with the cell-less
 * {@code setPathForNativePartition}. That is this branch's signature defect family exactly: a lookup
 * resolved BY TIMESTAMP whose result is applied per cell.
 * <p>
 * <b>MEASURED 2026-08-28, before the gate existed:</b> only the LAST of the four refused. The other
 * two OSS-reachable steps ran to completion on a composite table -- {@code prepare} force-squashed a
 * cellKey-0-resolved index, deleted a cell-less parquet path, and RETURNED THE TIMESTAMP, which tells
 * the policy job "this day is ready, generate parquet for it". So the sequence half-applied and only
 * failed at the end. The contract pinned here is that the refusal comes FIRST, at every entry point,
 * before any commit or squash -- a refusal thrown after a mutation is not a refusal.
 * <p>
 * The wording is "does not YET support": this is a deferral, not a ban. Composite already converts a
 * day to parquet per cell ({@code convertCompositePartitionNativeToParquet}); what is missing is
 * enterprise-side, where the job would have to generate one parquet file per cell and carry a cellKey
 * through an API that today has only a timestamp. That cannot be built or tested from this repository,
 * which is why this is a gate and not an implementation.
 * <p>
 * The plain twin is the control throughout: the same sequence must keep working there, or a
 * "composite refuses" assertion would pass simply because the sequence is broken for everyone.
 */
public class CompositeStoragePolicyTest extends AbstractCompositeTwinTest {

    /**
     * The composite table must refuse the storage-policy sequence at its FIRST entry point, with the
     * table left exactly as it was.
     * <p>
     * Asserting the refusal alone is not enough -- a refusal thrown after a force-squash has already
     * run is still a mutation. The partition inventory is captured before and compared after.
     */
    @Test(timeout = 120_000)
    public void testStoragePolicyRefusesBeforeMutatingAnything() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDaysTwoCells();

            final String inventoryBefore;
            final long day1Ts;
            try (TableWriter w = getWriter("c")) {
                inventoryBefore = inventory(w);
                day1Ts = w.getTxWriter().getPartitionTimestampByIndex(0);

                try {
                    w.preparePartitionForParquetConversion(day1Ts);
                    Assert.fail("the storage-policy squash step must be refused on a composite table:"
                            + " it resolves the partition by timestamp alone (cellKey 0) and"
                            + " force-squashes whatever that lookup lands on");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support STORAGE POLICY");
                    TestUtils.assertContains(e.getFlyweightMessage(), "STORAGE POLICY");
                }

                Assert.assertEquals(
                        "a refused storage-policy step must leave the partition inventory untouched",
                        inventoryBefore, inventory(w));
            }

            // The refusal must not have cost any rows either.
            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E0'");
        });
    }

    /**
     * The mark-ready step, refused on its own. It is reachable independently of the squash step (the
     * enterprise command re-enters per task type), so gating only the first entry point would leave
     * this one open.
     */
    @Test(timeout = 120_000)
    public void testMarkPartitionParquetReadyRefusedOnComposite() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDaysTwoCells();

            try (TableWriter w = getWriter("c")) {
                final TxWriter tx = w.getTxWriter();
                final long day1Ts = tx.getPartitionTimestampByIndex(0);
                final String inventoryBefore = inventory(w);

                try {
                    w.markPartitionParquetReady(day1Ts);
                    Assert.fail("the storage-policy mark-ready step must be refused on a composite table");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support STORAGE POLICY");
                    TestUtils.assertContains(e.getFlyweightMessage(), "STORAGE POLICY");
                }

                Assert.assertEquals("a refused mark-ready must not flag anything parquet-generated",
                        inventoryBefore, inventory(w));
            }
            assertTwinEqual("");
        });
    }

    /**
     * The switch step, already gated before this suite existed. Kept so all four entry points are
     * covered in one place, and so the gate's wording stays consistent with the other three.
     */
    @Test(timeout = 120_000)
    public void testSwitchNativePartitionWithParquetRefusedOnComposite() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDaysTwoCells();

            try (TableWriter w = getWriter("c")) {
                final long day1Ts = w.getTxWriter().getPartitionTimestampByIndex(0);
                final String inventoryBefore = inventory(w);
                try {
                    w.switchNativePartitionWithParquet(day1Ts, 0L);
                    Assert.fail("the storage-policy switch step must be refused on a composite table");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support STORAGE POLICY");
                }
                Assert.assertEquals("a refused switch must leave the inventory untouched",
                        inventoryBefore, inventory(w));
            }
            assertTwinEqual("");
        });
    }

    /**
     * The three by-timestamp READERS the policy job consults before it acts:
     * {@code getPartitionNameTxnByPartitionTimestamp}, {@code getPartitionRowCountByPartitionTimestamp}
     * and {@code getPartitionSquashCountByPartitionTimestamp}.
     * <p>
     * These are the SILENT half of the gap. They mutate nothing, so a gate on the write steps alone
     * leaves them returning cellKey 0's name-txn / row count / squash count as though it were "the
     * partition's" -- and the policy job compares exactly those against its own expectations to decide
     * whether its work is stale. A wrong-but-plausible answer there is what would make a future
     * enterprise implementation appear to work while acting on one arbitrary cell of the day.
     * <p>
     * Verified 2026-08-28 that all three {@code TableWriter} wrappers have ZERO callers in this
     * repository -- every OSS call site goes through {@code getTxFile()} / {@code txWriter} to the
     * TxReader methods instead. They exist solely for the enterprise storage-policy job, so refusing
     * them costs nothing here.
     */
    @Test(timeout = 120_000)
    public void testByTimestampReadersRefuseRatherThanAnswerForCellZero() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDaysTwoCells();

            try (TableWriter w = getWriter("c")) {
                final long day1Ts = w.getTxWriter().getPartitionTimestampByIndex(0);

                assertRefused("name-txn", () -> w.getPartitionNameTxnByPartitionTimestamp(day1Ts));
                assertRefused("row-count", () -> w.getPartitionRowCountByPartitionTimestamp(day1Ts));
                assertRefused("squash-count", () -> w.getPartitionSquashCountByPartitionTimestamp(day1Ts));
            }
            assertTwinEqual("");
        });
    }

    /**
     * The readers' CONTROL: on the plain twin all three must still answer, and answer correctly. A
     * blanket refusal would pass the test above just as well.
     */
    @Test(timeout = 120_000)
    public void testByTimestampReadersStillAnswerOnThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDaysTwoCells();

            try (TableWriter w = getWriter("p")) {
                final TxWriter tx = w.getTxWriter();
                final long day1Ts = tx.getPartitionTimestampByIndex(0);

                Assert.assertEquals("plain name-txn must still be readable",
                        tx.getPartitionNameTxn(0), w.getPartitionNameTxnByPartitionTimestamp(day1Ts));
                Assert.assertEquals("plain row count must still be readable and correct",
                        2L, w.getPartitionRowCountByPartitionTimestamp(day1Ts));
                Assert.assertEquals("plain squash count must still be readable",
                        tx.getPartitionSquashCount(0), w.getPartitionSquashCountByPartitionTimestamp(day1Ts));
            }
        });
    }

    /**
     * THE CONTROL. The same four-step sequence on the PLAIN twin must still complete: prepare returns
     * the timestamp, mark-ready accepts the planted file, and the switch reports success.
     * <p>
     * Without this, every assertion above would pass just as well if the sequence were broken for all
     * tables, which is the failure mode a gate is most likely to introduce.
     */
    @Test(timeout = 120_000)
    public void testPlainTwinStillCompletesTheSameSequence() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDaysTwoCells();

            try (TableWriter w = getWriter("p")) {
                final TxWriter tx = w.getTxWriter();
                final long day1Ts = tx.getPartitionTimestampByIndex(0);

                final long prepared = w.preparePartitionForParquetConversion(day1Ts);
                Assert.assertEquals("the plain twin must accept the squash step", day1Ts, prepared);

                plantEmptyDataParquet("p", day1Ts, tx.getPartitionNameTxn(0), tx.getTimestampType());
                Assert.assertTrue("the plain twin must accept the mark-ready step",
                        w.markPartitionParquetReady(day1Ts));
                Assert.assertTrue("day1 must now be flagged parquet-generated",
                        tx.isPartitionParquetGenerated(0));
            }
        });
    }

    /**
     * A DORMANT composite table -- dimensions declared but no cell ever routed -- takes the plain
     * paths everywhere else on this branch, so the storage-policy gate must leave it alone too.
     * <p>
     * This is the predicate check: a gate written on {@code dimCount > 0} instead of
     * {@code isRoutedComposite()} would refuse here, and would be a REGRESSION for a table that has
     * always been able to take these paths.
     */
    @Test(timeout = 120_000)
    public void testDormantCompositeTableIsNotRefused() throws Exception {
        assertMemoryLeak(() -> {
            // No rows at all, so nothing has been routed to a cell.
            execute("CREATE TABLE d (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            drainWalQueue();

            try (TableWriter w = getWriter("d")) {
                Assert.assertFalse("a table with no routed cell must not be treated as composite here",
                        w.isComposite());
                // Reaches the ordinary "partition does not exist" path rather than a composite refusal.
                try {
                    w.preparePartitionForParquetConversion(
                            io.questdb.cairo.MicrosTimestampDriver.floor("2023-01-01T00:00:00.000000Z"));
                } catch (CairoException e) {
                    Assert.assertFalse(
                            "a dormant composite table must not hit the composite storage-policy gate,"
                                    + " it takes the plain paths. Got: " + e.getFlyweightMessage(),
                            Chars.contains(e.getFlyweightMessage(), "composite partitioning"));
                }
            }
        });
    }

    private static void assertRefused(String step, Runnable call) {
        try {
            call.run();
            Assert.fail("the storage-policy '" + step + "' read must be refused on a composite table"
                    + " rather than silently answering for cellKey 0");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support STORAGE POLICY");
            TestUtils.assertContains(e.getFlyweightMessage(), step);
        }
    }

    /**
     * A stable, comparable rendering of every attached-partition entry: timestamp, cellKey, name-txn,
     * row count, squash count and the parquet flags.
     * <p>
     * Deliberately covers ALL cells, not just the day being acted on -- the damage a cellKey-0 lookup
     * does lands on a SIBLING cell, so an inventory of only the targeted entry would miss it.
     */
    private static String inventory(TableWriter w) {
        final TxWriter tx = w.getTxWriter();
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
            sb.append(tx.getPartitionTimestampByIndex(i)).append('/')
                    .append(tx.getPartitionCellKey(i)).append('/')
                    .append(tx.getPartitionNameTxn(i)).append('/')
                    .append(tx.getPartitionSize(i)).append('/')
                    .append(tx.getPartitionSquashCount(i)).append('/')
                    .append(tx.isPartitionParquet(i)).append('/')
                    .append(tx.isPartitionParquetGenerated(i)).append('\n');
        }
        return sb.toString();
    }

    private void plantEmptyDataParquet(String table, long partitionTimestamp, long partitionNameTxn, int tsType) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            final TableToken tt = engine.verifyTableName(table);
            p.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(p, tsType, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            p.concat(TableUtils.PARQUET_PARTITION_NAME).$();
            Assert.assertTrue("plant empty data.parquet", ff.touch(p.$()));
        }
    }

    /**
     * Two days, two cells each, so a cellKey-0 lookup has a SIBLING cell it can damage. A single-cell
     * day would make every by-timestamp lookup accidentally correct.
     */
    private void seedTwoDaysTwoCells() throws Exception {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                + "('2023-01-02T01:00:00.000000Z','E0',3.0),"
                + "('2023-01-02T02:00:00.000000Z','E1',4.0)");
        drainWalQueue();
        engine.releaseInactive();
    }
}
