/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | | |  _ \
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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A column-type change whose parquet->native prepass converts partitions must publish the whole
 * ALTER with a single {@code _txn} write. The writer already carries the ALTER's seqTxn while the
 * prepass runs, so an intermediate {@code _txn} would bind the OLD {@code _meta} to the NEW seqTxn;
 * anything that adopts that cut (an adaptive epoch, a crash in a syncing mode, an ALTER that fails
 * after the prepass) then skips the ALTER and keeps the old column type.
 * <p>
 * The single publish is observable as exactly one table-txn increment across the ALTER. Under
 * adaptive the epoch must be cut at the ALTER's own seqTxn, after that commit, and the superseded
 * parquet directory must be reclaimed inline: {@code drainWalQueue()} runs no partition purge job,
 * so a directory that is gone afterwards was removed by the writer itself.
 */
public class ColumnTypeChangeParquetPrepassPublishTest extends AbstractCairoTest {

    @Test
    public void testParquetPrepassPublishesOnceUnderAdaptive() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            final TableToken token = createTableWithParquetPartition();
            final long txnBefore;
            try (TableReader reader = getReader(token)) {
                txnBefore = reader.getTxn();
            }
            final String parquetDir = parquetPartitionDir(token);
            Assert.assertTrue("parquet source dir must exist before the ALTER", configuration.getFilesFacade().exists(Path.getThreadLocal(parquetDir).$()));

            execute("ALTER TABLE x ALTER COLUMN v TYPE DECIMAL(18,4)");
            drainWalQueue();

            assertConverted(token, txnBefore);

            // The epoch is cut on the ALTER's single commit: same table txn, same seqTxn as the applied ALTER.
            final long seqTxn;
            try (TableReader reader = getReader(token)) {
                seqTxn = reader.getTxFile().getSeqTxn();
            }
            try (Path path = new Path(); SnapshotMarker marker = new SnapshotMarker(configuration)) {
                marker.of(path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME).$());
                Assert.assertTrue("an adaptive epoch must exist after the ALTER", marker.tryLoad());
                Assert.assertEquals("epoch must be cut at the ALTER's seqTxn", seqTxn, marker.getEpochSeqTxn());
                Assert.assertEquals("epoch must be cut on the ALTER's single commit", txnBefore + 1, marker.getEpochTxn());
            }
            Assert.assertFalse(
                    "the post-commit epoch moves the pin, so the parquet source dir is reclaimed inline",
                    configuration.getFilesFacade().exists(Path.getThreadLocal(parquetDir).$())
            );
        });
    }

    @Test
    public void testParquetPrepassPublishesOnceUnderDefaultCommitMode() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTableWithParquetPartition();
            final long txnBefore;
            try (TableReader reader = getReader(token)) {
                txnBefore = reader.getTxn();
            }
            final String parquetDir = parquetPartitionDir(token);
            Assert.assertTrue("parquet source dir must exist before the ALTER", configuration.getFilesFacade().exists(Path.getThreadLocal(parquetDir).$()));

            execute("ALTER TABLE x ALTER COLUMN v TYPE DECIMAL(18,4)");
            drainWalQueue();

            assertConverted(token, txnBefore);
            Assert.assertFalse(
                    "no reader pins the old version, so the parquet source dir is reclaimed inline",
                    configuration.getFilesFacade().exists(Path.getThreadLocal(parquetDir).$())
            );
        });
    }

    private void assertConverted(TableToken token, long txnBefore) throws Exception {
        try (TableReader reader = getReader(token)) {
            Assert.assertEquals(
                    "the prepass and the metadata change must land in ONE _txn write",
                    txnBefore + 1,
                    reader.getTxn()
            );
        }
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        assertQuery("SELECT type FROM table_columns('x') WHERE \"column\" = 'v'").noRandomAccess().returns("type\nDECIMAL(18,4)\n");
        assertQuery("SELECT v FROM x ORDER BY ts").expectSize().returns("v\n1.2500\n2.5000\n");
        assertParquetPartitionCount(0);
    }

    private void assertParquetPartitionCount(int expected) throws Exception {
        assertQuery("SELECT count() AS count FROM table_partitions('x') WHERE isParquet")
                .noRandomAccess().expectSize().returns("count\n" + expected + "\n");
    }

    private TableToken createTableWithParquetPartition() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO x VALUES
                    ('2024-01-01T00:00:00.000000Z', 1.25),
                    ('2024-01-02T00:00:00.000000Z', 2.5)
                """);
        drainWalQueue();
        // The first day is not the active partition, so the conversion leaves a real parquet partition.
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        assertParquetPartitionCount(1);
        return engine.verifyTableName("x");
    }

    private static String parquetPartitionDir(TableToken token) {
        try (TableReader reader = getReader(token)) {
            final int partitionIndex = 0;
            Assert.assertTrue(reader.getTxFile().isPartitionParquet(partitionIndex));
            try (Path path = new Path()) {
                TableUtils.setPathForParquetPartition(
                        path.of(configuration.getDbRoot()).concat(token),
                        reader.getMetadata().getTimestampType(),
                        reader.getMetadata().getPartitionBy(),
                        reader.getTxFile().getPartitionTimestampByIndex(partitionIndex),
                        reader.getTxFile().getPartitionNameTxn(partitionIndex)
                );
                // setPathForParquetPartition names the data file; the assertion targets its directory.
                return path.parent().toString();
            }
        }
    }
}
