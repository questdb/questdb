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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers {@link TableWriter#dropLocalPartitionData(long)}: drops the
 * local {@code data.parquet} while keeping the {@code _pm} sidecar, the
 * partition directory, and the partition's {@code TxReader} entry intact.
 */
public class TableWriterDropLocalParquetTest extends AbstractCairoTest {

    @Test
    public void testDropLocalParquetActivePartitionIsNoOp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "t";
            createParquetTable(tableName);

            // Active partition (2024-06-12) is native — CONVERT PARTITION leaves the
            // most recent partition alone. The method must early-return on the
            // active partition even when non-parquet, so the non-parquet guard is
            // never reached.
            final TableToken token = engine.verifyTableName(tableName);
            final long activeTs = findPartitionTimestamp(token, 2);

            try (TableWriter writer = getWriter(token)) {
                writer.dropLocalPartitionData(activeTs);
            }

            assertSql("id\tts\n3\t2024-06-12T00:00:00.000000Z\n",
                    "SELECT * FROM " + tableName + " WHERE ts = '2024-06-12'");
        });
    }

    @Test
    public void testDropLocalParquetHappyPath() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "t";
            createParquetTable(tableName);

            final TableToken token = engine.verifyTableName(tableName);
            final long targetTs = findPartitionTimestamp(token, 0); // 2024-06-10 — non-active parquet
            final long nameTxn = findPartitionNameTxn(token, targetTs);
            Assert.assertTrue("precondition: data.parquet must exist before the drop",
                    dataParquetExists(token, targetTs, nameTxn));
            Assert.assertTrue("precondition: _pm must exist before the drop",
                    pmExists(token, targetTs, nameTxn));

            try (TableWriter writer = getWriter(token)) {
                writer.dropLocalPartitionData(targetTs);
            }

            Assert.assertFalse("data.parquet must be gone after drop",
                    dataParquetExists(token, targetTs, nameTxn));
            Assert.assertTrue("_pm sidecar must remain after drop",
                    pmExists(token, targetTs, nameTxn));
            assertPartitionStillParquet(token, targetTs);
        });
    }

    @Test
    public void testDropLocalParquetIdempotent() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "t";
            createParquetTable(tableName);

            final TableToken token = engine.verifyTableName(tableName);
            final long targetTs = findPartitionTimestamp(token, 0);
            final long nameTxn = findPartitionNameTxn(token, targetTs);

            try (TableWriter writer = getWriter(token)) {
                writer.dropLocalPartitionData(targetTs);
                final long txnAfterFirstDrop = writer.getTxn();
                // Second call must not throw, must leave state unchanged, and
                // must not advance the transaction. The ff.exists guard in the
                // method is the contract: no-op drops write no txn entry.
                writer.dropLocalPartitionData(targetTs);
                Assert.assertEquals(
                        "no-op drop must not advance the transaction",
                        txnAfterFirstDrop,
                        writer.getTxn()
                );
            }

            Assert.assertFalse(dataParquetExists(token, targetTs, nameTxn));
            Assert.assertTrue(pmExists(token, targetTs, nameTxn));
            assertPartitionStillParquet(token, targetTs);
        });
    }

    @Test
    public void testDropLocalParquetOnNativePartitionThrows() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "t";
            execute("CREATE TABLE " + tableName + " (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (1, '2024-06-10T00:00:00.000000Z'),
                    (2, '2024-06-11T00:00:00.000000Z'),
                    (3, '2024-06-12T00:00:00.000000Z')
                    """);
            // Convert only the 11th to parquet; 2024-06-10 stays native (non-active).
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-11'");

            final TableToken token = engine.verifyTableName(tableName);
            final long nativeTs = findPartitionTimestamp(token, 0); // 2024-06-10 is native

            try (TableWriter writer = getWriter(token)) {
                try {
                    writer.dropLocalPartitionData(nativeTs);
                    Assert.fail("expected CairoException for non-parquet partition");
                } catch (CairoException e) {
                    Assert.assertTrue(
                            "unexpected message: " + e.getFlyweightMessage(),
                            e.getFlyweightMessage().toString().contains("cannot drop local parquet for non-parquet partition")
                    );
                }
            }

            // Native partition's data must remain readable.
            assertSql("id\tts\n1\t2024-06-10T00:00:00.000000Z\n",
                    "SELECT * FROM " + tableName + " WHERE ts = '2024-06-10'");
        });
    }

    @Test
    public void testDropLocalParquetUnknownTimestampIsNoOp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "t";
            createParquetTable(tableName);

            final TableToken token = engine.verifyTableName(tableName);
            final long knownTs = findPartitionTimestamp(token, 0);
            final long knownNameTxn = findPartitionNameTxn(token, knownTs);
            // +1 year, no matching partition
            final long unknownTs = knownTs + 365L * 24L * 3_600L * 1_000_000L;

            try (TableWriter writer = getWriter(token)) {
                writer.dropLocalPartitionData(unknownTs);
            }

            Assert.assertTrue("known parquet partition must be untouched",
                    dataParquetExists(token, knownTs, knownNameTxn));
        });
    }

    private void assertPartitionStillParquet(TableToken token, long partitionTs) {
        engine.releaseAllReaders();
        try (TableReader reader = engine.getReader(token)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().getPartitionTimestampByIndex(i) == partitionTs) {
                    Assert.assertEquals(
                            "partition must remain in parquet format after drop",
                            PartitionFormat.PARQUET,
                            reader.getTxFile().isPartitionParquet(i) ? PartitionFormat.PARQUET : PartitionFormat.NATIVE
                    );
                    return;
                }
            }
            Assert.fail("partition not found in TxReader after drop");
        }
    }

    private void createParquetTable(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                (1, '2024-06-10T00:00:00.000000Z'),
                (2, '2024-06-11T00:00:00.000000Z'),
                (3, '2024-06-12T00:00:00.000000Z')
                """);
        // The most recent partition (2024-06-12) stays native/active; earlier partitions become parquet.
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-06-12'");
    }

    private boolean dataParquetExists(TableToken token, long partitionTs, long nameTxn) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartition(p, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxn);
            return ff.exists(p.$());
        }
    }

    private long findPartitionNameTxn(TableToken token, long partitionTs) {
        try (TableReader reader = engine.getReader(token)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().getPartitionTimestampByIndex(i) == partitionTs) {
                    return reader.getTxFile().getPartitionNameTxn(i);
                }
            }
        }
        Assert.fail("partition not found for timestamp " + partitionTs);
        return -1;
    }

    private long findPartitionTimestamp(TableToken token, int partitionIndex) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
        }
    }

    private boolean pmExists(TableToken token, long partitionTs, long nameTxn) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartitionMetadata(p, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxn);
            return ff.exists(p.$());
        }
    }
}
