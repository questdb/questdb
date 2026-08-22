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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ParquetPartitionCompactionCommand;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end coverage for the idle-triggered standalone Parquet partition rewrite:
 * {@link TableWriter#compactParquetPartition(long)} and its {@link ParquetPartitionCompactionCommand}
 * async-command wrapper.
 * <p>
 * Each test accumulates dead row-group bytes in a Parquet partition via repeated in-place O3
 * updates, kept below the automatic rewrite threshold (ratio/max-bytes disabled, row group count
 * kept above 1) so the normal O3-commit path never rewrites it on its own. It then drives
 * {@code compactParquetPartition} through the async-command dispatch used by
 * {@code CairoEngine#getWriterOrPublishCommand} -- once with an idle writer (direct apply) and
 * once with a busy writer (queued onto the writer's own command queue, applied later via
 * {@link TableWriter#tick()}) -- and asserts the dead bytes are gone and every row still reads back
 * correctly.
 */
public class ParquetPartitionCompactionTest extends AbstractCairoTest {

    @Test
    public void testCompactParquetPartitionBusyWriterAppliesFromCommandQueue() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();

        assertMemoryLeak(() -> {
            createTableWithDeadRowGroupBytes("y");
            final TableToken tableToken = engine.verifyTableName("y");
            final long partitionTimestamp = findParquetPartitionTimestamp(tableToken);

            assertUnusedBytesPositive(tableToken);

            final ParquetPartitionCompactionCommand command =
                    new ParquetPartitionCompactionCommand(tableToken, tableToken.getTableId(), partitionTimestamp);

            // Hold the writer busy on this thread so a publisher from another thread must take
            // the publish/queue path instead of applying directly.
            try (TableWriter ownerWriter = engine.getWriter(tableToken, "owner")) {
                final Thread publisher = new Thread(() -> {
                    try {
                        TableWriter w = engine.getWriterOrPublishCommand(tableToken, command);
                        Assert.assertNull("writer was busy, command should have been queued, not applied directly", w);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                publisher.start();
                publisher.join();

                // Drain and apply the queued command on the writer's own thread, same as a
                // real writer would between WAL-apply batches.
                ownerWriter.tick();
            }

            assertUnusedBytesZero(tableToken);
            assertQuery("SELECT count() FROM y")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n16\n");
            assertQuery("SELECT a, ts FROM y ORDER BY ts, a")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(
                            """
                                    a\tts
                                    1\t2020-01-01T00:00:00.000000Z
                                    2\t2020-01-01T01:00:00.000000Z
                                    101\t2020-01-01T01:30:00.000000Z
                                    3\t2020-01-01T02:00:00.000000Z
                                    102\t2020-01-01T02:30:00.000000Z
                                    4\t2020-01-01T03:00:00.000000Z
                                    103\t2020-01-01T03:30:00.000000Z
                                    5\t2020-01-01T04:00:00.000000Z
                                    6\t2020-01-01T05:00:00.000000Z
                                    7\t2020-01-01T06:00:00.000000Z
                                    8\t2020-01-01T07:00:00.000000Z
                                    9\t2020-01-01T08:00:00.000000Z
                                    10\t2020-01-01T09:00:00.000000Z
                                    11\t2020-01-01T10:00:00.000000Z
                                    12\t2020-01-01T11:00:00.000000Z
                                    99\t2020-01-02T00:00:00.000000Z
                                    """
                    );
        });
    }

    @Test
    public void testCompactParquetPartitionIdleWriterAppliesDirectly() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();

        assertMemoryLeak(() -> {
            createTableWithDeadRowGroupBytes("x");
            final TableToken tableToken = engine.verifyTableName("x");
            final long partitionTimestamp = findParquetPartitionTimestamp(tableToken);

            assertUnusedBytesPositive(tableToken);

            final ParquetPartitionCompactionCommand command =
                    new ParquetPartitionCompactionCommand(tableToken, tableToken.getTableId(), partitionTimestamp);
            command.setCommandCorrelationId(1L);

            try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
                Assert.assertNotNull("writer was idle, should have been handed back directly", writer);
                command.apply(writer, true);
            }

            assertUnusedBytesZero(tableToken);
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n16\n");
            assertQuery("SELECT a, ts FROM x ORDER BY ts, a")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(
                            """
                                    a\tts
                                    1\t2020-01-01T00:00:00.000000Z
                                    2\t2020-01-01T01:00:00.000000Z
                                    101\t2020-01-01T01:30:00.000000Z
                                    3\t2020-01-01T02:00:00.000000Z
                                    102\t2020-01-01T02:30:00.000000Z
                                    4\t2020-01-01T03:00:00.000000Z
                                    103\t2020-01-01T03:30:00.000000Z
                                    5\t2020-01-01T04:00:00.000000Z
                                    6\t2020-01-01T05:00:00.000000Z
                                    7\t2020-01-01T06:00:00.000000Z
                                    8\t2020-01-01T07:00:00.000000Z
                                    9\t2020-01-01T08:00:00.000000Z
                                    10\t2020-01-01T09:00:00.000000Z
                                    11\t2020-01-01T10:00:00.000000Z
                                    12\t2020-01-01T11:00:00.000000Z
                                    99\t2020-01-02T00:00:00.000000Z
                                    """
                    );
        });
    }

    private void assertUnusedBytesPositive(TableToken tableToken) throws Exception {
        try (TableReader reader = engine.getReader(tableToken)) {
            int parquetIdx = findParquetPartitionIndex(reader);
            Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
            reader.openPartition(parquetIdx);
            long unusedBytes = reader.getAndInitParquetPartitionDecoder(parquetIdx).metadata().getUnusedBytes();
            Assert.assertTrue(
                    "expected dead row-group bytes to have accumulated below the auto-rewrite threshold, got " + unusedBytes,
                    unusedBytes > 0
            );
        }
    }

    private void assertUnusedBytesZero(TableToken tableToken) throws Exception {
        try (TableReader reader = engine.getReader(tableToken)) {
            int parquetIdx = findParquetPartitionIndex(reader);
            Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
            reader.openPartition(parquetIdx);
            long unusedBytes = reader.getAndInitParquetPartitionDecoder(parquetIdx).metadata().getUnusedBytes();
            Assert.assertEquals("compaction should have dropped every dead row group", 0, unusedBytes);
        }
    }

    /**
     * Builds a Parquet-format partition with 3 row groups (row group size 4, 12 rows), then
     * performs 3 separate O3 (out-of-order) inserts into it, each an in-place update that appends
     * a merged row group and leaves the row group it replaced as dead bytes. With the auto-rewrite
     * ratio/max-bytes thresholds disabled and row group count staying above 1 (no schema change,
     * no dedup keys), none of these merges triggers an automatic rewrite.
     */
    private void createTableWithDeadRowGroupBytes(String tableName) throws Exception {
        execute(
                "CREATE TABLE " + tableName + " (a INT, ts TIMESTAMP)\n" +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL"
        );
        execute(
                "INSERT INTO " + tableName + "(a, ts) VALUES" +
                        "(1,  '2020-01-01T00:00:00.000Z')," +
                        "(2,  '2020-01-01T01:00:00.000Z')," +
                        "(3,  '2020-01-01T02:00:00.000Z')," +
                        "(4,  '2020-01-01T03:00:00.000Z')," +
                        "(5,  '2020-01-01T04:00:00.000Z')," +
                        "(6,  '2020-01-01T05:00:00.000Z')," +
                        "(7,  '2020-01-01T06:00:00.000Z')," +
                        "(8,  '2020-01-01T07:00:00.000Z')," +
                        "(9,  '2020-01-01T08:00:00.000Z')," +
                        "(10, '2020-01-01T09:00:00.000Z')," +
                        "(11, '2020-01-01T10:00:00.000Z')," +
                        "(12, '2020-01-01T11:00:00.000Z')"
        );
        // Push the table's max timestamp past 2020-01-01 so it is no longer the active
        // partition -- required for CONVERT PARTITION TO PARQUET, and so the O3 inserts below
        // are genuinely out-of-order relative to the table, not plain appends.
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
        drainWalQueue();

        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
        drainWalQueue();

        execute("INSERT INTO " + tableName + "(a, ts) VALUES (101, '2020-01-01T01:30:00.000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (102, '2020-01-01T02:30:00.000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (103, '2020-01-01T03:30:00.000Z')");
        drainWalQueue();
    }

    private int findParquetPartitionIndex(TableReader reader) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                return i;
            }
        }
        return -1;
    }

    private long findParquetPartitionTimestamp(TableToken tableToken) throws Exception {
        try (TableReader reader = engine.getReader(tableToken)) {
            int parquetIdx = findParquetPartitionIndex(reader);
            Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
            return reader.getPartitionTimestampByIndex(parquetIdx);
        }
    }

    private void setUpSmallRowGroupsNoAutoRewrite() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }
}
