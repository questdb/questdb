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
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end coverage for the idle-triggered Parquet partition compaction: {@link PartitionCompactionScanJob}
 * copies the live row groups off a reader snapshot into a staging directory, carries the index files over,
 * and publishes a swap that {@link TableWriter#swapCompactedParquetPartition} applies metadata-only - the
 * writer is never held for the copy.
 * <p>
 * Each test accumulates dead row-group bytes in a Parquet partition via repeated in-place O3 updates, kept
 * below the automatic rewrite threshold (ratio/max-bytes disabled, row group count kept above 1) so the
 * normal O3-commit path never rewrites it on its own. The partition's symbol column is INDEXed, so every
 * swap also has index files to carry over and an index query to answer afterwards.
 */
public class ParquetPartitionCompactionTest extends AbstractCairoTest {

    /**
     * A writer held by another thread cannot apply the swap directly: the job builds the compacted
     * partition anyway - the copy needs no writer - and queues the swap onto the writer's own command
     * queue, where the next {@link TableWriter#tick()} applies it. Between the two the staged directory
     * waits beside the live one and the live partition is untouched.
     */
    @Test
    public void testBusyWriterAppliesTheSwapFromItsCommandQueue() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createTableWithDeadRowGroupBytes("y", true);
            final TableToken tableToken = engine.verifyTableName("y");
            assertUnusedBytesPositive(tableToken);
            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            final String stagingDir = stagingDir(tableToken);

            try (TableWriter ownerWriter = engine.getWriter(tableToken, "owner")) {
                runSweepOnAnotherThread();

                Assert.assertTrue("the build should have staged a compacted copy beside the live partition", dirExists(stagingDir));
                Assert.assertEquals("a busy writer must not have been swapped by another thread", nameTxnBefore, parquetPartitionNameTxn(tableToken));

                // Drain and apply the queued swap on the writer's own thread, same as a real writer would
                // between WAL-apply batches.
                ownerWriter.tick();
            }

            Assert.assertFalse("the swap should have renamed the staging directory in", dirExists(stagingDir));
            Assert.assertNotEquals("the queued swap did not land", nameTxnBefore, parquetPartitionNameTxn(tableToken));
            assertUnusedBytesZero(tableToken);
            assertDataIntact("y");
        });
    }

    /**
     * The counterpart of the skip test: once the idle timeout has passed since the last write, the same
     * partition IS a candidate and one sweep reclaims its dead bytes. The job's clock is moved two hours
     * ahead of the file's modification time rather than waiting. The writer is idle, so the job applies
     * the swap itself.
     */
    @Test
    public void testIdleSweepRewritesAPartitionIdleForLongerThanTheTimeout() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createTableWithDeadRowGroupBytes("w", true);
            final TableToken tableToken = engine.verifyTableName("w");
            assertUnusedBytesPositive(tableToken);
            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            final String stagingDir = stagingDir(tableToken);

            try (PartitionCompactionScanJob job = newSweepPastTheIdleTimeout()) {
                job.run();
            }
            engine.releaseInactive();

            Assert.assertNotEquals("the sweep left an idle parquet partition with dead bytes alone", nameTxnBefore, parquetPartitionNameTxn(tableToken));
            Assert.assertFalse("the staging directory should have been renamed in", dirExists(stagingDir));
            assertUnusedBytesZero(tableToken);
            assertDataIntact("w");
        });
    }

    /**
     * The background sweep's parquet branch picks up IDLE partitions. Its {@code _txn}-only gate - a recency
     * check on the partition's own TIMESTAMP bound - cannot tell idle from old: yesterday's partition has
     * old timestamps no matter how recently late-arriving data landed in it. The write-recency check on the
     * {@code .parquet} file's modification time is what tells them apart, the way the composite branch
     * consults {@code PartitionGeometry.getLastWriteMicros}. Without it the rewrite is recurring rather than
     * one-off: every in-place O3 update leaves dead bytes, so the next sweep rewrites the whole partition
     * again - copying every live row group to reclaim whatever the last small update abandoned.
     */
    @Test
    public void testIdleSweepSkipsAPartitionWrittenToWithinTheIdleTimeout() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createTableWithDeadRowGroupBytes("z", true);
            final TableToken tableToken = engine.verifyTableName("z");
            assertUnusedBytesPositive(tableToken);

            // The fixture's O3 updates landed moments ago, so the partition is anything but idle.
            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            engine.releaseInactive();

            Assert.assertEquals(
                    "the sweep rewrote a parquet partition written to well inside the idle timeout;" +
                            " a partition that keeps taking late data is rewritten in full on every pass",
                    nameTxnBefore,
                    parquetPartitionNameTxn(tableToken)
            );
            assertUnusedBytesPositive(tableToken);
        });
    }

    /**
     * A write that lands between the snapshot and the swap makes the staged copy describe a partition that
     * no longer exists. The swap sees the source's parquet file size move - an in-place O3 update appends
     * to it - rejects the staged directory, deletes it and leaves the live partition, new row included,
     * alone; the next sweep starts over from a fresh snapshot.
     */
    @Test
    public void testSwapDiscardsAStagedCopyOfAPartitionWrittenToSinceTheSnapshot() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            // Non-WAL, so the row written mid-flight below goes straight through the held writer.
            createTableWithDeadRowGroupBytes("v", false);
            final TableToken tableToken = engine.verifyTableName("v");
            assertUnusedBytesPositive(tableToken);
            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            final String stagingDir = stagingDir(tableToken);

            try (TableWriter ownerWriter = engine.getWriter(tableToken, "owner")) {
                runSweepOnAnotherThread();
                Assert.assertTrue("the build should have staged a compacted copy", dirExists(stagingDir));

                // The write the snapshot never saw: an in-place O3 update into the parquet partition.
                final TableWriter.Row row = ownerWriter.newRow(MicrosFormatUtils.parseTimestamp("2020-01-01T04:30:00.000000Z"));
                row.putInt(0, 104);
                row.putSym(1, "k2");
                row.append();
                ownerWriter.commit();

                // Applies the queued swap, which must now reject the staged directory.
                ownerWriter.tick();
            }

            Assert.assertFalse("a stale staging directory must be deleted by the swap that rejects it", dirExists(stagingDir));
            Assert.assertEquals("a stale swap must not replace the live partition", nameTxnBefore, parquetPartitionNameTxn(tableToken));
            assertQuery("SELECT count() FROM v")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n17\n");
            assertQuery("SELECT a FROM v WHERE ts = '2020-01-01T04:30:00.000000Z'")
                    .noLeakCheck()
                    .returns("a\n104\n");
        });
    }

    /**
     * The writer's stray-directory purge at open applies the swap's own staleness test to a parquet
     * staging directory: one whose source generation - {@code nameTxn} and parquet file size in its name -
     * still matches the live partition is a build in flight and stays; one whose generation the partition
     * no longer carries can never be swapped in and goes.
     */
    @Test
    public void testWriterOpenPurgesAbandonedStagingDirectoryButKeepsAnInFlightOne() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();

        assertMemoryLeak(() -> {
            createTableWithDeadRowGroupBytes("u", true);
            final TableToken tableToken = engine.verifyTableName("u");
            final String inFlightDir = stagingDir(tableToken);
            final String abandonedDir = stagingDir(tableToken, -1);

            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                for (String dir : new String[]{inFlightDir, abandonedDir}) {
                    TableUtils.createDirsOrFail(ff, path.of(dir).slash(), configuration.getMkDirMode());
                    Assert.assertTrue(ff.touch(path.of(dir).concat(TableUtils.PARQUET_PARTITION_NAME).$()));
                }
            }

            // The stray-partition-dir purge runs when a writer opens the table.
            engine.releaseInactive();
            try (TableWriter ignore = engine.getWriter(tableToken, "test")) {
                Assert.assertNotNull(ignore);
            }

            Assert.assertFalse("abandoned staging directory survived the purge", dirExists(abandonedDir));
            Assert.assertTrue("in-flight staging directory was purged", dirExists(inFlightDir));

            // Leave nothing behind for the suite's own checks.
            try (Path path = new Path()) {
                ff.rmdir(path.of(inFlightDir), false);
            }
        });
    }

    private static boolean dirExists(String dir) {
        try (Path path = new Path()) {
            return configuration.getFilesFacade().exists(path.of(dir).$());
        }
    }

    private static PartitionCompactionScanJob newSweepPastTheIdleTimeout() {
        final Clock twoHoursAhead = () -> configuration.getMicrosecondClock().getTicks() + 2 * Micros.HOUR_MICROS;
        return new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), twoHoursAhead);
    }

    /**
     * One sweep from a thread of its own, with the clock past the idle timeout, so a writer the test
     * thread holds is busy from the job's point of view and the swap goes to the command queue.
     */
    private static void runSweepOnAnotherThread() throws InterruptedException {
        final Throwable[] failure = new Throwable[1];
        final Thread sweeper = new Thread(() -> {
            try (PartitionCompactionScanJob job = newSweepPastTheIdleTimeout()) {
                job.run();
            } catch (Throwable e) {
                failure[0] = e;
            } finally {
                // What WorkerPool's worker-halt cleaners do for the compaction pool's own thread: the
                // parquet copy runs in a per-thread native context that would otherwise outlive this thread.
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                Path.clearThreadLocals();
            }
        });
        sweeper.start();
        sweeper.join();
        if (failure[0] != null) {
            throw new AssertionError("the sweep failed on its own thread", failure[0]);
        }
    }

    private void assertDataIntact(String tableName) throws Exception {
        assertQuery("SELECT count() FROM " + tableName)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n16\n");
        // Through the index carried over into the swapped-in directory: the odd values of a.
        assertQuery("SELECT count() FROM " + tableName + " WHERE s = 'k1'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n9\n");
        assertQuery("SELECT a, s, ts FROM " + tableName + " ORDER BY ts, a")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(
                        """
                                a\ts\tts
                                1\tk1\t2020-01-01T00:00:00.000000Z
                                2\tk2\t2020-01-01T01:00:00.000000Z
                                101\tk1\t2020-01-01T01:30:00.000000Z
                                3\tk1\t2020-01-01T02:00:00.000000Z
                                102\tk2\t2020-01-01T02:30:00.000000Z
                                4\tk2\t2020-01-01T03:00:00.000000Z
                                103\tk1\t2020-01-01T03:30:00.000000Z
                                5\tk1\t2020-01-01T04:00:00.000000Z
                                6\tk2\t2020-01-01T05:00:00.000000Z
                                7\tk1\t2020-01-01T06:00:00.000000Z
                                8\tk2\t2020-01-01T07:00:00.000000Z
                                9\tk1\t2020-01-01T08:00:00.000000Z
                                10\tk2\t2020-01-01T09:00:00.000000Z
                                11\tk1\t2020-01-01T10:00:00.000000Z
                                12\tk2\t2020-01-01T11:00:00.000000Z
                                99\tk1\t2020-01-02T00:00:00.000000Z
                                """
                );
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
     * no dedup keys), none of these merges triggers an automatic rewrite. The symbol column is
     * INDEXed: odd values of {@code a} carry {@code k1}, even ones {@code k2}.
     */
    private void createTableWithDeadRowGroupBytes(String tableName, boolean isWal) throws Exception {
        execute(
                "CREATE TABLE " + tableName + " (a INT, s SYMBOL INDEX, ts TIMESTAMP)\n" +
                        "TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL")
        );
        execute(
                "INSERT INTO " + tableName + "(a, s, ts) VALUES" +
                        "(1,  'k1', '2020-01-01T00:00:00.000Z')," +
                        "(2,  'k2', '2020-01-01T01:00:00.000Z')," +
                        "(3,  'k1', '2020-01-01T02:00:00.000Z')," +
                        "(4,  'k2', '2020-01-01T03:00:00.000Z')," +
                        "(5,  'k1', '2020-01-01T04:00:00.000Z')," +
                        "(6,  'k2', '2020-01-01T05:00:00.000Z')," +
                        "(7,  'k1', '2020-01-01T06:00:00.000Z')," +
                        "(8,  'k2', '2020-01-01T07:00:00.000Z')," +
                        "(9,  'k1', '2020-01-01T08:00:00.000Z')," +
                        "(10, 'k2', '2020-01-01T09:00:00.000Z')," +
                        "(11, 'k1', '2020-01-01T10:00:00.000Z')," +
                        "(12, 'k2', '2020-01-01T11:00:00.000Z')"
        );
        // Push the table's max timestamp past 2020-01-01 so it is no longer the active
        // partition -- required for CONVERT PARTITION TO PARQUET, and so the O3 inserts below
        // are genuinely out-of-order relative to the table, not plain appends.
        execute("INSERT INTO " + tableName + "(a, s, ts) VALUES (99, 'k1', '2020-01-02T00:00:00.000Z')");
        drainWalQueue();

        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
        drainWalQueue();

        execute("INSERT INTO " + tableName + "(a, s, ts) VALUES (101, 'k1', '2020-01-01T01:30:00.000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + "(a, s, ts) VALUES (102, 'k2', '2020-01-01T02:30:00.000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + "(a, s, ts) VALUES (103, 'k1', '2020-01-01T03:30:00.000Z')");
        drainWalQueue();
        engine.releaseInactive();
    }

    private int findParquetPartitionIndex(TableReader reader) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                return i;
            }
        }
        return -1;
    }

    private long parquetPartitionNameTxn(TableToken tableToken) {
        try (TableReader reader = engine.getReader(tableToken)) {
            final int parquetIdx = findParquetPartitionIndex(reader);
            Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
            return reader.getTxFile().getPartitionNameTxn(parquetIdx);
        }
    }

    private void setUpSmallRowGroupsNoAutoRewrite() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    /** The directory the sweep stages the parquet partition's compacted copy into, for its live generation. */
    private String stagingDir(TableToken tableToken) {
        return stagingDir(tableToken, 0);
    }

    /**
     * Same, with the parquet file size in the name offset by {@code fileSizeDelta} - a non-zero delta
     * names a generation the live partition does not carry.
     */
    private String stagingDir(TableToken tableToken, long fileSizeDelta) {
        try (TableReader reader = engine.getReader(tableToken); Path path = new Path()) {
            final int parquetIdx = findParquetPartitionIndex(reader);
            Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
            path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getTxFile().getPartitionTimestampByIndex(parquetIdx),
                    reader.getTxFile().getPartitionNameTxn(parquetIdx)
            );
            path.put(TableUtils.COMPACTING_DIR_MARKER).put(reader.getTxFile().getPartitionParquetFileSize(parquetIdx) + fileSizeDelta);
            return path.toString();
        }
    }
}
