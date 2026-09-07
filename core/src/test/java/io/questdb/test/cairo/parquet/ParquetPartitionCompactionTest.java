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
import io.questdb.cairo.ColumnType;
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
import io.questdb.test.tools.TestUtils;
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
     * The schema check compares the file's stored column type against the table's EXACTLY, so a type whose
     * {@code _pm} descriptor did not round-trip byte for byte would read as a permanent schema change and
     * the sweep would rewrite the partition once per interval for the rest of the table's life. This walks
     * every column kind a partition can hold - fixed, var-size, symbol, geohash of each width, decimal,
     * array, uuid, long256, ipv4, binary, both timestamp units - through a conversion nothing has altered
     * since, and asserts the sweep finds nothing to do.
     */
    @Test
    public void testIdleSweepLeavesAFreshlyConvertedPartitionOfEveryColumnTypeAlone() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createAllColumnTypesParquetPartition();

            final TableToken tableToken = engine.verifyTableName("t");
            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            runSweepPastTheIdleTimeout();

            Assert.assertEquals(
                    "the sweep rewrote a parquet partition nothing has changed since it was converted;" +
                            " some column type does not round-trip through the _pm descriptor",
                    nameTxnBefore,
                    parquetPartitionNameTxn(tableToken)
            );
        });
    }

    /**
     * The clearing half of {@link #testIdleSweepLeavesAFreshlyConvertedPartitionOfEveryColumnTypeAlone}:
     * the re-encode has to reproduce every column kind under exactly the type the table names, or the
     * schema check it just satisfied would report the partition stale again on the very next pass. One
     * DROP over the all-types fixture, then two sweeps: the first rewrites, the second must find nothing,
     * and the rows must survive the round trip.
     */
    @Test
    public void testIdleSweepRewriteOfEveryColumnTypeSettlesAfterOnePass() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createAllColumnTypesParquetPartition();
            execute("ALTER TABLE t DROP COLUMN c_long");
            drainWalQueue();
            // Read off the parquet partition as it stands, to compare the re-encoded rows against.
            execute("CREATE TABLE t_expected AS (SELECT * FROM t) TIMESTAMP(ts) PARTITION BY DAY");
            engine.releaseInactive();

            final TableToken tableToken = engine.verifyTableName("t");
            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            runSweepPastTheIdleTimeout();
            final long nameTxnAfter = parquetPartitionNameTxn(tableToken);
            Assert.assertNotEquals("the sweep left a partition still carrying a dropped column alone", nameTxnBefore, nameTxnAfter);

            runSweepPastTheIdleTimeout();
            Assert.assertEquals(
                    "the re-encode did not reproduce some column's type exactly, so the sweep rewrites the" +
                            " partition on every pass",
                    nameTxnAfter,
                    parquetPartitionNameTxn(tableToken)
            );
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT * FROM t_expected", "SELECT * FROM t", LOG);
        });
    }

    /**
     * The ALTER COLUMN TYPE twin of {@link #testIdleSweepRewritesAParquetPartitionAfterDropColumn}: the file
     * keeps the old physical type and every read pays a lazy per-row cast for it. One sweep re-encodes the
     * column to the table's current type, and the pass after that finds nothing to do.
     */
    @Test
    public void testIdleSweepRewritesAParquetPartitionAfterAlterColumnType() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createCleanParquetPartition("ac");
            final TableToken tableToken = engine.verifyTableName("ac");
            Assert.assertEquals(ColumnType.INT, ColumnType.tagOf(parquetColumnType(tableToken, "a")));

            execute("ALTER TABLE ac ALTER COLUMN a TYPE LONG");
            drainWalQueue();
            engine.releaseInactive();

            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            runSweepPastTheIdleTimeout();

            Assert.assertNotEquals("the sweep left a parquet partition holding the pre-ALTER type alone", nameTxnBefore, parquetPartitionNameTxn(tableToken));
            Assert.assertEquals("the column was not re-encoded to the table's current type", ColumnType.LONG, ColumnType.tagOf(parquetColumnType(tableToken, "a")));

            final long nameTxnAfter = parquetPartitionNameTxn(tableToken);
            runSweepPastTheIdleTimeout();
            Assert.assertEquals("the schema trigger did not clear, so the sweep rewrites the partition every interval", nameTxnAfter, parquetPartitionNameTxn(tableToken));
            assertCleanParquetDataIntact("ac");
        });
    }

    /**
     * A DROP COLUMN leaves the dropped column's pages in every row group of an already-converted parquet
     * partition, and changes neither the partition's {@code nameTxn} nor its file size, so the dead-bytes
     * trigger alone never notices. The schema check picks it up, and the compaction re-encodes the
     * partition under the current schema instead of copying its row groups verbatim.
     * <p>
     * The second sweep is the important half: the rewrite must CLEAR what triggered it, or the job would
     * rewrite the whole partition once per interval for the rest of the table's life.
     */
    @Test
    public void testIdleSweepRewritesAParquetPartitionAfterDropColumn() throws Exception {
        setUpSmallRowGroupsNoAutoRewrite();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

        assertMemoryLeak(() -> {
            createCleanParquetPartition("dc");
            final TableToken tableToken = engine.verifyTableName("dc");
            Assert.assertTrue("the fixture should have converted with the column in place", parquetColumnType(tableToken, "b") >= 0);

            execute("ALTER TABLE dc DROP COLUMN b");
            drainWalQueue();
            engine.releaseInactive();

            final long nameTxnBefore = parquetPartitionNameTxn(tableToken);
            runSweepPastTheIdleTimeout();

            Assert.assertNotEquals("the sweep left a parquet partition still carrying a dropped column alone", nameTxnBefore, parquetPartitionNameTxn(tableToken));
            Assert.assertEquals("the dropped column is still in the rewritten file", -1, parquetColumnType(tableToken, "b"));

            // Nothing left to react to: a second pass must leave the partition where it is.
            final long nameTxnAfter = parquetPartitionNameTxn(tableToken);
            runSweepPastTheIdleTimeout();
            Assert.assertEquals("the schema trigger did not clear, so the sweep rewrites the partition every interval", nameTxnAfter, parquetPartitionNameTxn(tableToken));
            assertCleanParquetDataIntact("dc");
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

    /**
     * One sweep with the clock past the idle timeout, applied by the job itself - no writer is held.
     */
    private static void runSweepPastTheIdleTimeout() {
        try (PartitionCompactionScanJob job = newSweepPastTheIdleTimeout()) {
            job.run();
        }
        engine.releaseInactive();
    }

    private void assertCleanParquetDataIntact(String tableName) throws Exception {
        assertQuery("SELECT count() FROM " + tableName)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n5\n");
        assertQuery("SELECT a, s, ts FROM " + tableName + " ORDER BY ts")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(
                        """
                                a\ts\tts
                                1\tk1\t2020-01-01T00:00:00.000000Z
                                2\tk2\t2020-01-01T01:00:00.000000Z
                                3\tk1\t2020-01-01T02:00:00.000000Z
                                4\tk2\t2020-01-01T03:00:00.000000Z
                                99\tk1\t2020-01-02T00:00:00.000000Z
                                """
                );
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
     * Table {@code t}: a parquet partition holding one column of every kind a partition can hold, converted
     * and untouched since. The fixture both all-types schema tests are calibrated against.
     */
    private void createAllColumnTypesParquetPartition() throws Exception {
            execute("""
                    CREATE TABLE t AS (
                      SELECT
                        (x % 5 = 0) c_bool,
                        x::byte c_byte,
                        x::short c_short,
                        rnd_char() c_char,
                        x::int c_int,
                        x::long c_long,
                        x::float c_float,
                        x::double c_double,
                        cast(x as date) c_date,
                        rnd_uuid4() c_uuid,
                        rnd_long256() c_l256,
                        rnd_ipv4() c_ip,
                        rnd_bin(10, 20, 2) c_bin,
                        rnd_geohash(5) c_gh5,
                        rnd_geohash(15) c_gh15,
                        rnd_geohash(31) c_gh31,
                        rnd_geohash(60) c_gh60,
                        ('s' || (x % 3))::symbol c_sym,
                        ('str' || x)::string c_str,
                        rnd_varchar(1, 5, 1) c_vch,
                        ARRAY[[x::double, x + 0.5]] c_arr,
                        (x::double)::decimal(10, 2) c_dec64,
                        (x::double)::decimal(30, 4) c_dec128,
                        (x * 1000)::timestamp_ns c_ts_ns,
                        cast(x * 1000 as timestamp) c_ts_micro,
                        timestamp_sequence('2024-01-01', 60_000_000) ts
                      FROM long_sequence(20)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO t(c_int, ts) VALUES (1, '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts in '2024-01-01'");
        engine.releaseInactive();
    }

    /**
     * A parquet partition with no dead bytes at all, so the only thing a sweep can react to is the DDL the
     * test applies afterwards.
     */
    private void createCleanParquetPartition(String tableName) throws Exception {
        execute(
                "CREATE TABLE " + tableName + " (a INT, b LONG, s SYMBOL, ts TIMESTAMP)\n" +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL"
        );
        execute(
                "INSERT INTO " + tableName + "(a, b, s, ts) VALUES" +
                        "(1, 10, 'k1', '2020-01-01T00:00:00.000Z')," +
                        "(2, 20, 'k2', '2020-01-01T01:00:00.000Z')," +
                        "(3, 30, 'k1', '2020-01-01T02:00:00.000Z')," +
                        "(4, 40, 'k2', '2020-01-01T03:00:00.000Z')"
        );
        // Moves the table's max timestamp off the partition being converted: CONVERT PARTITION TO PARQUET
        // will not take the active one.
        execute("INSERT INTO " + tableName + "(a, b, s, ts) VALUES (99, 990, 'k1', '2020-01-02T00:00:00.000Z')");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
        drainWalQueue();
        engine.releaseInactive();
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

    /**
     * The type the parquet FILE holds for {@code columnName}, or -1 when the file no longer carries it.
     */
    private int parquetColumnType(TableToken tableToken, String columnName) {
        try (TableReader reader = engine.getReader(tableToken)) {
            final int parquetIdx = findParquetPartitionIndex(reader);
            Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
            reader.openPartition(parquetIdx);
            final var meta = reader.getAndInitParquetPartitionDecoder(parquetIdx).metadata();
            final int columnIndex = meta.getColumnIndex(columnName);
            return columnIndex < 0 ? -1 : meta.getColumnType(columnIndex);
        }
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

    /**
     * The directory the sweep stages the parquet partition's compacted copy into, for its live generation.
     */
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
