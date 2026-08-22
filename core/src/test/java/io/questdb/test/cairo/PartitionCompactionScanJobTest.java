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

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coverage for {@link PartitionCompactionScanJob}: the interval gate on its own (mirroring
 * {@link io.questdb.test.cairo.wal.WalPurgeJobTest}'s own interval test), then end-to-end sweeps that must
 * compact only genuinely idle composite/Parquet partitions and leave everything else - plain partitions,
 * already-compact ones, and anything too recent - untouched.
 */
public class PartitionCompactionScanJobTest extends AbstractCairoTest {

    /**
     * Mirrors {@link io.questdb.test.cairo.wal.WalPurgeJobTest}'s own interval-gate test: wraps the files
     * facade to count how many times the sweep actually touches a table's {@code _txn} file, and checks
     * that count only ever moves on a {@link PartitionCompactionScanJob#run()} call made after the
     * configured interval has elapsed, never before, and never twice for the same elapsed interval.
     */
    @Test
    public void testInterval() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                counter.incrementAndGet();
                return super.exists(path);
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + "(" +
                    "x long," +
                    "ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            drainWalQueue();

            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000; // ms to us.
            setCurrentMicros(1); // Some point in time that's not 0.

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                counter.set(0);

                // last == 0 at construction; not enough simulated time has passed yet.
                job.run();
                Assert.assertEquals("no sweep should run before the interval elapses", 0, counter.get());

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                final int afterFirstTrigger = counter.get();
                Assert.assertTrue("expected a sweep to have run", afterFirstTrigger > 0);

                // No clock movement: must not sweep again.
                job.run();
                job.run();
                Assert.assertEquals("no extra sweep without the clock advancing", afterFirstTrigger, counter.get());

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                final int afterSecondTrigger = counter.get();
                Assert.assertTrue("expected a second sweep to have run", afterSecondTrigger > afterFirstTrigger);

                // A large jump still triggers only once per run() call.
                setCurrentMicros(currentMicros + 10 * interval);
                job.run();
                Assert.assertTrue("expected a third sweep to have run", counter.get() > afterSecondTrigger);
            }
        });
    }

    /**
     * Builds two composite partitions in the same table, ten days apart, at the same simulated
     * "wall clock" write time - so the ONLY thing that can tell them apart at sweep time is each
     * partition's own data-timestamp recency, never {@code _geometry}'s {@code lastWriteMicros}.
     * Two plain (never split) partitions sit alongside them as a control.
     */
    @Test
    public void testScanCompactsIdleCompositePartitionButLeavesRecentAndPlainAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            // Every insert below lands at this same simulated wall-clock instant, so both composite
            // partitions get the exact same _geometry lastWriteMicros - only the recency filter (each
            // partition's OWN data timestamps) can distinguish "old enough" from "too recent" below.
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            // One day at 15s, so the partition holds 5760 rows before anything backdated lands.
            final String dayABase = "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            // A later, plain day - pushes the max timestamp forward so 2020-01-01 is never the active
            // partition, and the backfill below goes through the O3 path instead of an append.
            final String dayBPlain = "SELECT x::INT + 90000 i, timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)";
            // Lands ONLY inside 2020-01-01, cutting it into pieces (composite).
            final String dayABackfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            final String dayCBase = "SELECT x::INT + 200000 i, timestamp_sequence('2020-01-09', 15*1000000L) ts FROM long_sequence(5760)";
            final String dayDPlain = "SELECT x::INT + 290000 i, timestamp_sequence('2020-01-11', 60*1000000L) ts FROM long_sequence(50)";
            final String dayCBackfill = "SELECT x::INT + 270000 i, timestamp_sequence('2020-01-09T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            execute("CREATE TABLE cx AS (" + dayABase + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx " + dayBPlain);
            drainWalQueue();
            execute("INSERT INTO cx " + dayABackfill);
            drainWalQueue();

            execute("INSERT INTO cx " + dayCBase);
            drainWalQueue();
            execute("INSERT INTO cx " + dayDPlain);
            drainWalQueue();
            execute("INSERT INTO cx " + dayCBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            final long liveRowsABefore;
            final long liveRowsCBefore;
            try (TableReader reader = engine.getReader(cxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(4, tx.getPartitionCount());
                Assert.assertTrue("2020-01-01 should be composite", tx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", reader.getGeometry().getPieceCount(0) > 1);
                Assert.assertFalse("2020-01-03 is plain, never split", tx.isPartitionComposite(1));
                Assert.assertTrue("2020-01-09 should be composite", tx.isPartitionComposite(2));
                Assert.assertTrue("2020-01-09 should have more than one piece", reader.getGeometry().getPieceCount(2) > 1);
                Assert.assertFalse("2020-01-11 is plain, never split", tx.isPartitionComposite(3));
                liveRowsABefore = tx.getPartitionSize(0);
                liveRowsCBefore = tx.getPartitionSize(2);
            }
            Assert.assertEquals(5960, liveRowsABefore);
            Assert.assertEquals(5960, liveRowsCBefore);

            // 1 hour: long enough that anything written back at 2020-01-01T00:00 is idle by the time the
            // job runs, short enough that 2020-01-09's own upper bound (2020-01-10T00:00, ten minutes
            // before "now" below) still counts as too recent.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "3600000");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(cxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("2020-01-01 is idle, should have been compacted", tx.isPartitionComposite(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
                Assert.assertEquals("compaction must not change the row count", liveRowsABefore, tx.getPartitionSize(0));

                Assert.assertFalse("2020-01-03 is plain, must stay untouched", tx.isPartitionComposite(1));

                Assert.assertTrue("2020-01-09 is too recent, must stay composite", tx.isPartitionComposite(2));
                Assert.assertTrue(reader.getGeometry().getPieceCount(2) > 1);
                Assert.assertEquals("a skipped partition's row count must be unchanged", liveRowsCBefore, tx.getPartitionSize(2));

                Assert.assertFalse("2020-01-11 is plain, must stay untouched", tx.isPartitionComposite(3));
            }

            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n12020\n");

            execute("CREATE TABLE cx_oracle AS (SELECT i, ts FROM (" +
                    dayABase + " UNION ALL " + dayBPlain + " UNION ALL " + dayABackfill + " UNION ALL " +
                    dayCBase + " UNION ALL " + dayDPlain + " UNION ALL " + dayCBackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cx_oracle ORDER BY ts, i", "SELECT * FROM cx ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * A Parquet partition with dead row-group bytes below the automatic rewrite ratio gets rewritten once
     * idle; a second, never-updated Parquet partition - equally idle, but with nothing to reclaim - is left
     * exactly as it was (same name txn, zero dead bytes throughout).
     */
    @Test
    public void testScanCompactsIdleDirtyParquetPartitionButLeavesCleanOneAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE px (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "INSERT INTO px(a, ts) VALUES" +
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
            // Pusher day, so 2020-01-01 is inactive by the time it is converted below.
            execute("INSERT INTO px(a, ts) VALUES (90, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE px CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Three in-place O3 updates: each appends a merged row group and leaves the one it replaced
            // as dead bytes. Ratio/max-bytes thresholds are disabled above, so none auto-rewrites.
            execute("INSERT INTO px(a, ts) VALUES (101, '2020-01-01T01:30:00.000Z')");
            drainWalQueue();
            execute("INSERT INTO px(a, ts) VALUES (102, '2020-01-01T02:30:00.000Z')");
            drainWalQueue();
            execute("INSERT INTO px(a, ts) VALUES (103, '2020-01-01T03:30:00.000Z')");
            drainWalQueue();

            // A second, CLEAN Parquet partition: converted, never touched by an O3 update afterward.
            execute("INSERT INTO px(a, ts) VALUES (200, '2020-01-03T00:00:00.000Z')");
            execute("INSERT INTO px(a, ts) VALUES (400, '2020-01-04T00:00:00.000Z')"); // pusher
            drainWalQueue();
            execute("ALTER TABLE px CONVERT PARTITION TO PARQUET LIST '2020-01-03'");
            drainWalQueue();

            final TableToken pxToken = engine.verifyTableName("px");
            final long cleanNameTxnBefore;
            try (TableReader reader = engine.getReader(pxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue(tx.isPartitionParquet(0));
                Assert.assertTrue(tx.isPartitionParquet(2));
                cleanNameTxnBefore = tx.getPartitionNameTxn(2);
            }
            assertUnusedBytes(pxToken, 0, true);
            assertUnusedBytes(pxToken, 2, false);

            // The ratio/max-bytes thresholds above were disabled only so the 3 O3 updates could build up
            // dead bytes without the automatic mid-commit rewrite already reclaiming them. The idle scan
            // job reuses these exact same keys (see PartitionCompactionScanJob), so re-tighten them now,
            // after every write above has already landed, to the values the job itself should act on.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.01");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "3600000");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertUnusedBytes(pxToken, 0, false);
            assertUnusedBytes(pxToken, 2, false);
            try (TableReader reader = engine.getReader(pxToken)) {
                Assert.assertEquals(
                        "clean partition must not have been rewritten",
                        cleanNameTxnBefore, reader.getTxFile().getPartitionNameTxn(2)
                );
            }

            assertQuery("SELECT count() c FROM px").noRandomAccess().expectSize().returns("c\n18\n");
        });
    }

    private void assertUnusedBytes(TableToken tableToken, int partitionIndex, boolean expectPositive) throws Exception {
        try (TableReader reader = engine.getReader(tableToken)) {
            reader.openPartition(partitionIndex);
            final long unusedBytes = reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getUnusedBytes();
            if (expectPositive) {
                Assert.assertTrue("expected dead row-group bytes, got " + unusedBytes, unusedBytes > 0);
            } else {
                Assert.assertEquals("expected no dead row-group bytes", 0, unusedBytes);
            }
        }
    }
}
