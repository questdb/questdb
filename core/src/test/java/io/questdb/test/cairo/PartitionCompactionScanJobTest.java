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
import io.questdb.cairo.TableWriter;
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
     * Builds one composite partition in each of two SEPARATE tables: {@code cx}'s 2020-01-01 gets its
     * pieces at a long-ago simulated "wall clock" write time, {@code cy}'s 2020-01-09 only 20 minutes
     * before the job runs below - genuinely recent by BOTH measures at once, its own data timestamps AND
     * {@code _geometry}'s {@code lastWriteMicros}. Kept as two tables, not two partitions of one, because
     * {@link #dispatchComposite} runs through {@link TableWriter#compactPartitionNoCommit(int)} plus a
     * real {@link TableWriter#commit()}: that commit runs the writer's OWN per-commit housekeeping too,
     * which would freely sweep up any OTHER wall-clock-idle composite partition on the SAME writer,
     * regardless of this job's own recency filter - a separate table means {@code cx}'s compaction commit
     * cannot reach {@code cy} at all. A plain (never split) partition sits alongside each as a control.
     */
    @Test
    public void testScanCompactsIdleCompositePartitionButLeavesRecentAndPlainAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            // cx's pieces land at this long-ago simulated wall-clock instant.
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

            // cy's pieces land only 20 minutes before the job runs below - well inside the 1-hour idle
            // window on both measures at once. A separate table, so this advance cannot make cx's OWN
            // composite partition look any more idle than it already is.
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-09T23:50:00.000000Z"));
            execute("CREATE TABLE cy AS (" + dayCBase + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cy " + dayDPlain);
            drainWalQueue();
            execute("INSERT INTO cy " + dayCBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            final TableToken cyToken = engine.verifyTableName("cy");
            final long liveRowsABefore;
            final long liveRowsCBefore;
            try (TableReader cxReader = engine.getReader(cxToken); TableReader cyReader = engine.getReader(cyToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals(2, cxTx.getPartitionCount());
                Assert.assertTrue("2020-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", cxReader.getGeometry().getPieceCount(0) > 1);
                Assert.assertFalse("2020-01-03 is plain, never split", cxTx.isPartitionComposite(1));

                final TxReader cyTx = cyReader.getTxFile();
                Assert.assertEquals(2, cyTx.getPartitionCount());
                Assert.assertTrue("2020-01-09 should be composite", cyTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-09 should have more than one piece", cyReader.getGeometry().getPieceCount(0) > 1);
                Assert.assertFalse("2020-01-11 is plain, never split", cyTx.isPartitionComposite(1));

                liveRowsABefore = cxTx.getPartitionSize(0);
                liveRowsCBefore = cyTx.getPartitionSize(0);
            }
            Assert.assertEquals(5960, liveRowsABefore);
            Assert.assertEquals(5960, liveRowsCBefore);

            // 1 hour: long enough that anything written back at 2020-01-01T00:00 is idle by the time the
            // job runs, short enough that cy's pieces - built only 20 minutes before "now" below - still
            // count as too recent, on both the data-timestamp recency check and the writer's own
            // _geometry-based age rule.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken); TableReader cyReader = engine.getReader(cyToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertFalse("2020-01-01 is idle, should have been compacted", cxTx.isPartitionComposite(0));
                Assert.assertEquals(1, cxReader.getGeometry().getPieceCount(0));
                Assert.assertEquals("compaction must not change the row count", liveRowsABefore, cxTx.getPartitionSize(0));
                Assert.assertFalse("2020-01-03 is plain, must stay untouched", cxTx.isPartitionComposite(1));

                final TxReader cyTx = cyReader.getTxFile();
                Assert.assertTrue("2020-01-09 is too recent, must stay composite", cyTx.isPartitionComposite(0));
                Assert.assertTrue(cyReader.getGeometry().getPieceCount(0) > 1);
                Assert.assertEquals("a skipped partition's row count must be unchanged", liveRowsCBefore, cyTx.getPartitionSize(0));
                Assert.assertFalse("2020-01-11 is plain, must stay untouched", cyTx.isPartitionComposite(1));
            }

            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
            assertQuery("SELECT count() c FROM cy").noRandomAccess().expectSize().returns("c\n6010\n");

            execute("CREATE TABLE cx_oracle AS (SELECT i, ts FROM (" +
                    dayABase + " UNION ALL " + dayBPlain + " UNION ALL " + dayABackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cx_oracle ORDER BY ts, i", "SELECT * FROM cx ORDER BY ts, i", LOG
            );

            execute("CREATE TABLE cy_oracle AS (SELECT i, ts FROM (" +
                    dayCBase + " UNION ALL " + dayDPlain + " UNION ALL " + dayCBackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cy_oracle ORDER BY ts, i", "SELECT * FROM cy ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * Same fixture shape as {@link #testScanCompactsIdleCompositePartitionButLeavesRecentAndPlainAlone}, but
     * the composite partition also carries a BITMAP-indexed symbol column and a POSTING-indexed one.
     * {@link TableWriter#compactPartitionNoCommit(int)} - the job's one dispatch path for a composite
     * partition - must leave both indexes able to find every row after the REWRITE, not just the row count
     * and the raw column bytes right.
     */
    @Test
    public void testScanCompactsIdleCompositePartitionPreservesIndexes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            final String dayBase = "SELECT x::INT i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            // Later, plain day - pushes the max timestamp forward so 2020-01-01 is never the active
            // partition, and the backfill below goes through the O3 path instead of an append.
            final String dayPlain = "SELECT x::INT + 90000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)";
            // Lands ONLY inside 2020-01-01, cutting it into pieces (composite).
            final String dayBackfill = "SELECT x::INT + 70000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            execute("CREATE TABLE cx AS (" + dayBase + "), index(sym_bitmap), index(sym_posting TYPE POSTING)" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx " + dayPlain);
            drainWalQueue();
            execute("INSERT INTO cx " + dayBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", cxReader.getGeometry().getPieceCount(0) > 1);
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                Assert.assertFalse("2020-01-01 is idle, should have been REWRITE-compacted", cxReader.getTxFile().isPartitionComposite(0));
            }

            execute("CREATE TABLE cx_oracle AS (SELECT i, sym_bitmap, sym_posting, ts FROM (" +
                    dayBase + " UNION ALL " + dayPlain + " UNION ALL " + dayBackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            for (int k = 0; k < 7; k++) {
                final String value = "sym" + k;
                // UNION ALL of independently symbol-typed subqueries widens the column to VARCHAR in the
                // oracle, so both sides are cast to VARCHAR here purely to line up the comparison's column
                // types - the filter itself still exercises each table's own indexed SYMBOL column.
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        LOG
                );
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        LOG
                );
            }
        });
    }

    /**
     * Same as {@link #testScanCompactsIdleCompositePartitionPreservesIndexes}, except the composite
     * partition being compacted is the table's only - and therefore ACTIVE - partition, exercising
     * {@link TableWriter#compactPartitionNoCommit(int)}'s other branch: closing and reopening the writer's
     * own column handles against the freshly compacted directory (mirrors
     * {@code UpdateTest#testUpdateOnActiveCompositePartition}, which checks the writer stays usable for
     * plain appends afterward but carries no indexed column). Here the check is the index, not the append:
     * both the BITMAP and the POSTING index must still find every row afterward.
     */
    @Test
    public void testScanCompactsIdleActiveCompositePartitionPreservesIndexes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));

            // Small partition, well under a day - mirrors UpdateTest#testUpdateOnActiveCompositePartition's
            // proven recipe for a composite ACTIVE partition (a single PARTITION BY DAY partition is, by
            // construction, the active one throughout).
            final String dayBase = "SELECT x::INT i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence(0, 15*1000000L) ts FROM long_sequence(400)";
            // A small batch landing well inside the partition's own range - forces a merge-append there,
            // leaving the ACTIVE partition composite.
            final String dayBackfill = "SELECT x::INT + 70000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('1970-01-01T00:20:07', 1000000L) ts FROM long_sequence(3)";

            execute("CREATE TABLE cx AS (" + dayBase + "), index(sym_bitmap), index(sym_posting TYPE POSTING)" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO cx " + dayBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have exactly one, the active, partition", 1, cxTx.getPartitionCount());
                Assert.assertTrue("1970-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("1970-01-01 should have more than one piece", cxReader.getGeometry().getPieceCount(0) > 1);
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T03:00:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                Assert.assertFalse(
                        "the active partition is idle past the timeout, should have been REWRITE-compacted",
                        cxReader.getTxFile().isPartitionComposite(0)
                );
            }

            // The writer must still be usable for an ordinary append after reopening its active-partition
            // column handles - see UpdateTest#testUpdateOnActiveCompositePartition for the same check.
            final String dayAppend = "SELECT x::INT + 90000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('1970-01-01T04:00:00', 1000000L) ts FROM long_sequence(5)";
            execute("INSERT INTO cx " + dayAppend);
            drainWalQueue();

            execute("CREATE TABLE cx_oracle AS (SELECT i, sym_bitmap, sym_posting, ts FROM (" +
                    dayBase + " UNION ALL " + dayBackfill + " UNION ALL " + dayAppend +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            for (int k = 0; k < 7; k++) {
                final String value = "sym" + k;
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        LOG
                );
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        LOG
                );
            }
        });
    }

    /**
     * A composite ACTIVE partition leaves {@code columns[]} closed, so {@code TableWriter#finishO3Commit}
     * skips its usual {@code openPartition} call for it - the one call that reconfigures every indexer's
     * live writer. POSTING indexers get their own fixup there ({@code sealPostingIndexesForO3Partitions},
     * chain-based, independent of {@code columns[]}); a BITMAP indexer has no such path and relied solely
     * on {@code openPartition}, so its underlying {@code BitmapIndexWriter} was left exactly as an earlier
     * {@code closeActivePartition} call left it - closed, its key-file memory unmapped.
     * <p>
     * Shape: {@code sym_late} (BITMAP) is added while day0 is still plain, a real follower gets wired up.
     * A backfill inside day0 then forces a merge-append there, making it composite while still active -
     * closing that follower with nothing to reopen it, since day0 stays the active partition. day1 is then
     * created and becomes composite shortly after its own birth (a row landing earlier within day1 itself).
     * With day1 now active and day0 idle, {@link PartitionCompactionScanJob} REWRITE-compacts day0 (the
     * non-active branch, which does not reopen the ACTIVE partition's indexers) and commits - and that
     * commit's own indexing pass dereferences {@code sym_late}'s still-unmapped {@code BitmapIndexWriter}.
     */
    @Test
    public void testScanCompactsIdleNonActiveCompositePartitionSurvivesStaleBitmapIndexerAcrossPartitionSwitch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));

            final String day0Base = "SELECT x::INT i, timestamp_sequence(0, 15*1000000L) ts FROM long_sequence(400)";

            execute("CREATE TABLE cx AS (" + day0Base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Column added while day0 is still plain - a real follower gets wired up.
            execute("ALTER TABLE cx ADD COLUMN sym_late SYMBOL INDEX");
            drainWalQueue();

            // Backfill inside day0 - forces a merge-append there, leaving it composite while still active.
            execute("INSERT INTO cx SELECT x::INT + 70000 i, timestamp_sequence('1970-01-01T00:20:07', 1000000L) ts," +
                    " NULL sym_late FROM long_sequence(3)");
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have exactly one, the active, partition", 1, cxTx.getPartitionCount());
                Assert.assertTrue("day0 should be composite", cxTx.isPartitionComposite(0));
            }

            // day1's own first commit creates it; the second lands earlier within day1 itself - a real
            // merge-append there, so day1 is composite from shortly after its own birth, not plain.
            execute("INSERT INTO cx SELECT x::INT + 80000 i, timestamp_sequence('1970-01-02T00:00:10', 1000000L) ts," +
                    " NULL sym_late FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 90000 i, timestamp_sequence('1970-01-02T00:00:00', 1000000L) ts," +
                    " NULL sym_late FROM long_sequence(3)");
            drainWalQueue();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have two partitions now", 2, cxTx.getPartitionCount());
                Assert.assertTrue("day0 should still be composite", cxTx.isPartitionComposite(0));
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-05T00:00:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final TableToken cxTokenAfter = engine.verifyTableName("cx");
            Assert.assertFalse("the compaction commit suspended the table",
                    engine.getTableSequencerAPI().isSuspended(cxTokenAfter));
        });
    }

    /**
     * A cold-opened {@link TableWriter} - the writer pool had evicted it, so the next access builds a fresh
     * instance - never configures a BITMAP indexer for its own ACTIVE partition when that partition is
     * ALREADY composite at open time: {@code initLastPartition} calls
     * {@code openLastPartitionAndSetAppendPosition}, which is a no-op for a composite last partition (see
     * its own javadoc - nothing appends to one in place), so {@code openPartition}'s
     * {@code indexer.configureFollowerAndWriter} call never runs. But {@code initLastPartition} calls
     * {@code populateDenseIndexerList()} right after regardless, which adds every non-null
     * {@code ColumnIndexer} to {@code denseIndexers} without checking whether it was ever configured - so
     * the still-pristine indexer, its {@code BitmapIndexWriter} never {@code of()}-ed, lands in the set
     * {@code commit()} indexes on. Any later commit on this writer - even
     * {@link PartitionCompactionScanJob#dispatchComposite}'s, compacting a wholly different, non-active,
     * idle partition - reaches {@code updateIndexesSlow} and dereferences that indexer's unmapped key file,
     * an {@link AssertionError} deep in {@code AbstractMemoryCR.addressOf}.
     */
    @Test
    public void testScanCommitCrashesOnColdOpenedWriterWithComposedActivePartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE cx AS (" +
                    "SELECT x::INT i, timestamp_sequence(0, 15*1000000L) ts FROM long_sequence(400)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Added after the table already holds rows - carries a real column top, matching a
            // fuzz-generated table's own "_top" column idiom.
            execute("ALTER TABLE cx ADD COLUMN sym_top SYMBOL INDEX");
            drainWalQueue();

            // Backfill inside day0 - forces a merge-append there, leaving it composite. day0 is still the
            // only, active, partition at this point.
            execute("INSERT INTO cx SELECT x::INT + 70000 i, timestamp_sequence('1970-01-01T00:20:07', 1000000L) ts," +
                    " ('sym' || (x % 7))::symbol sym_top FROM long_sequence(3)");
            drainWalQueue();

            // day1's own first commit creates it, pushing day0 out of the active slot. The second lands
            // earlier within day1 itself - a real merge-append there, so the NEW active partition, day1,
            // is ALSO composite - the shape a cold-opened writer never initializes an indexer for.
            execute("INSERT INTO cx SELECT x::INT + 80000 i, timestamp_sequence('1970-01-02T00:00:10', 1000000L) ts," +
                    " ('sym' || (x % 7))::symbol sym_top FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 90000 i, timestamp_sequence('1970-01-02T00:00:00', 1000000L) ts," +
                    " ('sym' || (x % 7))::symbol sym_top FROM long_sequence(3)");
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have two partitions now", 2, cxTx.getPartitionCount());
                Assert.assertTrue("day0 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("day1, the active partition, should also be composite", cxTx.isPartitionComposite(1));
            }

            // Evict the writer from the pool - the writer pool does this itself once WAL apply's own
            // time-quota ejects a table mid-fuzz-run (see the "WriterPool closed [... reason=IDLE]" log
            // line that precedes the crash in the wild). The NEXT access below cold-opens a fresh
            // TableWriter while day1 is still composite.
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-05T00:00:00.000000Z"));

            // Only day0 (non-active, composite, idle) is a candidate here - day1 is still the active
            // partition and stays well inside the recency window. dispatchComposite cold-opens the writer
            // (day1 still composite at that point), compacts day0 (unrelated to day1), then commits - the
            // commit is where indexing dereferences day1's never-configured sym_top indexer.
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final TableToken cxTokenAfter = engine.verifyTableName("cx");
            Assert.assertFalse("the compaction commit suspended the table",
                    engine.getTableSequencerAPI().isSuspended(cxTokenAfter));
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
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
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
