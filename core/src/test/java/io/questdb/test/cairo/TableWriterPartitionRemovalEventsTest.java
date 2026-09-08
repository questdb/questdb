/*+*****************************************************************************
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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionRemovalEvents;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the partition removal log a {@link TableWriter} keeps and the WAL apply job
 * collects: one event per physical partition, exact interval bounds, the committed row
 * count, the TTL-versus-DROP source and the seqTxn of the commit that carried it. A live
 * view's refresh worker consumes this log to reconcile its checkpoint timeline, so the
 * contract is pinned on plain tables here where every shape is easy to produce.
 */
public class TableWriterPartitionRemovalEventsTest extends AbstractCairoTest {

    @Test
    public void testDropPartitionListWithSeparatedPartitionsKeepsDisjointEvents() throws Exception {
        // One DROP PARTITION LIST naming two partitions with a survivor between them must
        // produce two events with a gap, never one min/max envelope: a consumer retiring
        // checkpoint roots inside the removed ranges has to keep the survivor's.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES " +
                    "('2026-01-01T10:00:00.000000Z', 1), ('2026-01-01T11:00:00.000000Z', 2), " +
                    "('2026-01-02T10:00:00.000000Z', 3), " +
                    "('2026-01-03T10:00:00.000000Z', 4), ('2026-01-03T11:00:00.000000Z', 5), ('2026-01-03T12:00:00.000000Z', 6), " +
                    "('2026-01-05T10:00:00.000000Z', 7)");
            drainWalQueue();
            execute("ALTER TABLE t DROP PARTITION LIST '2026-01-01', '2026-01-03'");

            final TableToken token = engine.verifyTableName("t");
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 1)) {
                job.applyWalDirect(token, Job.RUNNING_STATUS);
                final PartitionRemovalEvents events = job.getCommittedRemovalEvents();
                Assert.assertEquals(2, events.size());
                Assert.assertEquals(5, events.getTotalRemovedRows());

                // The DROP is the table's second transaction.
                assertEvent(events, 0, 2, "2026-01-01", "2026-01-02", 2, PartitionRemovalEvents.SOURCE_DROP_PARTITION);
                // 2026-01-04 is absent, so the interval ends at the logical partition's
                // own ceiling, not at the next attached partition.
                assertEvent(events, 1, 2, "2026-01-03", "2026-01-04", 3, PartitionRemovalEvents.SOURCE_DROP_PARTITION);

                // The next apply starts a fresh log.
                execute("INSERT INTO t VALUES ('2026-01-06T10:00:00.000000Z', 8)");
                job.applyWalDirect(token, Job.RUNNING_STATUS);
                Assert.assertTrue("an apply that removed nothing must report no events", job.getCommittedRemovalEvents().isEmpty());
            }
            assertQuery("SELECT name FROM table_partitions('t') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name
                            2026-01-02
                            2026-01-05
                            2026-01-06
                            """);
        });
    }

    @Test
    public void testNonWalRemovePartitionRecordsEventUntilCountersReset() throws Exception {
        // The writer-level contract without a WAL in the way: removePartition commits the
        // removal and publishes the event; resetWalApplyCounters starts a new batch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES " +
                    "('2026-01-01T10:00:00.000000Z', 1), ('2026-01-01T11:00:00.000000Z', 2), " +
                    "('2026-01-02T10:00:00.000000Z', 3), " +
                    "('2026-01-03T10:00:00.000000Z', 4)");
            try (TableWriter writer = getWriter("t")) {
                writer.resetWalApplyCounters();
                Assert.assertTrue(writer.removePartition(MicrosTimestampDriver.floor("2026-01-01")));
                final PartitionRemovalEvents events = writer.getCommittedPartitionRemovals();
                Assert.assertEquals(1, events.size());
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01"), events.getLo(0));
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-02"), events.getHiExclusive(0));
                Assert.assertEquals(2, events.getRemovedRows(0));
                Assert.assertFalse(events.isTtl(0));

                // A second removal on the same writer appends in commit order...
                Assert.assertTrue(writer.removePartition(MicrosTimestampDriver.floor("2026-01-02")));
                Assert.assertEquals(2, events.size());
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-02"), events.getLo(1));
                Assert.assertEquals(3, events.getTotalRemovedRows());

                // ...and a removal that finds nothing to remove records nothing.
                writer.resetWalApplyCounters();
                Assert.assertTrue(events.isEmpty());
                Assert.assertFalse(writer.removePartition(MicrosTimestampDriver.floor("2026-01-09")));
                Assert.assertTrue(events.isEmpty());
            }
        });
    }

    @Test
    public void testSplitPartitionReportsEachPhysicalPartSubRange() throws Exception {
        // A logical partition stored as two physical parts reports two events whose
        // intervals tile the logical partition: [floor, splitTs) and [splitTs, ceiling).
        // The consumer corrects checkpoint positions per boundary, so the bounds must be
        // the ranges the parts actually held.
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2026-01-01T00:00:00.000000Z', 60_000_000L), x::int FROM long_sequence(100)");
            execute("INSERT INTO t VALUES ('2026-01-02T00:00:00.000000Z', 0)");
            final TableToken token = engine.verifyTableName("t");
            // Same fixture as AlterTableDropPartitionTest's split cases: a reader pins the
            // day, and the writer splits a partition when the rows before the out-of-order
            // insertion point outnumber twice the rows it has to merge and copy after it, so
            // the late row lands near the end of the day - 99 rows before it, one after.
            try (TableReader ignore = engine.getReader(token)) {
                execute("INSERT INTO t VALUES ('2026-01-01T01:38:00.500000Z', -1)");
            }
            assertQuery("SELECT count() FROM table_partitions('t') WHERE name LIKE '2026-01-01%'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");

            try (TableWriter writer = getWriter("t")) {
                writer.resetWalApplyCounters();
                Assert.assertTrue(writer.removePartition(MicrosTimestampDriver.floor("2026-01-01")));
                final PartitionRemovalEvents events = writer.getCommittedPartitionRemovals();
                Assert.assertEquals(2, events.size());
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01"), events.getLo(0));
                Assert.assertEquals("the two parts must tile the logical partition", events.getHiExclusive(0), events.getLo(1));
                Assert.assertTrue("the split timestamp must sit inside the day", events.getLo(1) > events.getLo(0));
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-02"), events.getHiExclusive(1));
                Assert.assertTrue(events.getRemovedRows(0) > 0);
                Assert.assertTrue(events.getRemovedRows(1) > 0);
                Assert.assertEquals(101, events.getTotalRemovedRows());
                Assert.assertEquals(events.getSeqTxn(0), events.getSeqTxn(1));
            }
        });
    }

    @Test
    public void testTtlEvictionInsideDataCommitRecordsTtlEvents() throws Exception {
        // TTL runs inside the DATA commit's housekeeping, so the event carries that
        // commit's seqTxn and the TTL source, and a commit that evicts nothing reports
        // nothing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY WAL");
            execute("INSERT INTO t VALUES ('2026-01-01T10:00:00.000000Z', 1), ('2026-01-01T11:00:00.000000Z', 2)");
            execute("INSERT INTO t VALUES ('2026-01-02T10:00:00.000000Z', 3)");
            final TableToken token = engine.verifyTableName("t");
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 1)) {
                job.applyWalDirect(token, Job.RUNNING_STATUS);
                // 2026-01-01's ceiling is 2026-01-02; one day past that is 2026-01-03, which
                // the table has not reached.
                Assert.assertTrue(job.getCommittedRemovalEvents().isEmpty());

                execute("INSERT INTO t VALUES ('2026-01-03T10:00:00.000000Z', 4)");
                job.applyWalDirect(token, Job.RUNNING_STATUS);
                final PartitionRemovalEvents events = job.getCommittedRemovalEvents();
                Assert.assertEquals(1, events.size());
                assertEvent(events, 0, 3, "2026-01-01", "2026-01-02", 2, PartitionRemovalEvents.SOURCE_TTL);
                Assert.assertTrue(events.isTtl(0));
            }
            assertQuery("SELECT name FROM table_partitions('t') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name
                            2026-01-02
                            2026-01-03
                            """);
        });
    }

    private static void assertEvent(
            PartitionRemovalEvents events,
            int index,
            long expectedSeqTxn,
            String expectedLo,
            String expectedHiExclusive,
            long expectedRows,
            byte expectedSource
    ) {
        Assert.assertEquals("event " + index + " seqTxn", expectedSeqTxn, events.getSeqTxn(index));
        Assert.assertEquals("event " + index + " lo", MicrosTimestampDriver.floor(expectedLo), events.getLo(index));
        Assert.assertEquals("event " + index + " hiExclusive", MicrosTimestampDriver.floor(expectedHiExclusive), events.getHiExclusive(index));
        Assert.assertEquals("event " + index + " rows", expectedRows, events.getRemovedRows(index));
        Assert.assertEquals("event " + index + " source", expectedSource, events.getSource(index));
    }
}
