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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Asserts the read-only partition write-barrier invariant: once a
 * partition's {@code read_only} bit is set, every {@link TableWriter}
 * data-modification path (in-order INSERT, O3 INSERT, UPDATE) must
 * silently drop or hard-reject the write. On WAL apply, an UPDATE that reaches a read-only
 * partition must remain unapplied and suspend the table; advancing the apply
 * watermark would permanently lose a statement already acknowledged by the
 * sequencer.
 */
public class ReadOnlyPartitionWriteBarrierTest extends AbstractCairoTest {

    @Test
    public void testRemoteSwitchOnReadOnlyPartitionAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_cs (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_rw_cs VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-02T00:00:00')");
            final TableToken tt = engine.verifyTableName("t_rw_cs");
            final long ts = 1_577_836_800_000_000L;
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionParquet(ts, 1024L);
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(ts, true);
                writer.bumpPartitionTableVersion();
                writer.commit();

                writer.getTxWriter().setPartitionParquet(ts, 2048L);
                writer.getTxWriter().setPartitionParquetGenerated(0, false);
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertTrue(reader.getTxFile().isPartitionRemote(0));
                Assert.assertEquals("size swapped to the new size",
                        2048L, reader.getTxFile().getPartitionParquetFileSize(0));
                Assert.assertFalse("parquet_generated cleared",
                        reader.getTxFile().isPartitionParquetGenerated(0));
                Assert.assertTrue("read_only preserved",
                        reader.getTxFile().isPartitionReadOnly(0));
            }
        });
    }

    @Test
    public void testInOrderInsertReadOnlyPartitionDropped() throws Exception {
        // The in-order INSERT path returns NOOP_ROW when
        // lastOpenPartitionIsReadOnly is true. row count is unchanged.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_in (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_rw_in VALUES (1, '2020-01-01T00:00:00')");
            final TableToken tt = engine.verifyTableName("t_rw_in");
            final long activeTs = 1_577_836_800_000_000L; // 2020-01-01
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(activeTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            // Re-open the writer so lastOpenPartitionIsReadOnly is refreshed
            // from the just-committed _txn (the field is cached at
            // openLastPartitionAndSetAppendPosition time).
            try (TableWriter writer = getWriter(tt)) {
                final long rowCountBefore = writer.getRowCount();
                // In-order insert into the active partition (now read-only).
                final TableWriter.Row row = writer.newRow(activeTs + 3_600_000_000L); // +1 hour
                row.putLong(0, 42L);
                row.append();
                writer.commit();
                Assert.assertEquals("read-only in-order INSERT must NOOP_ROW",
                        rowCountBefore, writer.getRowCount());
            }
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertEquals("post-insert row count unchanged",
                        1L, reader.size());
            }
        });
    }

    @Test
    public void testO3InsertReadOnlyPartitionDropped() throws Exception {
        // The O3 path's per-partition loop continues past read-only
        // partitions (TableWriter.java line ~7803). row count and the
        // read-only partition's size are unchanged.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_o3 (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_rw_o3 VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-03T00:00:00')");
            final TableToken tt = engine.verifyTableName("t_rw_o3");
            final long readOnlyTs = 1_577_836_800_000_000L; // 2020-01-01
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();

                final long rowCountBefore = writer.getRowCount();
                final int readOnlyPartitionIdx = writer.getTxWriter().getPartitionIndex(readOnlyTs);
                final long readOnlyPartitionSizeBefore = writer.getTxWriter().getPartitionSize(readOnlyPartitionIdx);

                // O3 insert: timestamp targets the read-only 2020-01-01 partition,
                // submitted while the active partition is 2020-01-03.
                final TableWriter.Row row = writer.newRow(readOnlyTs + 3_600_000_000L); // 2020-01-01T01:00
                row.putLong(0, 99L);
                row.append();
                writer.commit();

                Assert.assertEquals("O3 insert into read-only partition must not change total row count",
                        rowCountBefore, writer.getRowCount());
                Assert.assertEquals("read-only partition's size must not grow",
                        readOnlyPartitionSizeBefore,
                        writer.getTxWriter().getPartitionSize(readOnlyPartitionIdx));
            }
        });
    }

    @Test
    public void testUpdateReadOnlyPartitionRejected() throws Exception {
        // UpdateOperatorImpl rejects updates that target a read-only
        // partition with a clear CairoException.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_upd (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_rw_upd VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-03T00:00:00')");
            final TableToken tt = engine.verifyTableName("t_rw_upd");
            final long readOnlyTs = 1_577_836_800_000_000L; // 2020-01-01
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            try {
                update("UPDATE t_rw_upd SET x = 999 WHERE ts = '2020-01-01T00:00:00.000000Z'");
                Assert.fail("UPDATE on read-only partition must throw");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot update read-only partition");
            }

            // Untouched partition's value preserved.
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertEquals(2L, reader.size());
            }
        });
    }

    @Test
    public void testRemoteBitFlipOnReadOnlyPartitionAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_up (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_rw_up VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-02T00:00:00')");
            final TableToken tt = engine.verifyTableName("t_rw_up");
            final long ts = 1_577_836_800_000_000L;
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionParquet(ts, 4096L);
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(ts, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                Assert.assertTrue(writer.getTxWriter().isPartitionReadOnly(0));
                Assert.assertFalse(writer.getTxWriter().isPartitionRemote(0));

                writer.getTxWriter().setPartitionRemoteByTimestamp(ts, true);
                writer.bumpPartitionTableVersion();
                writer.commit();

                Assert.assertTrue("REMOTE bit flip must be allowed on read-only partition",
                        writer.getTxWriter().isPartitionRemote(0));
                Assert.assertTrue("read_only bit preserved",
                        writer.getTxWriter().isPartitionReadOnly(0));
            }
        });
    }

    @Test
    public void testWalUpdateHotRowsAppliesWithReadOnlyPartitionPresent() throws Exception {
        // An UPDATE whose matched rows all live in writable partitions applies
        // normally even when the table carries a read-only partition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_wal_hot (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t_rw_wal_hot VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-03T00:00:00')");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t_rw_wal_hot");
            final long readOnlyTs = 1_577_836_800_000_000L; // 2020-01-01
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            update("UPDATE t_rw_wal_hot SET x = 1002 WHERE ts IN '2020-01-03'");
            drainWalQueue();

            Assert.assertFalse("hot-rows-only UPDATE must apply, not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("t_rw_wal_hot").noLeakCheck().timestamp("ts").expectSize().returns(
                    "x\tts\n" +
                            "1\t2020-01-01T00:00:00.000000Z\n" +
                            "1002\t2020-01-03T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testWalUpdateReadOnlyPartitionSuspendsWithoutAdvancingApplyTxn() throws Exception {
        // UPDATE is acknowledged when it is sequenced. If apply later discovers
        // that a matched partition is read-only, the transaction must remain at
        // the apply watermark so an operator can see and resolve it; skipping it
        // would turn the earlier acknowledgement into silent data loss.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_wal_upd (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t_rw_wal_upd VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-03T00:00:00')");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t_rw_wal_upd");
            final long readOnlyTs = 1_577_836_800_000_000L; // 2020-01-01
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final long appliedSeqTxnBeforeUpdate;
            try (TableWriter writer = getWriter(tt)) {
                appliedSeqTxnBeforeUpdate = writer.getAppliedSeqTxn();
            }

            update("UPDATE t_rw_wal_upd SET x = 999 WHERE ts IN '2020-01-01'");
            execute("INSERT INTO t_rw_wal_upd VALUES (3, '2020-01-04T00:00:00')");
            drainWalQueue();

            Assert.assertTrue("UPDATE over a read-only partition must suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt));
            try (TableWriter writer = getWriter(tt)) {
                Assert.assertEquals("failed UPDATE and following INSERT must remain unapplied",
                        appliedSeqTxnBeforeUpdate, writer.getAppliedSeqTxn());
            }
            assertQuery("t_rw_wal_upd").noLeakCheck().timestamp("ts").expectSize().returns(
                    "x\tts\n" +
                            "1\t2020-01-01T00:00:00.000000Z\n" +
                            "2\t2020-01-03T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testWalUpdateSpanningHotAndReadOnlyPartitionsSuspendsAndRollsBack() throws Exception {
        // The read-only partition sits AFTER the hot one in scan order, so the
        // row loop has already staged hot-partition updates when it trips the
        // read-only check: the rollback must discard them - all-or-nothing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_wal_span (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t_rw_wal_span VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-03T00:00:00')");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t_rw_wal_span");
            final long readOnlyTs = 1_578_009_600_000_000L; // 2020-01-03
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            update("UPDATE t_rw_wal_span SET x = x + 1000");
            drainWalQueue();

            Assert.assertTrue("spanning UPDATE must suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("t_rw_wal_span").noLeakCheck().timestamp("ts").expectSize().returns(
                    "x\tts\n" +
                            "1\t2020-01-01T00:00:00.000000Z\n" +
                            "2\t2020-01-03T00:00:00.000000Z\n");
        });
    }
}
