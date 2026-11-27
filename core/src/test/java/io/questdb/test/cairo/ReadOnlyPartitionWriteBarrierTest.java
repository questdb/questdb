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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Asserts the read-only partition write-barrier invariant: once a
 * partition's {@code read_only} bit is set, every {@link TableWriter}
 * data-modification path (in-order INSERT, O3 INSERT, UPDATE) must
 * silently drop or hard-reject the write. On WAL apply, an UPDATE, or an INSERT into a dedup
 * table, that reaches a read-only partition must remain unapplied and suspend the table; advancing the apply
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
    public void testO3InsertDroppedRowsDoNotMoveTableBounds() throws Exception {
        assertMemoryLeak(() -> {
            // the written row, then the table's bounds after it
            final String[][] cases = {
                    {"2020-01-03T00:30:00.000000Z", "2020-01-01T01:00:00.000000Z", "2020-01-03T01:00:00.000000Z"}, // merge
                    {"2020-01-03T05:00:00.000000Z", "2020-01-01T01:00:00.000000Z", "2020-01-03T05:00:00.000000Z"}, // append
                    {"2020-01-04T00:00:00.000000Z", "2020-01-01T01:00:00.000000Z", "2020-01-04T00:00:00.000000Z"}, // new last
                    {"2019-12-31T00:00:00.000000Z", "2019-12-31T00:00:00.000000Z", "2020-01-03T01:00:00.000000Z"}, // new first
            };
            int tableId = 0;
            for (String[] testCase : cases) {
                for (boolean isWal : new boolean[]{false, true}) {
                    for (boolean isMergeAppend : new boolean[]{false, true}) {
                        for (boolean isParquet : new boolean[]{false, true}) {
                            final String table = "t_rw_o3_bounds" + tableId++;
                            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, Boolean.toString(isMergeAppend));
                            execute("CREATE TABLE " + table + " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
                            execute("INSERT INTO " + table + " VALUES (1, '2020-01-01T01:00:00'), (2, '2020-01-02T01:00:00'), (3, '2020-01-03T00:00:00'), (4, '2020-01-03T01:00:00')");
                            if (isParquet) {
                                execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
                            }
                            drainWalQueue();
                            final TableToken tt = engine.verifyTableName(table);
                            try (TableWriter writer = getWriter(tt)) {
                                writer.getTxWriter().setPartitionReadOnly(0, true);
                                writer.getTxWriter().setPartitionReadOnly(1, true);
                                writer.bumpPartitionTableVersion();
                            }

                            execute("INSERT INTO " + table + " VALUES (10, '2020-01-01T00:30:00'), (20, '2020-01-02T00:30:00'), (30, '" + testCase[0] + "')");
                            drainWalQueue();

                            try (TableReader reader = engine.getReader(tt)) {
                                Assert.assertEquals(table + ": wrong row count", 5, reader.size());
                                Assert.assertEquals(table + ": wrong min timestamp", MicrosTimestampDriver.floor(testCase[1]), reader.getTxFile().getMinTimestamp());
                                Assert.assertEquals(table + ": wrong max timestamp", MicrosTimestampDriver.floor(testCase[2]), reader.getTxFile().getMaxTimestamp());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testO3InsertReadOnlyLastPartitionKeepsMaxTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_o3_last (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_rw_o3_last VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-02T00:00:00')");
            final TableToken tt = engine.verifyTableName("t_rw_o3_last");
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnly(1, true);
                writer.bumpPartitionTableVersion();

                // the late row starts the O3 batch, which then ends past the max of the read-only partition
                TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor("2020-01-01T01:00:00.000000Z"));
                row.putLong(0, 10L);
                row.append();
                row = writer.newRow(MicrosTimestampDriver.floor("2020-01-02T05:00:00.000000Z"));
                row.putLong(0, 20L);
                row.append();
                writer.commit();
                Assert.assertEquals(MicrosTimestampDriver.floor("2020-01-02T00:00:00.000000Z"), writer.getMaxTimestamp());
            }
            assertQuery("t_rw_o3_last").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    10\t2020-01-01T01:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    """);
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
    public void testWalInOrderDedupInsertReadOnlyLastPartitionSuspends() throws Exception {
        assertMemoryLeak(() -> {
            int tableId = 0;
            for (boolean isMergeAppend : new boolean[]{false, true}) {
                for (InOrderApply apply : new InOrderApply[]{InOrderApply.SINGLE, InOrderApply.ONE_BY_ONE}) {
                    final String table = "t_rw_wal_last_dedup" + tableId++;
                    setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, Boolean.toString(isMergeAppend));
                    final TableToken tt = createReadOnlyLastPartition(table, "SYMBOL", "TIMESTAMP", false, true);
                    final long appliedSeqTxnBeforeInsert = engine.getTableSequencerAPI().getTxnTracker(tt).getWriterTxn();

                    insertInOrder(tt, apply);

                    Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
                    Assert.assertEquals(appliedSeqTxnBeforeInsert, engine.getTableSequencerAPI().getTxnTracker(tt).getWriterTxn());
                    // the first transaction fails on its own row, none waits in LAG for the next one
                    assertQuery("SELECT errorMessage FROM wal_tables() WHERE name = '" + table + "'").noRandomAccess().noLeakCheck().returns(
                            "errorMessage\ncannot insert into read-only partition [table=" + table
                                    + ", partitionTimestamp=2020-01-01T00:00:00.000000Z, rowCount=1]\n");
                    assertReadOnlyLastPartition(tt, table, "1\n2\n", "2020-01-01T02:00:00.000000Z");
                }
            }
        });
    }

    @Test
    public void testWalInOrderInsertReadOnlyLastPartitionDropped() throws Exception {
        assertMemoryLeak(() -> {
            int tableId = 0;
            for (String symbolType : new String[]{"SYMBOL", "SYMBOL INDEX", "SYMBOL INDEX TYPE POSTING INCLUDE (x)"}) {
                for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                    for (boolean isParquet : new boolean[]{false, true}) {
                        for (boolean isMergeAppend : new boolean[]{false, true}) {
                            for (InOrderApply apply : InOrderApply.values()) {
                                final String table = "t_rw_wal_last" + tableId++;
                                setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, Boolean.toString(isMergeAppend));
                                final TableToken tt = createReadOnlyLastPartition(table, symbolType, timestampType, isParquet, false);

                                insertInOrder(tt, apply);

                                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                                if (apply == InOrderApply.INTO_NEXT_PARTITION) {
                                    assertReadOnlyLastPartition(tt, table, "1\n2\n30\n", "2020-01-03T00:00:00.000000Z");
                                } else {
                                    assertReadOnlyLastPartition(tt, table, "1\n2\n", "2020-01-01T02:00:00.000000Z");
                                }

                                execute("INSERT INTO " + table + " VALUES (4, 'a', '2020-01-04T00:00:00')");
                                drainWalQueue();
                                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                                assertQuery("SELECT count() c, sum(x) total FROM " + table + " WHERE s = 'a'")
                                        .noLeakCheck()
                                        .noRandomAccess()
                                        .expectSize()
                                        .returns(apply == InOrderApply.INTO_NEXT_PARTITION ? "c\ttotal\n4\t37\n" : "c\ttotal\n3\t7\n");
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWalO3DedupInsertReadOnlyPartitionSuspendsAndResumeSkipsIt() throws Exception {
        // A dedup INSERT is acknowledged when it is sequenced and may replace sealed rows, so apply
        // must not drop rows that land in a read-only partition. The writable 2020-01-01 is merged before the loop
        // reaches the read-only 2020-01-02, so the rollback must discard that merge too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_wal_ins (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("""
                    INSERT INTO t_rw_wal_ins VALUES
                    (1, '2020-01-01T00:00:00'),
                    (2, '2020-01-02T00:00:00'),
                    (3, '2020-01-03T00:00:00')
                    """);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t_rw_wal_ins");
            final long readOnlyTs = 1_577_923_200_000_000L; // 2020-01-02
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final long appliedSeqTxnBeforeInsert;
            try (TableWriter writer = getWriter(tt)) {
                appliedSeqTxnBeforeInsert = writer.getAppliedSeqTxn();
            }

            execute("""
                    INSERT INTO t_rw_wal_ins VALUES
                    (10, '2020-01-01T01:00:00'),
                    (20, '2020-01-02T01:00:00'),
                    (21, '2020-01-02T02:00:00')
                    """);
            drainWalQueue();
            execute("INSERT INTO t_rw_wal_ins VALUES (4, '2020-01-04T00:00:00')");
            drainWalQueue();

            Assert.assertTrue("late INSERT into a read-only partition must suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt));
            try (TableWriter writer = getWriter(tt)) {
                Assert.assertEquals("failed INSERT and the following one must remain unapplied",
                        appliedSeqTxnBeforeInsert, writer.getAppliedSeqTxn());
            }
            assertQuery("SELECT errorMessage FROM wal_tables() WHERE name = 't_rw_wal_ins'").noRandomAccess().noLeakCheck().returns("""
                    errorMessage
                    cannot insert into read-only partition [table=t_rw_wal_ins, partitionTimestamp=2020-01-02T00:00:00.000000Z, rowCount=2]
                    """);
            assertQuery("t_rw_wal_ins").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    """);

            // The operator skips the failed transaction; the one after it applies.
            execute("ALTER TABLE t_rw_wal_ins RESUME WAL FROM TRANSACTION " + (appliedSeqTxnBeforeInsert + 2));
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("t_rw_wal_ins").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    4\t2020-01-04T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testWalO3InsertReadOnlyPartitionDropped() throws Exception {
        // Without dedup, WAL apply drops only the rows that land in a read-only partition
        // and applies the rest of the transaction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_rw_wal_drop (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t_rw_wal_drop VALUES
                    (1, '2020-01-01T00:00:00'),
                    (2, '2020-01-02T00:00:00'),
                    (3, '2020-01-03T00:00:00')
                    """);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t_rw_wal_drop");
            final long readOnlyTs = 1_577_923_200_000_000L; // 2020-01-02
            try (TableWriter writer = getWriter(tt)) {
                writer.getTxWriter().setPartitionReadOnlyByTimestamp(readOnlyTs, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final long appliedSeqTxnBeforeInsert;
            try (TableWriter writer = getWriter(tt)) {
                appliedSeqTxnBeforeInsert = writer.getAppliedSeqTxn();
            }

            execute("""
                    INSERT INTO t_rw_wal_drop VALUES
                    (10, '2020-01-01T01:00:00'),
                    (20, '2020-01-02T01:00:00'),
                    (21, '2020-01-02T02:00:00')
                    """);
            drainWalQueue();
            execute("INSERT INTO t_rw_wal_drop VALUES (4, '2020-01-04T00:00:00')");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            try (TableWriter writer = getWriter(tt)) {
                Assert.assertEquals(appliedSeqTxnBeforeInsert + 2, writer.getAppliedSeqTxn());
            }
            assertQuery("t_rw_wal_drop").noLeakCheck().timestamp("ts").expectSize().returns("""
                    x\tts
                    1\t2020-01-01T00:00:00.000000Z
                    10\t2020-01-01T01:00:00.000000Z
                    2\t2020-01-02T00:00:00.000000Z
                    3\t2020-01-03T00:00:00.000000Z
                    4\t2020-01-04T00:00:00.000000Z
                    """);
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

    @Test
    public void testWalWritableLastPartitionKeepsFastAppendsNextToReadOnlyPartition() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE t_rw_fast (x LONG, s SYMBOL INDEX TYPE POSTING INCLUDE (x), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // the empty table takes these as one block
                for (int hour = 1; hour <= 3; hour++) {
                    execute("INSERT INTO t_rw_fast VALUES (" + hour + ", 'a', '2020-01-01T0" + hour + ":00:00')");
                }
                drainWalQueue();
                execute("INSERT INTO t_rw_fast VALUES (4, 'a', '2020-01-02T00:00:00')");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t_rw_fast");
                try (TableWriter writer = getWriter(tt)) {
                    writer.getTxWriter().setPartitionReadOnly(0, true);
                    writer.bumpPartitionTableVersion();
                }

                PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
                for (int hour = 1; hour <= 3; hour++) {
                    execute("INSERT INTO t_rw_fast VALUES (" + (4 + hour) + ", 'a', '2020-01-02T0" + hour + ":00:00')");
                }
                drainWalQueue();
                Assert.assertTrue("the block left the fast append", PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get() > 0);

                PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
                engine.getTableSequencerAPI().getTxnTracker(tt).getMemPressureControl().setMaxBlockRowCount(1);
                for (int hour = 4; hour <= 6; hour++) {
                    execute("INSERT INTO t_rw_fast VALUES (" + (4 + hour) + ", 'a', '2020-01-02T0" + hour + ":00:00')");
                }
                drainWalQueue();
                Assert.assertTrue("the single transaction left the fast append", PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);

                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                assertQuery("SELECT count() c, sum(x) total FROM t_rw_fast WHERE s = 'a'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("c\ttotal\n10\t55\n");
            });
        } finally {
            PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        }
    }

    private static TableToken createReadOnlyLastPartition(
            String table,
            String symbolType,
            String timestampType,
            boolean isParquet,
            boolean isDedup
    ) throws Exception {
        execute("CREATE TABLE " + table + " (x LONG, s " + symbolType + ", ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY WAL"
                + (isDedup ? " DEDUP UPSERT KEYS(ts)" : ""));
        execute("INSERT INTO " + table + " VALUES (1, 'a', '2020-01-01T01:00:00'), (2, 'a', '2020-01-01T02:00:00'), (3, 'a', '2020-01-02T00:00:00')");
        if (isParquet) {
            execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
        }
        drainWalQueue();
        final TableToken tt = engine.verifyTableName(table);
        try (TableWriter writer = getWriter(tt)) {
            writer.getTxWriter().setPartitionReadOnly(0, true);
            writer.bumpPartitionTableVersion();
        }
        execute("ALTER TABLE " + table + " DROP PARTITION LIST '2020-01-02'");
        drainWalQueue();
        engine.releaseAllWriters();
        return tt;
    }

    private static void insertInOrder(TableToken tt, InOrderApply apply) throws Exception {
        final String insert = "INSERT INTO " + tt.getTableName() + " VALUES ";
        execute(insert + "(10, 'a', '2020-01-01T03:00:00')");
        if (apply != InOrderApply.SINGLE) {
            execute(insert + "(20, 'a', '2020-01-01T04:00:00')");
            execute(apply == InOrderApply.INTO_NEXT_PARTITION
                    ? insert + "(30, 'a', '2020-01-03T00:00:00')"
                    : insert + "(30, 'a', '2020-01-01T05:00:00')");
        }
        if (apply == InOrderApply.ONE_BY_ONE) {
            engine.getTableSequencerAPI().getTxnTracker(tt).getMemPressureControl().setMaxBlockRowCount(1);
        }
        drainWalQueue();
    }

    private void assertReadOnlyLastPartition(TableToken tt, String table, String expectedX, String expectedMaxTimestamp) throws Exception {
        try (TableReader reader = engine.getReader(tt)) {
            Assert.assertTrue(table, reader.getTxFile().isPartitionReadOnly(0));
            Assert.assertEquals(table + ": LAG left in the read-only partition", 0, reader.getTxFile().getLagRowCount());
            Assert.assertEquals(
                    table + ": wrong max timestamp",
                    ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType())
                            .fromMicros(MicrosTimestampDriver.floor(expectedMaxTimestamp)),
                    reader.getTxFile().getMaxTimestamp()
            );
        }
        assertQuery("SELECT x FROM " + table).noLeakCheck().expectSize().returns("x\n" + expectedX);
    }

    private enum InOrderApply {
        BLOCK,
        INTO_NEXT_PARTITION,
        ONE_BY_ONE,
        SINGLE
    }
}
