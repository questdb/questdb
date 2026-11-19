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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionDeltaWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A WAL apply that the Delta gate stops is retried by CheckWalTransactionsJob, through a stub gate.
 */
public class WalApplyDeltaStopTest extends AbstractCairoTest {
    private static final long DAY_2020_01_01 = 1_577_836_800_000_000L;
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final String TAG_WRITER_TXN = "questdb_wal_apply_writer_txn";
    private static final AtomicInteger denials = new AtomicInteger();
    private static final AtomicInteger gateCalls = new AtomicInteger();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) -> new CairoTestConfiguration(root, telemetry, overrides) {
            @Override
            public PartitionDeltaWriter newPartitionDeltaWriter() {
                return new PartitionDeltaWriter() {
                    @Override
                    public void initialize(TableWriter writer, int partitionIndex) {
                    }

                    @Override
                    public boolean isWalApplyReady(TableWriter writer, long seqTxn) {
                        gateCalls.incrementAndGet();
                        return denials.getAndUpdate(d -> Math.max(0, d - 1)) == 0;
                    }

                    @Override
                    public boolean purge(Path partitionDir) {
                        return true;
                    }

                    @Override
                    public void readTimestampBounds(TableWriter writer, int partitionIndex, long boundsAddr) {
                        Unsafe.getUnsafe().putLong(boundsAddr, Long.MAX_VALUE);
                        Unsafe.getUnsafe().putLong(boundsAddr + Long.BYTES, Long.MIN_VALUE);
                    }

                    @Override
                    public boolean rollback(TableWriter writer, int partitionIndex, long seqTxn) {
                        return false;
                    }

                    @Override
                    public void writeCommit(
                            TableWriter writer,
                            long partitionTimestamp,
                            long partitionNameTxn,
                            ReadOnlyObjList<? extends MemoryCR> o3Columns,
                            long sortedTimestampsAddr,
                            long srcOooLo,
                            long srcOooHi,
                            long seqTxn,
                            long commitTimestamp
                    ) {
                    }
                };
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void resetGate() {
        denials.set(0);
    }

    @Test
    public void testCheckJobRetryKeepsTrackerAndPendingRowCount() throws Exception {
        assertMemoryLeak(() -> {
            final long startMicros = MicrosecondClockImpl.INSTANCE.getTicks();
            setCurrentMicros(startMicros);
            execute("CREATE TABLE tab (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO tab VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-10T00:00:00')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("tab");
            try (TableWriter writer = getWriter("tab")) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(DAY_2020_01_01);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);

            try (
                    ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0);
                    CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine)
            ) {
                // The first run scans and records the republish counter
                checkJob.run();
                walApplyJob.drain(0);
                assertEquals(1, tracker.getWriterTxn());
                final long pendingRowsBefore = walPendingRowCount(token);
                final long gaugeBefore = TestUtils.getMetricValue(engine, TAG_WRITER_TXN);

                denials.set(1);
                gateCalls.set(0);
                // seqTxn 2 and 4 write the native last partition, seqTxn 3 the Delta-active one
                execute("INSERT INTO tab SELECT x::INT, '2020-01-10T00:00:00'::TIMESTAMP + x * 1_000_000L FROM long_sequence(10)");
                execute("INSERT INTO tab VALUES (3, '2020-01-01T01:00:00')");
                execute("INSERT INTO tab SELECT x::INT, '2020-01-10T01:00:00'::TIMESTAMP + x * 1_000_000L FROM long_sequence(10)");
                assertEquals(pendingRowsBefore + 21, walPendingRowCount(token));

                final long republishCount = engine.getUnpublishedWalTxnCount();
                walApplyJob.drain(0);

                // The gate stopped before seqTxn 3, and the tracker keeps the applied txn
                assertEquals(1, gateCalls.get());
                assertTrue(tracker.isInitialised());
                assertEquals(2, tracker.getWriterTxn());
                assertEquals(4, tracker.getSeqTxn());
                assertEquals(gaugeBefore + 1, TestUtils.getMetricValue(engine, TAG_WRITER_TXN));
                assertEquals(pendingRowsBefore + 11, walPendingRowCount(token));
                assertEquals(republishCount + 1, engine.getUnpublishedWalTxnCount());

                // The stop requeues nothing, and the check job does not rescan within its interval
                walApplyJob.drain(0);
                checkJob.run();
                walApplyJob.drain(0);
                assertEquals(1, gateCalls.get());
                assertEquals(2, tracker.getWriterTxn());

                // The next paced scan retries the gate, which now passes
                setCurrentMicros(startMicros + configuration.getSequencerCheckMinInterval() * 1_000L);
                checkJob.run();
                walApplyJob.drain(0);
                assertEquals(2, gateCalls.get());
                assertEquals(4, tracker.getWriterTxn());
                assertEquals(4, tracker.getSeqTxn());
                assertEquals(gaugeBefore + 3, TestUtils.getMetricValue(engine, TAG_WRITER_TXN));
                assertEquals(pendingRowsBefore, walPendingRowCount(token));
            }
        });
    }

    @Test
    public void testGateConsultedOnlyForTransactionsReachingDeltaActivePartition() throws Exception {
        // A table name of its own: the write tracker keeps a seqTxn floor per table token across tests
        final String mergeAppend = configuration.isO3PartitionMergeAppendEnabled() ? "true" : "false";
        final long splitMinSize = configuration.getPartitionO3SplitMinSize();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE gate_tab (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO gate_tab VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-10T00:00:00')");
                drainWalQueue();
                try (TableWriter writer = getWriter("gate_tab")) {
                    writer.getTxWriter().setPartitionDeltaActiveByTimestamp(DAY_2020_01_01);
                    writer.getTxWriter().setPartitionDeltaActiveByTimestamp(DAY_2020_01_01 + 9 * DAY_MICROS);
                    writer.bumpPartitionTableVersion();
                    writer.commit();
                }

                // Past the Delta-active last partition: native rows only
                assertEquals(0, insertAndCountGateCalls("SELECT x::INT, '2020-01-20T00:00:00'::TIMESTAMP + x * 1_000_000L FROM long_sequence(20) UNION ALL SELECT 4, '2020-01-20T23:00:00'"));
                // A gap day after a Delta-active day, before the next partition
                assertEquals(0, insertAndCountGateCalls("VALUES (5, '2020-01-05T00:00:00')"));
                // Into the Delta-active day
                assertEquals(1, insertAndCountGateCalls("VALUES (6, '2020-01-10T01:00:00')"));
                // From a gap into a later Delta-active day
                assertEquals(1, insertAndCountGateCalls("VALUES (7, '2020-01-07T00:00:00'), (8, '2020-01-10T02:00:00')"));

                // An O3 row in the middle of 01-20 splits it
                execute("INSERT INTO gate_tab VALUES (9, '2020-01-20T12:00:00')");
                drainWalQueue();
                final long splitTs;
                try (TableWriter writer = getWriter("gate_tab")) {
                    final int last = writer.getPartitionCount() - 1;
                    splitTs = writer.getTxWriter().getPartitionTimestampByIndex(last);
                    assertTrue(splitTs > DAY_2020_01_01 + 19 * DAY_MICROS);
                }
                // A later split part of a native day, not at its lo timestamp
                assertEquals(0, insertAndCountGateCalls("VALUES (10, '2020-01-20T23:30:00')"));
                try (TableWriter writer = getWriter("gate_tab")) {
                    writer.getTxWriter().setPartitionDeltaActiveByTimestamp(splitTs);
                    writer.bumpPartitionTableVersion();
                    writer.commit();
                }
                // A Delta-active split part
                assertEquals(1, insertAndCountGateCalls("VALUES (11, '2020-01-20T23:45:00')"));
            });
        } finally {
            // Overrides outlive the test
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, mergeAppend);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, splitMinSize);
        }
    }

    @Test
    public void testMatViewTransactionStops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mv_base (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv_view AS (SELECT ts, sum(v) s FROM mv_base SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO mv_base VALUES (1, '2020-01-01T00:00:00')");
            drainWalAndMatViewQueues();
            final TableToken view = engine.verifyTableName("mv_view");
            try (TableWriter writer = getWriter(view)) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(DAY_2020_01_01);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(view);

            // The refresh ends with a MAT_VIEW_DATA transaction into the Delta-active day
            denials.set(Integer.MAX_VALUE);
            gateCalls.set(0);
            execute("INSERT INTO mv_base VALUES (2, '2020-01-01T01:00:00')");
            drainWalAndMatViewQueues();

            assertTrue(gateCalls.get() > 0);
            assertEquals(tracker.getSeqTxn() - 1, tracker.getWriterTxn());
            assertFalse(engine.getTableSequencerAPI().isSuspended(view));
        });
    }

    @Test
    public void testStopKeepsMemoryPressureRegulation() throws Exception {
        assertMemoryLeak(() -> {
            long nowMicros = MicrosecondClockImpl.INSTANCE.getTicks();
            setCurrentMicros(nowMicros);
            final TableToken token = createTableWithDeltaActiveDay("stop_memory");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            try (
                    ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0);
                    CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine)
            ) {
                // The first run scans and records the republish counter
                checkJob.run();
                walApplyJob.drain(0);

                denials.set(Integer.MAX_VALUE);
                gateCalls.set(0);
                execute("INSERT INTO stop_memory VALUES (3, '2020-01-01T01:00:00')");
                // Two out-of-memory errors back the table off
                final TableWriterPressureControl control = tracker.getMemPressureControl();
                control.onOutOfMemory();
                control.onOutOfMemory();
                final int regulation = control.getMemoryPressureRegulationValue();
                assertEquals(2, control.getMemoryPressureLevel());

                for (int retry = 0; retry < 12; retry++) {
                    // Past the backoff
                    nowMicros += 30_000_000L;
                    setCurrentMicros(nowMicros);
                    final int gateCallsBefore = gateCalls.get();
                    checkJob.run();
                    walApplyJob.drain(0);
                    assertTrue(gateCalls.get() > gateCallsBefore);
                    assertEquals(1, tracker.getWriterTxn());
                    assertEquals(regulation, control.getMemoryPressureRegulationValue());
                    assertEquals(2, control.getMemoryPressureLevel());
                }

                // A run that applies its transaction eases the pressure
                denials.set(0);
                nowMicros += 30_000_000L;
                setCurrentMicros(nowMicros);
                checkJob.run();
                walApplyJob.drain(0);
                assertEquals(2, tracker.getWriterTxn());
                assertEquals(1, control.getMemoryPressureLevel());
            }
        });
    }

    @Test
    public void testStopWithNothingAppliedKeepsTableTxn() throws Exception {
        assertMemoryLeak(() -> {
            final long startMicros = MicrosecondClockImpl.INSTANCE.getTicks();
            setCurrentMicros(startMicros);
            final TableToken token = createTableWithDeltaActiveDay("stop_txn");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long intervalMicros = configuration.getSequencerCheckMinInterval() * 1_000L;
            try (
                    ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0);
                    CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine)
            ) {
                // The first run scans and records the republish counter
                checkJob.run();
                walApplyJob.drain(0);
                final long tableTxn = readTableTxn(token);

                denials.set(3);
                gateCalls.set(0);
                execute("INSERT INTO stop_txn VALUES (3, '2020-01-01T01:00:00')");
                walApplyJob.drain(0);
                assertEquals(1, gateCalls.get());
                assertEquals(tableTxn, readTableTxn(token));

                // Each paced retry stops first thing
                for (int retry = 1; retry <= 2; retry++) {
                    setCurrentMicros(startMicros + retry * intervalMicros);
                    checkJob.run();
                    walApplyJob.drain(0);
                    assertEquals(1 + retry, gateCalls.get());
                    assertEquals(1, tracker.getWriterTxn());
                    assertEquals(tableTxn, readTableTxn(token));
                }

                // The retry that the gate lets through commits as usual
                setCurrentMicros(startMicros + 3 * intervalMicros);
                checkJob.run();
                walApplyJob.drain(0);
                assertEquals(2, tracker.getWriterTxn());
                assertTrue(readTableTxn(token) > tableTxn);
            }
        });
    }

    @Test
    public void testTruncateDoesNotConsultGate() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTableWithDeltaActiveDay("stop_truncate");
            denials.set(1);
            gateCalls.set(0);
            execute("TRUNCATE TABLE stop_truncate");
            drainWalQueue();
            assertEquals(0, gateCalls.get());
            assertEquals(1, denials.get());
            denials.set(0);
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            assertEquals(tracker.getSeqTxn(), tracker.getWriterTxn());
            assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() FROM stop_truncate").noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    // Its first day is Delta-active and its last is not.
    private static TableToken createTableWithDeltaActiveDay(String name) throws Exception {
        execute("CREATE TABLE " + name + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + name + " VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-10T00:00:00')");
        drainWalQueue();
        try (TableWriter writer = getWriter(name)) {
            writer.getTxWriter().setPartitionDeltaActiveByTimestamp(DAY_2020_01_01);
            writer.bumpPartitionTableVersion();
            writer.commit();
        }
        return engine.verifyTableName(name);
    }

    private static int insertAndCountGateCalls(String insertBody) throws Exception {
        gateCalls.set(0);
        execute("INSERT INTO gate_tab " + insertBody);
        drainWalQueue();
        return gateCalls.get();
    }

    private static long readTableTxn(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getTxn();
        }
    }

    private static long walPendingRowCount(TableToken token) {
        final RecentWriteTracker.WriteStats stats = engine.getRecentWriteTracker().getWriteStats(token);
        return stats == null ? -1 : stats.getWalRowCount();
    }
}
