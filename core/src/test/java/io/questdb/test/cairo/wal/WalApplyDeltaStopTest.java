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

import io.questdb.cairo.PartitionDeltaWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A WAL apply that the Delta gate stops is retried by CheckWalTransactionsJob, through a stub gate.
 */
public class WalApplyDeltaStopTest extends AbstractCairoTest {
    private static final long DAY_2020_01_01 = 1_577_836_800_000_000L;
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

    private static long walPendingRowCount(TableToken token) {
        final RecentWriteTracker.WriteStats stats = engine.getRecentWriteTracker().getWriteStats(token);
        return stats == null ? -1 : stats.getWalRowCount();
    }
}
