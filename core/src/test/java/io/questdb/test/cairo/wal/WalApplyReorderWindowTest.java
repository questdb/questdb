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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.WalApplyReorderTimer;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.griffin.SqlException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QuestDBTestNode;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_INT;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;
import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class WalApplyReorderWindowTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY, 8);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDisabledWindowAppliesOnFirstTickWithoutTimer() throws Exception {
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final long timerCount = engine.getTimerShards().size();

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            Assert.assertNull(tracker.getReorderTimer());
            Assert.assertEquals(timerCount, engine.getTimerShards().size());
            assertQuery("select count() from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testExtremeWindowSaturatesDeadline() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, Long.MAX_VALUE);
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
            Assert.assertEquals(Long.MAX_VALUE, tracker.getDeferredDeadlineMicros());
            final WalApplyReorderTimer timer = tracker.getReorderTimer();
            Assert.assertNotNull(timer);
            timer.cancel();
            tracker.promoteExpiredAndGetState(Long.MAX_VALUE);
        });
    }

    @Test
    public void testFutureCommitTimestampDeadlineIsClampedToLocalClock() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        // Commit while the clock reads far ahead, then step it back before apply runs.
        // This simulates a txnlog stamped by another host's clock (replica, PITR) or a
        // local clock step: the deadline must anchor at the local "now", not at the
        // future commit timestamp, so the deferral never exceeds the window.
        setCurrentMicros(5_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
            }

            setCurrentMicros(1_000_000);
            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());
                releaseAndDrain(tracker, applyJob, 1_100_000);
            }

            assertQuery("select count() from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testLostTimerIsReleasedBySequencerCheck() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final long timerCount = engine.getTimerShards().size();

            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
            }
            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                applyJob.run();
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                timer.cancel();
                Assert.assertEquals(timerCount, engine.getTimerShards().size());

                setCurrentMicros(1_100_000);
                new CheckWalTransactionsJob(engine).runSerially();

                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertEquals(
                        0,
                        TestUtils.getMetricValue(engine, "questdb_wal_apply_reorder_waiting_tables")
                );
                Assert.assertEquals(
                        1,
                        TestUtils.getMetricValue(engine, "questdb_wal_apply_reorder_windows_total")
                );
                Assert.assertEquals(
                        1,
                        TestUtils.getMetricValue(engine, "questdb_wal_apply_reorder_transactions_total")
                );
                Assert.assertEquals(
                        1,
                        TestUtils.getMetricValue(engine, "questdb_wal_apply_reorder_sweep_releases_total")
                );
                applyJob.drain(0);
            }

            assertQuery("select count() from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testDropCancelsDeferredTimerAndLateExpiryIsNoOp() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final long timerCount = engine.getTimerShards().size();
            final WalApplyReorderTimer timer;

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);

                execute("drop table x");
                Assert.assertNull(tracker.getReorderTimer());
                Assert.assertEquals(timerCount, engine.getTimerShards().size());
                timer.expire();
                applyJob.drain(0);
            }

            Assert.assertTrue(engine.isTableDropped(tableToken));
        });
    }

    @Test
    public void testReleaseAllCancelsDeferredTimerAndReconstructsWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker oldTracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final long timerCount = engine.getTimerShards().size();
            final WalApplyReorderTimer oldTimer;

            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                try (WalWriter writer = engine.getWalWriter(tableToken)) {
                    appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                    writer.commit();
                }
                applyJob.run();
                oldTimer = oldTracker.getReorderTimer();
                Assert.assertNotNull(oldTimer);
                Assert.assertEquals(timerCount + 1, engine.getTimerShards().size());

                engine.getTableSequencerAPI().releaseAll();
                Assert.assertTrue(oldTimer.isCancelled());
                Assert.assertEquals(SeqTxnTracker.REORDER_NONE, oldTracker.getReorderState());
                Assert.assertEquals(timerCount, engine.getTimerShards().size());
                Assert.assertEquals(
                        0,
                        TestUtils.getMetricValue(engine, "questdb_wal_apply_reorder_waiting_tables")
                );

                new CheckWalTransactionsJob(engine).runSerially();
                final SeqTxnTracker newTracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
                Assert.assertNotSame(oldTracker, newTracker);
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, newTracker.getReorderState());
                Assert.assertEquals(1_100_000, newTracker.getDeferredDeadlineMicros());
                releaseAndDrain(newTracker, applyJob, 1_100_000);
            }

            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testLiveZeroRowCommitForceReleasesV1() throws Exception {
        testLiveZeroRowCommitForceReleases();
    }

    @Test
    public void testLiveZeroRowCommitForceReleasesV2() throws Exception {
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testLiveZeroRowCommitForceReleases();
    }

    @Test
    public void testPersistedStructuralBarrierForceReleasesV1() throws Exception {
        testPersistedStructuralBarrierForceReleases();
    }

    @Test
    public void testPersistedStructuralBarrierForceReleasesV2() throws Exception {
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testPersistedStructuralBarrierForceReleases();
    }

    @Test
    public void testPersistedZeroRowBarrierIsConservativeOnV1() throws Exception {
        testPersistedZeroRowBarrier(false);
    }

    @Test
    public void testPersistedZeroRowBarrierForceReleasesV2() throws Exception {
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testPersistedZeroRowBarrier(true);
    }

    @Test
    public void testRenameForceReleasesAndAppliesWithUpdatedToken() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken oldToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(oldToken);

            try (
                    WalWriter writer = engine.getWalWriter(oldToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
            }

            execute("rename table x to y");
            Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
            Assert.assertNull(tracker.getReorderTimer());

            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                applyJob.drain(0);
            }
            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testStoredAndEffectiveTableOverrides() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        assertMemoryLeak(() -> {
            execute("create table disabled (ts timestamp) timestamp(ts) partition by day wal " +
                    "with walApplyReorderWindow = 0");
            execute("create table enabled (ts timestamp) timestamp(ts) partition by day wal " +
                    "with walApplyReorderWindow = 25ms");
            execute("create table inherited (ts timestamp) timestamp(ts) partition by day wal");
            execute("create table plain (ts timestamp) timestamp(ts) partition by day");

            assertQuery(
                    "select table_name, walApplyReorderWindow, walApplyReorderWindowEffective " +
                            "from tables() where table_name in ('disabled', 'enabled', 'inherited', 'plain') " +
                            "order by table_name"
            )
                    .noLeakCheck()
                    .returns(
                            "table_name\twalApplyReorderWindow\twalApplyReorderWindowEffective\n" +
                                    "disabled\t0\t0\n" +
                                    "enabled\t25000\t25000\n" +
                                    "inherited\tnull\t100000\n" +
                                    "plain\tnull\t0\n"
                    );

            execute("alter table enabled set param walApplyReorderWindow = 50ms");
            drainWalQueue();
            assertQuery(
                    "select walApplyReorderWindow, walApplyReorderWindowEffective " +
                            "from tables() where table_name = 'enabled'"
            )
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(
                            "walApplyReorderWindow\twalApplyReorderWindowEffective\n" +
                                    "50000\t50000\n"
                    );

            execute("alter table enabled set param walApplyReorderWindow = default");
            drainWalQueue();
            assertQuery(
                    "select walApplyReorderWindow, walApplyReorderWindowEffective " +
                            "from tables() where table_name = 'enabled'"
            )
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(
                            "walApplyReorderWindow\twalApplyReorderWindowEffective\n" +
                                    "null\t100000\n"
                    );
        });
    }

    @Test
    public void testInheritedMaterializedViewWindowIsZeroButExplicitOverrideWins() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by day wal"
            );
            execute(
                    "create materialized view price_1h as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h"
            );
            execute(
                    "create materialized view price_1d as " +
                            "select ts, avg(price) as price from price_1h sample by 1d"
            );

            assertQuery(
                    "select table_name, walApplyReorderWindow, walApplyReorderWindowEffective " +
                            "from tables() where table_name in ('base_price', 'price_1h', 'price_1d') order by table_name"
            )
                    .noLeakCheck()
                    .returns(
                            "table_name\twalApplyReorderWindow\twalApplyReorderWindowEffective\n" +
                                    "base_price\tnull\t100000\n" +
                                    "price_1d\tnull\t0\n" +
                                    "price_1h\tnull\t0\n"
                    );

            execute("alter materialized view price_1h set param walApplyReorderWindow = 25ms");
            drainWalQueue();
            assertQuery(
                    "select walApplyReorderWindow, walApplyReorderWindowEffective " +
                            "from tables() where table_name = 'price_1h'"
            )
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(
                            "walApplyReorderWindow\twalApplyReorderWindowEffective\n" +
                                    "25000\t25000\n"
                    );
        });
    }

    @Test
    public void testLegacyMetadataReadsReorderWindowAsInherit() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x (ts timestamp, value int) timestamp(ts) partition by day wal " +
                            "with walApplyReorderWindow = 25ms"
            );
            final TableToken tableToken = engine.verifyTableName("x");

            try (
                    MemoryMARW mem = Vm.getCMARWInstance();
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME);
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                final int versionField = mem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                mem.putLong(TableUtils.META_OFFSET_WAL_APPLY_REORDER_WINDOW, 123_456);
                mem.putInt(
                        TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION,
                        Numbers.encodeLowHighShorts(
                                Numbers.decodeLowShort(versionField),
                                TableUtils.META_FORMAT_MINOR_VERSION_TABLE_FORMAT
                        )
                );
            }

            try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken)) {
                metadata.loadMetadata();
                Assert.assertEquals(
                        TableUtils.WAL_APPLY_REORDER_WINDOW_INHERIT,
                        metadata.getWalApplyReorderWindow()
                );
            }
        });
    }

    @Test
    public void testNegativeTableWindowsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            assertNegativeWindowRejected(
                    "create table x (ts timestamp) timestamp(ts) partition by day wal " +
                            "with walApplyReorderWindow = -1ms"
            );
            execute("create table x (ts timestamp) timestamp(ts) partition by day wal");
            assertNegativeWindowRejected("alter table x set param walApplyReorderWindow = -1ms");
        });
    }

    @Test
    public void testSuspendExpiryAndResumePreservePendingWork() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);

                execute("alter table x suspend wal");
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                setCurrentMicros(1_100_000);
                timer.expire();
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                applyJob.drain(0);
                assertQuery("select count() from x")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n0\n");

                execute("alter table x resume wal");
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testTableParameterChangeReleasesCurrentWindowAndChangesNextDeadline() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());

                execute("alter table x set param walApplyReorderWindow = 200ms");
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertNull(tracker.getReorderTimer());
                applyJob.drain(0);
                Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());

                setCurrentMicros(2_000_000);
                appendRange(writer, "2024-01-01T00:01:00.000000Z", 1, 2);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(2_200_000, tracker.getDeferredDeadlineMicros());

                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                setCurrentMicros(2_200_000);
                timer.expire();
                applyJob.drain(0);
            }

            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n2\n");
        });
    }

    @Test
    public void testZeroWindowDisorderedBaselineRewritesFirstBatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter secondWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:01:00.000000Z", 10, 100);
                firstWriter.commit();
                applyJob.drain(0);

                appendRange(secondWriter, "2024-01-01T00:00:00.000000Z", 10, 0);
                secondWriter.commit();
                applyJob.drain(0);
            }

            assertQuery("select count(), min(value), max(value) from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tmin\tmax\n20\t0\t109\n");
            Assert.assertEquals(
                    30,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testTwoWritersAreReleasedAtFixedDeadlineAndAppliedTogether() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter secondWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:01:00.000000Z", 10, 100);
                firstWriter.commit();

                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());
                Assert.assertNotNull(tracker.getReorderTimer());
                assertQuery("select count() from x")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n0\n");

                setCurrentMicros(1_050_000);
                appendRange(secondWriter, "2024-01-01T00:00:00.000000Z", 10, 0);
                secondWriter.commit();

                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());
                Assert.assertEquals(2, tracker.getSeqTxn());

                setCurrentMicros(1_100_000);
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                timer.expire();
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                applyJob.drain(0);
            }

            assertQuery("select ts, value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(
                            "ts\tvalue\n" +
                                    "2024-01-01T00:00:00.000000Z\t0\n" +
                                    "2024-01-01T00:00:01.000000Z\t1\n" +
                                    "2024-01-01T00:00:02.000000Z\t2\n" +
                                    "2024-01-01T00:00:03.000000Z\t3\n" +
                                    "2024-01-01T00:00:04.000000Z\t4\n" +
                                    "2024-01-01T00:00:05.000000Z\t5\n" +
                                    "2024-01-01T00:00:06.000000Z\t6\n" +
                                    "2024-01-01T00:00:07.000000Z\t7\n" +
                                    "2024-01-01T00:00:08.000000Z\t8\n" +
                                    "2024-01-01T00:00:09.000000Z\t9\n" +
                                    "2024-01-01T00:01:00.000000Z\t100\n" +
                                    "2024-01-01T00:01:01.000000Z\t101\n" +
                                    "2024-01-01T00:01:02.000000Z\t102\n" +
                                    "2024-01-01T00:01:03.000000Z\t103\n" +
                                    "2024-01-01T00:01:04.000000Z\t104\n" +
                                    "2024-01-01T00:01:05.000000Z\t105\n" +
                                    "2024-01-01T00:01:06.000000Z\t106\n" +
                                    "2024-01-01T00:01:07.000000Z\t107\n" +
                                    "2024-01-01T00:01:08.000000Z\t108\n" +
                                    "2024-01-01T00:01:09.000000Z\t109\n"
                    );
            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            Assert.assertEquals(
                    20,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testNonDeferredQwpFramesAreAppliedTogether() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (value int, ts timestamp) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final LineHttpProcessorConfiguration lineConfiguration =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            final QwpIngressProcessorState state =
                    new QwpIngressProcessorState(1024, 4096, engine, lineConfiguration);

            try (
                    QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                    QwpTableBuffer tableBuffer = new QwpTableBuffer("x");
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                sendQwpRange(
                        state,
                        encoder,
                        tableBuffer,
                        "2024-01-01T00:01:00.000000Z",
                        10,
                        100
                );
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());

                setCurrentMicros(1_050_000);
                sendQwpRange(
                        state,
                        encoder,
                        tableBuffer,
                        "2024-01-01T00:00:00.000000Z",
                        10,
                        0
                );
                Assert.assertEquals(2, tracker.getSeqTxn());
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());

                setCurrentMicros(1_100_000);
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                timer.expire();
                applyJob.drain(0);
            } finally {
                state.onDisconnected();
                state.close();
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns(
                            "value\n" +
                                    "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n" +
                                    "100\n101\n102\n103\n104\n105\n106\n107\n108\n109\n"
                    );
        });
    }

    @Test
    public void testEmptyParquetTableAppliesReleasedWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (ts timestamp, value int) " +
                            "timestamp(ts) partition by day format parquet wal"
            );
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter secondWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:01:00.000000Z", 10, 100);
                firstWriter.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                setCurrentMicros(1_050_000);
                appendRange(secondWriter, "2024-01-01T00:00:00.000000Z", 10, 0);
                secondWriter.commit();

                releaseAndDrain(tracker, applyJob, 1_100_000);
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("select name, isParquet from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("name\tisParquet\n2024-01-01\ttrue\n");
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns(
                            "value\n" +
                                    "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n" +
                                    "100\n101\n102\n103\n104\n105\n106\n107\n108\n109\n"
                    );
        });
    }

    @Test
    public void testExistingParquetLastPartitionAppliesReleasedWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (ts timestamp, value int) " +
                            "timestamp(ts) partition by day format parquet wal " +
                            "with walApplyReorderWindow = 0"
            );
            execute("insert into x values ('2024-01-01T00:02:00.000000Z', 200)");
            drainWalQueue();
            assertQuery("select name, isParquet from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("name\tisParquet\n2024-01-01\ttrue\n");

            execute("alter table x set param walApplyReorderWindow = 100ms");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter secondWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:01:00.000000Z", 10, 100);
                firstWriter.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                setCurrentMicros(1_050_000);
                appendRange(secondWriter, "2024-01-01T00:00:00.000000Z", 10, 0);
                secondWriter.commit();

                releaseAndDrain(tracker, applyJob, 1_100_000);
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("select name, isParquet from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("name\tisParquet\n2024-01-01\ttrue\n");
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns(
                            "value\n" +
                                    "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n" +
                                    "100\n101\n102\n103\n104\n105\n106\n107\n108\n109\n" +
                                    "200\n"
                    );
        });
    }

    @Test
    public void testDeduplicationAcrossReleasedWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (ts timestamp, value int) timestamp(ts) partition by day " +
                            "wal dedup upsert keys(ts)"
            );
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter secondWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:00:05.000000Z", 10, 100);
                firstWriter.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                setCurrentMicros(1_050_000);
                appendRange(secondWriter, "2024-01-01T00:00:00.000000Z", 10, 0);
                secondWriter.commit();

                releaseAndDrain(tracker, applyJob, 1_100_000);
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns(
                            "value\n" +
                                    "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n" +
                                    "105\n106\n107\n108\n109\n"
                    );
        });
    }

    @Test
    public void testQueueFullResetPreservesDeferredAndReleasedStates() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            execute("create table blocker (ts timestamp) timestamp(ts) partition by day wal");
            execute("alter table blocker suspend wal");
            drainWalQueue();

            final TableToken tableToken = engine.verifyTableName("x");
            final TableToken blockerToken = engine.verifyTableName("blocker");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                final long deadline = tracker.getDeferredDeadlineMicros();
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);

                for (int i = 0; i < 8; i++) {
                    Assert.assertTrue(engine.notifyWalTxnCommitted(blockerToken));
                }

                // A young-window notification that cannot be enqueued performs the
                // normal UNINITIALIZED_TXN reset without discarding reorder state.
                Assert.assertFalse(engine.notifyWalTxnCommitted(tableToken));
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(deadline, tracker.getDeferredDeadlineMicros());
                Assert.assertSame(timer, tracker.getReorderTimer());

                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                setCurrentMicros(deadline);
                timer.expire();

                // Timer publication also loses to the full queue. RELEASED must
                // survive its second UNINITIALIZED_TXN reset until the republisher
                // can enqueue the table after queue capacity is available.
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertNull(tracker.getReorderTimer());
                applyJob.drain(0);
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());

                final CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);
                checkJob.runSerially();
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testDuplicateNotificationsDoNotMoveDeadlineOrDuplicateApply() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                final long deadline = tracker.getDeferredDeadlineMicros();
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);

                Assert.assertTrue(engine.notifyWalTxnCommitted(tableToken));
                Assert.assertTrue(engine.notifyWalTxnCommitted(tableToken));
                Assert.assertTrue(engine.notifyWalTxnCommitted(tableToken));
                applyJob.drain(0);

                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(deadline, tracker.getDeferredDeadlineMicros());
                Assert.assertSame(timer, tracker.getReorderTimer());

                releaseAndDrain(tracker, applyJob, deadline);
            }

            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testWriterContentionPreservesReleasedBacklog() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(walWriter, "2024-01-01T00:00:00.000000Z", 1, 1);
                walWriter.commit();
                applyJob.run();

                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                setCurrentMicros(1_100_000);

                try (TableWriter ignored = engine.getWriter(tableToken, "reorder contention test")) {
                    timer.expire();
                    applyJob.run();
                    Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                }

                new CheckWalTransactionsJob(engine).runSerially();
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testTimeQuotaContinuationDoesNotOpenAnotherWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
        final long[] clockMicros = {1_000_000};
        final boolean[] advanceClock = {false};
        testMicrosClock = () -> advanceClock[0] ? clockMicros[0]++ : clockMicros[0];
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                for (int i = 0; i < 6; i++) {
                    appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, i);
                    writer.commit();
                    if (i == 0) {
                        applyJob.run();
                        Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                    }
                }

                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                clockMicros[0] = 1_100_000;
                timer.expire();

                // Make every clock read consume the zero quota. This ensures the
                // lookahead and outer apply loop eject after their first progress,
                // instead of observing a frozen test clock forever.
                advanceClock[0] = true;
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertTrue(tracker.getWriterTxn() >= 1);
                Assert.assertTrue(tracker.getWriterTxn() < tracker.getSeqTxn());

                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select count() from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n6\n");
        });
    }

    @Test
    public void testPressureBackoffPreservesReleasedBacklog() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setProperty(PropertyKey.CAIRO_WAL_SEQUENCER_CHECK_INTERVAL, 1);
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();

                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                setCurrentMicros(1_100_000);
                timer.expire();

                tracker.getMemPressureControl().onOutOfMemory();
                Assert.assertFalse(tracker.getMemPressureControl().isReadyToProcess());
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertEquals(0, tracker.getWriterTxn());

                setCurrentMicros(11_100_000);
                Assert.assertTrue(tracker.getMemPressureControl().isReadyToProcess());
                checkJob.runSerially();
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testRestartReconstructsDeadlineAndUsesChangedDefault() throws Exception {
        final String restartRoot = temp.newFolder("wal-reorder-restart").getAbsolutePath();
        assertMemoryLeak(() -> {
            WalApplyReorderTimer shutdownTimer;

            QuestDBTestNode node = openRestartNode(restartRoot, 100_000, 1_000_000);
            try {
                final CairoEngine restartEngine = node.getEngine();
                restartEngine.execute(
                        "create table x (ts timestamp, value int) timestamp(ts) partition by day wal",
                        node.getSqlExecutionContext()
                );
                final TableToken tableToken = restartEngine.verifyTableName("x");
                final SeqTxnTracker tracker =
                        restartEngine.getTableSequencerAPI().getTxnTracker(tableToken);
                try (
                        WalWriter writer = restartEngine.getWalWriter(tableToken);
                        ApplyWal2TableJob applyJob = createWalApplyJob(node)
                ) {
                    appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                    writer.commit();
                    applyJob.run();
                    Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                    Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());
                    shutdownTimer = tracker.getReorderTimer();
                    Assert.assertNotNull(shutdownTimer);
                }

                restartEngine.signalClose();
                Assert.assertTrue(shutdownTimer.isCancelled());
                Assert.assertNull(tracker.getReorderTimer());
                Assert.assertFalse(tracker.isSuspended());
            } finally {
                closeRestartNode(node);
            }

            // Before the original deadline, restart reconstructs the same fixed
            // deadline from the first pending transaction's durable commit time.
            node = openRestartNode(restartRoot, 100_000, 1_050_000);
            try {
                final CairoEngine restartEngine = node.getEngine();
                final TableToken tableToken = restartEngine.verifyTableName("x");
                new CheckWalTransactionsJob(restartEngine).runSerially();
                try (ApplyWal2TableJob applyJob = createWalApplyJob(node)) {
                    applyJob.run();
                }
                final SeqTxnTracker tracker =
                        restartEngine.getTableSequencerAPI().getTxnTracker(tableToken);
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_100_000, tracker.getDeferredDeadlineMicros());
            } finally {
                closeRestartNode(node);
            }

            // Reorder state itself is intentionally not persisted. A changed
            // inherited server default therefore governs reconstruction.
            node = openRestartNode(restartRoot, 200_000, 1_150_000);
            try {
                final CairoEngine restartEngine = node.getEngine();
                final TableToken tableToken = restartEngine.verifyTableName("x");
                new CheckWalTransactionsJob(restartEngine).runSerially();
                try (ApplyWal2TableJob applyJob = createWalApplyJob(node)) {
                    applyJob.run();
                }
                final SeqTxnTracker tracker =
                        restartEngine.getTableSequencerAPI().getTxnTracker(tableToken);
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertEquals(1_200_000, tracker.getDeferredDeadlineMicros());
            } finally {
                closeRestartNode(node);
            }

            // At the reconstructed deadline, startup recovery applies immediately.
            node = openRestartNode(restartRoot, 200_000, 1_200_000);
            try {
                final CairoEngine restartEngine = node.getEngine();
                final TableToken tableToken = restartEngine.verifyTableName("x");
                new CheckWalTransactionsJob(restartEngine).runSerially();
                try (ApplyWal2TableJob applyJob = createWalApplyJob(node)) {
                    applyJob.drain(0);
                }
                final SeqTxnTracker tracker =
                        restartEngine.getTableSequencerAPI().getTxnTracker(tableToken);
                Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
                try (TableReader reader = restartEngine.getReader(tableToken)) {
                    Assert.assertEquals(1, reader.size());
                }
            } finally {
                closeRestartNode(node);
            }
        });
    }

    @Test
    public void testCommitAfterExpiryBeforeDequeueJoinsReleasedRun() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter secondWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:01:00.000000Z", 1, 100);
                firstWriter.commit();
                applyJob.run();

                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);
                Assert.assertTrue(engine.getTimerShards().unregister(timer));
                setCurrentMicros(1_100_000);
                timer.expire();
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());

                // The timer notification is queued but not consumed yet.
                appendRange(secondWriter, "2024-01-01T00:00:00.000000Z", 1, 0);
                secondWriter.commit();
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n0\n100\n");
        });
    }

    @Test
    public void testMixedTablesDelayOnlyPositiveEffectiveWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table delayed (ts timestamp, value int) timestamp(ts) partition by day wal");
            execute(
                    "create table immediate (ts timestamp, value int) timestamp(ts) partition by day wal " +
                            "with walApplyReorderWindow = 0"
            );
            final TableToken delayedToken = engine.verifyTableName("delayed");
            final TableToken immediateToken = engine.verifyTableName("immediate");
            final SeqTxnTracker delayedTracker =
                    engine.getTableSequencerAPI().getTxnTracker(delayedToken);
            final SeqTxnTracker immediateTracker =
                    engine.getTableSequencerAPI().getTxnTracker(immediateToken);

            try (
                    WalWriter delayedWriter = engine.getWalWriter(delayedToken);
                    WalWriter immediateWriter = engine.getWalWriter(immediateToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(delayedWriter, "2024-01-01T00:00:00.000000Z", 1, 1);
                delayedWriter.commit();
                appendRange(immediateWriter, "2024-01-01T00:00:00.000000Z", 1, 2);
                immediateWriter.commit();
                applyJob.drain(0);

                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, delayedTracker.getReorderState());
                Assert.assertEquals(SeqTxnTracker.REORDER_NONE, immediateTracker.getReorderState());
                Assert.assertNull(immediateTracker.getReorderTimer());
                assertQuery("select count() from delayed")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n0\n");
                assertQuery("select count() from immediate")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n1\n");

                releaseAndDrain(delayedTracker, applyJob, 1_100_000);
            }

            assertQuery("select count() from delayed")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testCheckpointAndWalPurgePreserveDeferredTransaction() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
            }

            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                applyJob.run();
                final WalApplyReorderTimer timer = tracker.getReorderTimer();
                Assert.assertNotNull(timer);

                engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
                engine.checkpointRelease();
                drainPurgeJob();

                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertSame(timer, tracker.getReorderTimer());
                releaseAndDrain(tracker, applyJob, 1_100_000);
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    @Test
    public void testWalUpdateReturnsBeforeVisibleApplyAndForceReleases() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (ts timestamp, value int) timestamp(ts) partition by day wal " +
                            "with walApplyReorderWindow = 0"
            );
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("alter table x set param walApplyReorderWindow = 100ms");
            drainWalQueue();

            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            setCurrentMicros(2_000_000);
            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:01:00.000000Z", 1, 2);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                // UPDATE acknowledgement remains the sequencer boundary. Its
                // visible effect is still behind writerTxn until WAL apply runs.
                execute("update x set value = 3 where value = 1");
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                assertQuery("select value from x")
                        .noLeakCheck()
                        .expectSize()
                        .returns("value\n1\n");

                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n3\n2\n");
        });
    }

    @Test
    public void testTruncateForceReleasesDeferredData() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (ts timestamp, value int) timestamp(ts) partition by day wal " +
                            "with walApplyReorderWindow = 0"
            );
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.drain(0);

                execute("alter table x set param walApplyReorderWindow = default");
                applyJob.drain(0);

                setCurrentMicros(2_000_000);
                appendRange(writer, "2024-01-01T00:01:00.000000Z", 1, 2);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                execute("truncate table x");
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertNull(tracker.getReorderTimer());
                applyJob.drain(0);
            }

            assertQuery("select count() from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testReplaceRangeDataJoinsWindowAndEmptyRangeForceReleases() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            final long rangeLo = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");

            try (
                    WalWriter firstWriter = engine.getWalWriter(tableToken);
                    WalWriter replaceWriter = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(firstWriter, "2024-01-01T00:00:00.000000Z", 5, 0);
                firstWriter.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                appendRange(replaceWriter, "2024-01-01T00:00:00.000000Z", 2, 100);
                replaceWriter.commitWithParams(
                        rangeLo,
                        rangeLo + 3 * Micros.SECOND_MICROS,
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                releaseAndDrain(tracker, applyJob, 1_100_000);

                assertQuery("select value from x order by ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("value\n100\n101\n3\n4\n");

                setCurrentMicros(2_000_000);
                appendRange(firstWriter, "2024-01-01T00:00:10.000000Z", 1, 10);
                firstWriter.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());

                replaceWriter.commitWithParams(
                        rangeLo + 10 * Micros.SECOND_MICROS,
                        rangeLo + 11 * Micros.SECOND_MICROS,
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                applyJob.drain(0);
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("select value from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n100\n101\n3\n4\n");
        });
    }

    private static void appendRange(WalWriter writer, CharSequence timestamp, int rowCount, int valueOffset) {
        final long timestampMicros = MicrosTimestampDriver.floor(timestamp);
        for (int i = 0; i < rowCount; i++) {
            final TableWriter.Row row = writer.newRow(timestampMicros + i * Micros.SECOND_MICROS);
            row.putInt(1, valueOffset + i);
            row.append();
        }
    }

    private static void assertNegativeWindowRejected(CharSequence sql) throws Exception {
        try {
            execute(sql);
            Assert.fail("negative WAL apply reorder window was accepted");
        } catch (SqlException ex) {
            TestUtils.assertContainsEither(
                    ex.getFlyweightMessage(),
                    "walApplyReorderWindow must be non negative",
                    "invalid interval qualifier -"
            );
        }
    }

    private static void sendQwpRange(
            QwpIngressProcessorState state,
            QwpWebSocketEncoder encoder,
            QwpTableBuffer tableBuffer,
            CharSequence timestamp,
            int rowCount,
            int valueOffset
    ) {
        final QwpTableBuffer.ColumnBuffer valueColumn =
                tableBuffer.getOrCreateColumn("value", TYPE_INT, false);
        final QwpTableBuffer.ColumnBuffer timestampColumn =
                tableBuffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
        final long timestampMicros = MicrosTimestampDriver.floor(timestamp);
        for (int i = 0; i < rowCount; i++) {
            valueColumn.addInt(valueOffset + i);
            timestampColumn.addLong(timestampMicros + i * Micros.SECOND_MICROS);
            tableBuffer.nextRow();
        }

        final int size = encoder.encode(tableBuffer);
        final QwpBufferWriter buffer = encoder.getBuffer();
        state.addData(buffer.getBufferPtr(), buffer.getBufferPtr() + size);
        Assert.assertFalse(state.isDeferCommit());
        state.processMessage();
        Assert.assertTrue(state.getErrorText().toString(), state.isOk());
        state.commit();
        Assert.assertTrue(state.getErrorText().toString(), state.isOk());
        state.clear();
        tableBuffer.reset();
    }

    private static QuestDBTestNode openRestartNode(
            String restartRoot,
            long reorderWindowMicros,
            long currentMicros
    ) {
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, reorderWindowMicros);
        overrides.setCurrentMicros(currentMicros);
        final QuestDBTestNode node = new QuestDBTestNode(42);
        node.initCairo(
                restartRoot,
                false,
                overrides,
                CairoEngine::new,
                CairoTestConfiguration::new
        );
        node.getEngine().load();
        node.initGriffin();
        node.setUpGriffin();
        return node;
    }

    private static void closeRestartNode(QuestDBTestNode node) {
        if (node != null) {
            node.getEngine().signalClose();
            node.closeCairo();
        }
    }

    private void releaseAndDrain(
            SeqTxnTracker tracker,
            ApplyWal2TableJob applyJob,
            long deadlineMicros
    ) {
        setCurrentMicros(deadlineMicros);
        final WalApplyReorderTimer timer = tracker.getReorderTimer();
        Assert.assertNotNull(timer);
        Assert.assertTrue(engine.getTimerShards().unregister(timer));
        timer.expire();
        Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
        applyJob.drain(0);
        Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
    }

    private void testLiveZeroRowCommitForceReleases() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (
                    WalWriter writer = engine.getWalWriter(tableToken);
                    ApplyWal2TableJob applyJob = createWalApplyJob()
            ) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
                applyJob.run();
                Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                Assert.assertNotNull(tracker.getReorderTimer());

                execute("update x set value = value where value < 0");
                Assert.assertEquals(SeqTxnTracker.REORDER_RELEASED, tracker.getReorderState());
                Assert.assertNull(tracker.getReorderTimer());
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }

    private void testPersistedStructuralBarrierForceReleases() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
            }
            execute("alter table x add column extra long");

            engine.getTableSequencerAPI().releaseAll();
            new CheckWalTransactionsJob(engine).runSerially();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                applyJob.run();
                applyJob.drain(0);
            }

            Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
            assertQuery("select value, extra from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\textra\n1\tnull\n");
        });
    }

    private void testPersistedZeroRowBarrier(boolean expectImmediateRelease) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW, "100ms");
        setCurrentMicros(1_000_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("x");
            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                appendRange(writer, "2024-01-01T00:00:00.000000Z", 1, 1);
                writer.commit();
            }
            execute("update x set value = value where value < 0");

            engine.getTableSequencerAPI().releaseAll();
            new CheckWalTransactionsJob(engine).runSerially();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);

            try (ApplyWal2TableJob applyJob = createWalApplyJob()) {
                applyJob.run();
                if (expectImmediateRelease) {
                    applyJob.drain(0);
                    Assert.assertEquals(SeqTxnTracker.REORDER_NONE, tracker.getReorderState());
                } else {
                    Assert.assertEquals(SeqTxnTracker.REORDER_DEFERRED, tracker.getReorderState());
                    final WalApplyReorderTimer timer = tracker.getReorderTimer();
                    Assert.assertNotNull(timer);
                    Assert.assertTrue(engine.getTimerShards().unregister(timer));
                    setCurrentMicros(1_100_000);
                    timer.expire();
                    applyJob.drain(0);
                }
            }

            assertQuery("select value from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n1\n");
        });
    }
}
