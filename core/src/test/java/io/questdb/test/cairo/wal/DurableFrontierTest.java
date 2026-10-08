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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.crash.AbstractCrashConsistencyTest;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.ExposedFlusher;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.FailingSeqFdatasyncFacade;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.FailingWalFdatasyncFacade;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.WalFdatasyncFacade;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.questdb.test.cairo.wal.GroupCommitTestUtils.assertDataSyncFailure;
import static io.questdb.test.cairo.wal.GroupCommitTestUtils.commitRow;
import static io.questdb.test.cairo.wal.GroupCommitTestUtils.markDistressed;

/**
 * The local durable frontier under adaptive group commit ({@code cairo.adaptive.commit.group.window}, W). At
 * W=0 a commit is durable before it returns. At W&gt;0 each commit syncs its writer-private WAL files before it
 * is sequenced, the shared sequencer flush is batched, and the frontier lags until a trigger flushes the batch.
 * The frontier must never cover a transaction whose WAL files and sequencer record are not durable, and must not
 * move after a failed barrier.
 */
public class DurableFrontierTest extends AbstractCrashConsistencyTest {
    private static final byte CUSTOM_EVENT_TYPE = 64;
    private static final long T0 = 2_000_000L;
    // The crash harness reads columns as strings.
    private static final String V = "cast(v as string)";
    private static final long WINDOW_US = 1_000_000L;

    @Test
    public void testBackgroundFdatasyncFailurePoisonsEngineWithoutRetryOrFrontierAdvance() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        final FailingWalFdatasyncFacade ff = new FailingWalFdatasyncFacade();
        try {
            assertMemoryLeak(ff, () -> {
                final SeqTxnTracker tracker = createTable("x");
                final TableToken tt = engine.verifyTableName("x");
                final AtomicInteger fatalCalls = new AtomicInteger();
                engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());

                try (WalWriter writer = engine.getWalWriter(tt); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                    commitRow(writer, 0, 1);
                    final long durableBefore = tracker.getLocalDurableSeqTxn();
                    ff.failNext();
                    setCurrentMicros(T0 + WINDOW_US + 1);
                    try {
                        flusher.flushNow();
                        Assert.fail("background fdatasync failure must remain fatal");
                    } catch (CairoError expected) {
                        Assert.assertTrue(CairoException.isDataSyncFailure(expected));
                    }
                    Assert.assertEquals(1, fatalCalls.get());
                    Assert.assertEquals(1, ff.failureAttempts);
                    Assert.assertTrue(engine.isDurabilityFailed());
                    Assert.assertEquals(durableBefore, tracker.getLocalDurableSeqTxn());

                    // A poisoned engine denies durable-ack handshakes and reports no frontier to a connection that
                    // already holds a grant, so the parked ack for the never-synced commit is never sent.
                    final DurableAckRegistry registry = engine.getDurableAckRegistry();
                    Assert.assertFalse(registry.isEnabled());
                    Assert.assertFalse(registry.isTierAvailable(DurabilityTier.LOCAL));
                    Assert.assertFalse(registry.isTierSetAvailable(DurabilityTier.LOCAL));
                    Assert.assertEquals(-1L, registry.getLocalDurableSeqTxn(tt.getDirName()));

                    try {
                        commitRow(writer, 60_000_000L, 2);
                        Assert.fail("distressed writer must not be reusable");
                    } catch (CairoException | CairoError expected) {
                        // no second fdatasync is allowed after the first indeterminate failure
                    }
                    Assert.assertEquals(1, ff.failureAttempts);
                    try {
                        engine.getWalWriter(tt);
                        Assert.fail("poisoned engine must reject new WAL writers");
                    } catch (CairoError expected) {
                        TestUtils.assertContains(expected.getMessage(), "engine is poisoned");
                    }
                }
            });
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    /**
     * {@code fdatasyncTxnLog} releases the sequencer write lock before the flusher reaches
     * {@code markWriterDurable}, so the writer's own next commit can be sequenced in between. The flusher must
     * not claim that commit: a tracker subclass parks the flusher inside {@code markWriterDurable} until the
     * commit is sequenced, and the {@code _txnlog} fdatasync samples the highest seqTxn it can have covered.
     */
    @Test
    public void testBackgroundFlusherDoesNotOverClaimWritersOwnNextCommit() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        final CountDownLatch flusherInMark = new CountDownLatch(1);
        final CountDownLatch nextCommitSequenced = new CountDownLatch(1);
        final AtomicReference<Thread> flusherThread = new AtomicReference<>();
        final AtomicReference<SeqTxnTracker> trackerRef = new AtomicReference<>();
        final AtomicLong seqTxnAtLastTxnlogFdatasync = new AtomicLong(-1);
        final WalFdatasyncFacade ff = new WalFdatasyncFacade() {
            @Override
            public void fdatasync(long fd) {
                super.fdatasync(fd);
                final String p = fdToPath.get(fd);
                final SeqTxnTracker t = trackerRef.get();
                if (t != null && p != null && (p.endsWith(WalUtils.TXNLOG_FILE_NAME) || p.endsWith(WalUtils.TXNLOG_FILE_NAME + "."))) {
                    seqTxnAtLastTxnlogFdatasync.set(t.getSeqTxn()); // under the sequencer write lock
                }
            }
        };

        assertMemoryLeak(ff, () -> {
            final TableSequencerAPI seqApi = engine.getTableSequencerAPI();
            final Field factoryField = TableSequencerAPI.class.getDeclaredField("createTxnTracker");
            factoryField.setAccessible(true);
            final Object originalFactory = factoryField.get(seqApi);
            final Function<CharSequence, SeqTxnTracker> racingFactory = dir -> {
                final SeqTxnTracker t = new SeqTxnTracker(engine.getConfiguration()) {
                    @Override
                    public void markWriterDurable(int walId, long orphanSweepMark, long coveredSeqTxn) {
                        if (Thread.currentThread() == flusherThread.get()) {
                            flusherInMark.countDown();
                            try {
                                Assert.assertTrue(nextCommitSequenced.await(10, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        super.markWriterDurable(walId, orphanSweepMark, coveredSeqTxn);
                    }

                    @Override
                    public boolean notifyOnCommit(long newSeqTxn) {
                        final boolean r = super.notifyOnCommit(newSeqTxn);
                        if (flusherInMark.getCount() == 0) {
                            nextCommitSequenced.countDown(); // ingest thread, inside the sequencer write lock
                        }
                        return r;
                    }
                };
                trackerRef.set(t);
                return t;
            };
            factoryField.set(seqApi, racingFactory);
            try {
                final SeqTxnTracker tracker = createTable("x");
                Assert.assertSame(trackerRef.get(), tracker);
                try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x")); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                    commitRow(w, 0L, 1);
                    final long firstSeqTxn = tracker.getSeqTxn();
                    setCurrentMicros(T0 + WINDOW_US + 1);
                    final AtomicReference<Throwable> flusherError = new AtomicReference<>();
                    final Thread t = new Thread(() -> {
                        try {
                            flusher.flushNow();
                        } catch (Throwable th) {
                            flusherError.set(th);
                        }
                    });
                    flusherThread.set(t);
                    t.start();
                    Assert.assertTrue(flusherInMark.await(10, TimeUnit.SECONDS));

                    // Sequenced while the flusher is parked; then waits on the writer monitor until it finishes.
                    commitRow(w, 60_000_000L, 2);
                    t.join(10_000);
                    Assert.assertFalse(t.isAlive());
                    if (flusherError.get() != null) {
                        throw new AssertionError(flusherError.get());
                    }
                    final long secondSeqTxn = tracker.getSeqTxn();
                    Assert.assertEquals(firstSeqTxn + 1, secondSeqTxn);
                    Assert.assertEquals("no sequencer fdatasync ran after txn " + secondSeqTxn + " was sequenced",
                            firstSeqTxn, seqTxnAtLastTxnlogFdatasync.get());
                    Assert.assertEquals("durable frontier over-claims the writer's own un-fsynced txn " + secondSeqTxn,
                            firstSeqTxn, tracker.getLocalDurableSeqTxn());
                    Assert.assertEquals("pending batch must keep a contiguous-prefix pin", 1, tracker.getPendingWriterPinCount());
                }
            } finally {
                factoryField.set(seqApi, originalFactory);
            }
        });
    }

    /**
     * {@code openNewSegment()} closes and reopens the fds a background flush iterates; it deregisters the writer's
     * pending batch under the writer monitor first. A flush of a closed fd is counted as a failed flush.
     */
    @Test
    public void testBackgroundFlusherDoesNotRaceSegmentRoll() throws Exception {
        // 1us on the wall clock: the flusher fires on every pass.
        setGroupCommit(1, 16);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, "3");
        final int rows = 600;
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            assertNoFlushRace("a segment roll", () -> {
                try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x"))) {
                    for (int i = 0; i < rows; i++) {
                        commitRow(w, i * 1000L, i);
                    }
                }
            });
            drainWalQueue();
            assertQuery("select count() from x").noRandomAccess().expectSize().returns("count\n" + rows + "\n");
            assertQuery("select sum(v) from x").noRandomAccess().expectSize().returns("sum\n" + ((long) (rows - 1) * rows / 2) + "\n");
        });
    }

    /**
     * A writer closing with a pending batch drops it under the writer monitor before any fd is closed, so a
     * concurrent sweep finds nothing to flush.
     */
    @Test
    public void testBackgroundFlusherDoesNotRaceTeardown() throws Exception {
        setGroupCommit(1, 16);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            assertNoFlushRace("a writer teardown", () -> {
                for (int iter = 0; iter < 50; iter++) {
                    try (WalWriter w = engine.getWalWriter(tt)) {
                        for (int i = 0; i < 10; i++) {
                            commitRow(w, (long) (iter * 10 + i) * 1_000_000L, i);
                        }
                    }
                }
            });
        });
    }

    /**
     * W&gt;0: a custom event on an otherwise idle writer (the ENT partition-seal shape) enters the writer's pending
     * batch, so the pool-return flush releases its pin; otherwise the frontier freezes below the event.
     */
    @Test
    public void testCustomEventOnIdleWriterIsFlushedOnRelease() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            final SeqTxnTracker tracker = createTable("x");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 0)");
            Assert.assertEquals(tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());

            final long customSeqTxn;
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x"))) {
                ff.reset();
                customSeqTxn = w.appendCustomEvent(CUSTOM_EVENT_TYPE, mem -> mem.putLong(42L));
                Assert.assertTrue("the event file must be synced before sequencing", ff.eventFdatasyncs() > 0);
                Assert.assertTrue("custom event must stay pending", tracker.getLocalDurableSeqTxn() < customSeqTxn);
                Assert.assertEquals(1, tracker.getPendingWriterPinCount());
            }
            Assert.assertEquals(customSeqTxn, tracker.getLocalDurableSeqTxn());
            Assert.assertEquals(0, tracker.getPendingWriterPinCount());

            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 1)");
            Assert.assertEquals("a peer's later commit must carry the frontier past the custom event",
                    tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
        });
    }

    @Test
    public void testDiscardedWriterFlushFailureStopsTheEngine() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        final FailingSeqFdatasyncFacade ff = new FailingSeqFdatasyncFacade();
        try {
            assertMemoryLeak(ff, () -> {
                final SeqTxnTracker tracker = createTable("x");
                final AtomicInteger fatalCalls = new AtomicInteger();
                engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
                try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x"))) {
                    commitRow(w, 0L, 0);
                    Assert.assertTrue("test setup: the commit must wait for its batch flush", tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());
                    ff.failNextSequencerFdatasync();
                    markDistressed(w);
                }
                Assert.assertTrue("test setup: the discarded writer's flush must have failed", ff.injected);
                // The kernel reports a writeback error once, so no later flush of the log can prove the batch.
                Assert.assertTrue("a failed flush of the sequencer log must stop the engine", engine.isDurabilityFailed());
                Assert.assertEquals(1, fatalCalls.get());
                Assert.assertTrue(tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());
                try {
                    execute("insert into x values (60000000, 1)");
                    Assert.fail("a poisoned engine must not take a commit");
                } catch (CairoError expected) {
                    TestUtils.assertContains(expected.getMessage(), "engine is poisoned");
                }
            });
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    @Test
    public void testDropAndViewChangeFlushTheSequencerLogV1() throws Exception {
        assertDropAndViewChangeFlushTheSequencerLog(0);
    }

    @Test
    public void testDropAndViewChangeFlushTheSequencerLogV2() throws Exception {
        assertDropAndViewChangeFlushTheSequencerLog(16);
    }

    /**
     * A transaction whose frontier has advanced (the durable ack fired) survives a power loss end to end.
     */
    @Test
    public void testDurableAckedTxnSurvivesCrash() throws Exception {
        setGroupCommit(WINDOW_US, 0);
        // Epoch every apply batch, so recovery rebuilds the applied table from the durable WAL.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        runWithCrashFacade(() -> {
            final SeqTxnTracker tracker = createTable("t");
            final TableToken tt = engine.verifyTableName("t");
            markDurableBaseline();
            try (WalWriter w = engine.getWalWriter(tt); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                commitRow(w, 0, 10);
                setCurrentMicros(T0 + 1000);
                commitRow(w, 3_600_000_000L, 11);
                Assert.assertTrue("must be pending before flush", tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());
                setCurrentMicros(T0 + WINDOW_US + 1000);
                flusher.flushNow();
                Assert.assertEquals(tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
            }
            drainWalQueue();

            crashAndReopen();
            recoverAndApply(tt);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals("both acked rows must survive the crash", Arrays.asList("10", "11"), readColumn("t", V));
        });
    }

    @Test
    public void testDurableAppendsMoveTheDurableFrontierAdaptiveW0() throws Exception {
        assertDurableAppendsMoveTheDurableFrontier("adaptive", 0);
    }

    @Test
    public void testDurableAppendsMoveTheDurableFrontierGroupCommit() throws Exception {
        assertDurableAppendsMoveTheDurableFrontier("adaptive", WINDOW_US);
    }

    @Test
    public void testDurableAppendsMoveTheDurableFrontierNosync() throws Exception {
        assertDurableAppendsMoveTheDurableFrontier("nosync", 0);
    }

    @Test
    public void testDurableAppendsMoveTheDurableFrontierSync() throws Exception {
        assertDurableAppendsMoveTheDurableFrontier("sync", 0);
    }

    @Test
    public void testPeerCommitAfterFailedInlineSequencerBarrierDoesNotAdvanceFrontier() throws Exception {
        assertPeerBarrierAfterFailedSequencerBarrierDoesNotAdvanceFrontier(0);
    }

    @Test
    public void testPeerFlushAfterFailedSequencerBarrierDoesNotAdvanceFrontier() throws Exception {
        assertPeerBarrierAfterFailedSequencerBarrierDoesNotAdvanceFrontier(WINDOW_US);
    }

    /**
     * A writer torn down without a device flush orphans its pin rather than removing it (removing it would let a
     * peer's flush over-claim the writer's unflushed records), and the next peer flush reaps the orphan: every
     * writer syncs its private WAL files before sequencing, so one sequencer flush by anyone covers the dead
     * writer's records too.
     */
    @Test
    public void testTornDownWriterOrphansPinAndPeerFlushReapsIt() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        // The epoch would advance the frontier by a route that bypasses the pin map.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            final TableToken tt = engine.verifyTableName("x");
            final long deadSeqTxn;
            try (WalWriter a = engine.getWalWriter(tt)) {
                commitRow(a, 0L, 0);
                deadSeqTxn = tracker.getSeqTxn();
                Assert.assertEquals(1, tracker.getPendingWriterPinCount());
                a.simulatePowerLossDropPending();
            }
            Assert.assertEquals("a torn-down writer's pin must be orphaned, not removed", 1, tracker.getOrphanedWriterPinCount());
            Assert.assertEquals(1, tracker.getPendingWriterPinCount());
            Assert.assertTrue("the frontier must not cover a batch that was never flushed", tracker.getLocalDurableSeqTxn() < deadSeqTxn);

            try (WalWriter b = engine.getWalWriter(tt); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                setCurrentMicros(T0 + 1000);
                commitRow(b, 60_000_000L, 1);
                setCurrentMicros(T0 + WINDOW_US + 2000);
                flusher.flushNow();
                Assert.assertEquals("a peer's flush must reap the orphaned pin", 0, tracker.getOrphanedWriterPinCount());
                Assert.assertEquals(0, tracker.getPendingWriterPinCount());
                Assert.assertEquals(tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
            }
        });
    }

    /**
     * A W&gt;0 batch on a held writer stays pending, then one trigger makes it durable. For every trigger: each
     * commit syncs its private WAL files before it is sequenced and the sequencer flush is batched; after the
     * trigger the frontier covers the batch, no pin is left, and every sequencer part holding a durable
     * transaction was fdatasync'd. Failures of all rows are reported together.
     */
    @Test
    public void testTriggersMakeThePendingBatchDurable() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        final TriggerRow[] rows = {
                new TriggerRow("next commit after the window", 2, (w, f, t) -> {
                    setCurrentMicros(T0 + WINDOW_US + 50_000);
                    commitRow(w, 100 * 60_000_000L, 100);
                    return true;
                }),
                new TriggerRow("background flusher on an idle writer", 1, (w, f, t) -> {
                    final long pending = t.getSeqTxn();
                    setCurrentMicros(T0 + WINDOW_US / 2);
                    f.flushNow();
                    Assert.assertTrue("before the window ends the flusher must not flush", t.getLocalDurableSeqTxn() < pending);
                    setCurrentMicros(T0 + WINDOW_US + 50_000);
                    f.flushNow();
                    return true;
                }),
                new TriggerRow("background flusher after the clock steps back", 1, (w, f, t) -> {
                    setCurrentMicros(T0 - 1_000_000L);
                    f.flushNow();
                    return true;
                }),
                new TriggerRow("structural change", 2, (w, f, t) -> {
                    // Its fullSync flushes the sequencer, which must not outrun the pending column data.
                    setCurrentMicros(T0 + 50_000);
                    addColumn(w, "w", ColumnType.LONG);
                    return true;
                }),
                new TriggerRow("writer release", 1, (w, f, t) -> false),
                new TriggerRow("discarded writer", 1, (w, f, t) -> {
                    markDistressed(w);
                    return false;
                }),
                new TriggerRow("custom event joining the batch", 1, (w, f, t) -> {
                    final long dataSeqTxn = t.getSeqTxn();
                    final int eventSyncs = ffOf().eventFdatasyncs();
                    final int seqSyncs = ffOf().sequencerFdatasyncs();
                    Assert.assertEquals(dataSeqTxn + 1, w.appendCustomEvent(CUSTOM_EVENT_TYPE, mem -> mem.putLong(42L)));
                    Assert.assertTrue("the event file must be synced before sequencing", ffOf().eventFdatasyncs() > eventSyncs);
                    Assert.assertEquals("the event must not flush the shared sequencer", seqSyncs, ffOf().sequencerFdatasyncs());
                    Assert.assertTrue("the event must stay pending", t.getLocalDurableSeqTxn() < dataSeqTxn);
                    return false;
                }),
                new TriggerRow("release of a batch over rotated sequencer parts", 40, (w, f, t) -> false),
                new TriggerRow("release after the pool closed the part's sequencer", 15, (w, f, t) -> {
                    // Part 0 is full and unflushed; the next commit reopens the log and rotates away from it.
                    Assert.assertTrue(engine.getTableSequencerAPI().releaseInactive());
                    setCurrentMicros(T0 + 50_000);
                    commitRow(w, 100 * 60_000_000L, 100);
                    return false;
                }),
        };
        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        final List<String> failures = new ArrayList<>();
        assertMemoryLeak(ff, () -> {
            for (int r = 0; r < rows.length; r++) {
                final TriggerRow row = rows[r];
                try {
                    final SeqTxnTracker tracker = createTable("t" + r);
                    execute("insert into t" + r + " values (0, 0)");
                    ff.reset();
                    try (WalWriter w = engine.getWalWriter(engine.verifyTableName("t" + r)); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                        for (int i = 1; i <= row.commits; i++) {
                            setCurrentMicros(T0 + i * 1000L);
                            commitRow(w, i * 60_000_000L, i);
                        }
                        final long pending = tracker.getSeqTxn();
                        Assert.assertTrue("the batch must be pending", tracker.getLocalDurableSeqTxn() < pending);
                        Assert.assertTrue("every commit must sync its private WAL files before sequencing", ff.privateWalFdatasyncs() >= row.commits);
                        Assert.assertTrue("the sequencer flush must be batched", ff.sequencerFdatasyncs() < row.commits);
                        if (row.trigger.fire(w, flusher, tracker)) {
                            Assert.assertTrue("the trigger must make the batch durable", tracker.getLocalDurableSeqTxn() >= pending);
                        }
                    }
                    Assert.assertEquals(tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
                    Assert.assertEquals("no pin may be left", 0, tracker.getPendingWriterPinCount());
                    for (long part = 0, n = (tracker.getLocalDurableSeqTxn() - 1) / 16; part <= n; part++) {
                        Assert.assertTrue("part " + part + " holds durable txns but was never fdatasynced", ff.seqPartFdatasyncs(part) > 0);
                    }
                } catch (AssertionError e) {
                    failures.add(row.name + ": " + e.getMessage());
                }
            }
        });
        Assert.assertTrue(String.join("\n", failures), failures.isEmpty());
    }

    /**
     * Two writers share one tracker. A commits the lower seqTxn, B the higher, and B flushes its batch first while
     * A's commit is still in the page cache: the frontier must stay at the contiguous durable prefix. A power loss
     * before A flushes then keeps the acked baseline, and no surviving row is wrong.
     */
    @Test
    public void testTwoWriterOutOfOrderFlushDurableAckNeverOverClaimsAcrossCrash() throws Exception {
        setGroupCommit(WINDOW_US, 0);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        runWithCrashFacade(() -> {
            final SeqTxnTracker tracker = createTable("t");
            final TableToken tt = engine.verifyTableName("t");
            execute("insert into t values (0, 1)");
            drainWalQueue();
            final long baselineTxn = engine.getTableSequencerAPI().lastTxn(tt);
            Assert.assertEquals(baselineTxn, tracker.getLocalDurableSeqTxn());
            markDurableBaseline();

            WalWriter a = engine.getWalWriter(tt);
            WalWriter b = engine.getWalWriter(tt);
            try {
                Assert.assertNotEquals(a.getWalId(), b.getWalId());
                setCurrentMicros(T0 + 1000);
                commitRow(a, 3_600_000_000L, 10);
                final long seqA = tracker.getSeqTxn();
                setCurrentMicros(T0 + 2000);
                commitRow(b, 7_200_000_000L, 11);
                Assert.assertEquals(seqA + 1, tracker.getSeqTxn());

                // B's commit-driven flush past the window; A stays unflushed.
                setCurrentMicros(T0 + WINDOW_US + 3000);
                commitRow(b, 10_800_000_000L, 12);
                Assert.assertEquals("the frontier must stay at the contiguous durable prefix (the baseline)",
                        baselineTxn, tracker.getLocalDurableSeqTxn());

                a.simulatePowerLossDropPending();
                b.simulatePowerLossDropPending();
                crashFf.crash(engine.getConfiguration().getDbRoot());
            } finally {
                a.close();
                b.close();
            }
            releaseAll();
            recoverAndApply(tt);

            // The unacked tail may be gone or surface loudly; a surviving row is never wrong.
            assertNoSilentCorruption("t", V, Arrays.asList("1", "10", "11", "12"));
            List<String> rows;
            try {
                rows = readColumn("t", V);
            } catch (Throwable th) {
                rows = List.of();
            }
            Assert.assertFalse("the acked baseline row must survive the crash", rows.isEmpty());
            Assert.assertEquals("1", rows.getFirst());
        });
    }

    /**
     * A commit sequenced but not flushed may be lost on a power loss (RPO &le; W); the durable baseline reads back
     * clean, the table is not suspended, and the frontier never covered the lost commit.
     */
    @Test
    public void testUnflushedTxnMayBeLostButNoCorruption() throws Exception {
        setGroupCommit(WINDOW_US, 0);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        runWithCrashFacade(() -> {
            final SeqTxnTracker tracker = createTable("t");
            final TableToken tt = engine.verifyTableName("t");
            execute("insert into t values (0, 1)");
            drainWalQueue();
            final long frontierAfterFirst = engine.getTableSequencerAPI().lastTxn(tt);
            Assert.assertEquals("the release flush makes the first commit durable", 1, tracker.getLocalDurableSeqTxn());
            markDurableBaseline();

            final long unflushedSeqTxn;
            final WalWriter w = engine.getWalWriter(tt);
            try {
                setCurrentMicros(T0 + 1000);
                commitRow(w, 3_600_000_000L, 2);
                unflushedSeqTxn = tracker.getSeqTxn();
                Assert.assertEquals(frontierAfterFirst + 1, unflushedSeqTxn);
                Assert.assertTrue(tracker.getLocalDurableSeqTxn() < unflushedSeqTxn);
                // Power loss: drop the pending batch without a flush, then roll every file back to its durable content.
                w.simulatePowerLossDropPending();
                crashFf.crash(engine.getConfiguration().getDbRoot());
            } finally {
                w.close();
            }
            releaseAll();

            Assert.assertEquals("the unflushed commit is lost: the durable log ends at the first commit",
                    frontierAfterFirst, engine.getTableSequencerAPI().lastTxn(tt));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals(List.of("1"), readColumn("t", V));
        });
    }

    /**
     * W=0: a data commit and a custom event each sync their event file before the sequencer record, and the
     * frontier covers them before the call returns.
     */
    @Test
    public void testWindowZeroSyncsEventsBeforeSequencerAndPublishesAtOnce() throws Exception {
        setGroupCommit(0, 16);
        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            for (boolean customEvent : new boolean[]{false, true}) {
                final String table = customEvent ? "custom" : "data";
                final SeqTxnTracker tracker = createTable(table);
                execute("insert into " + table + " values ('2024-01-01T00:00:00.000000Z', 0)");
                try (WalWriter w = engine.getWalWriter(engine.verifyTableName(table))) {
                    ff.reset();
                    final long seqTxn;
                    if (customEvent) {
                        seqTxn = w.appendCustomEvent(CUSTOM_EVENT_TYPE, mem -> mem.putLong(42L));
                    } else {
                        commitRow(w, 60_000_000L, 1);
                        seqTxn = tracker.getSeqTxn();
                    }
                    final int eventIdx = ff.firstIndex(WalFdatasyncFacade::isEventFile);
                    final int seqIdx = ff.firstIndex(WalFdatasyncFacade::isSequencerFile);
                    Assert.assertTrue(table + ": the event file must be synced", eventIdx > -1);
                    Assert.assertTrue(table + ": W=0 must sync the sequencer record", seqIdx > -1);
                    Assert.assertTrue(table + ": event file before sequencer [eventIdx=" + eventIdx + ", seqIdx=" + seqIdx + ']', eventIdx < seqIdx);
                    Assert.assertEquals(table, seqTxn, tracker.getLocalDurableSeqTxn());
                    Assert.assertEquals(table, 0, tracker.getPendingWriterPinCount());
                }
            }
        });
    }

    private static void assertDropAndViewChangeFlushTheSequencerLog(int seqPartTxnCount) throws Exception {
        // No WAL writer batch follows a drop or a view change, so under a window they must flush the log themselves.
        setGroupCommit(WINDOW_US, seqPartTxnCount);
        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table base_t (ts timestamp, x int, y int) timestamp(ts) partition by day wal");
            execute("create table drop_t (ts timestamp, x int) timestamp(ts) partition by day wal");
            execute("insert into drop_t values (1, 1)");
            execute("create view v as (select ts, x from base_t)");

            ff.reset();
            execute("alter view v as (select ts, y from base_t)");
            Assert.assertTrue("ALTER VIEW left its sequencer record unflushed", ff.sequencerFdatasyncs() > 0);

            ff.reset();
            execute("drop table drop_t");
            Assert.assertTrue("DROP TABLE left its sequencer record unflushed", ff.sequencerFdatasyncs() > 0);
        });
    }

    /**
     * In a mode that promises durability, every record is covered by the frontier once its statement returns,
     * whatever kind of record it is, and the registry reports that frontier. NOSYNC does not track it, also after
     * the apply, whose epoch is the frontier's other route.
     */
    private static void assertDurableAppendsMoveTheDurableFrontier(String commitMode, long windowUs) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, windowUs);
        final boolean tracked = !"nosync".equals(commitMode);
        assertMemoryLeak(() -> {
            final TableSequencerAPI sequencerAPI = engine.getTableSequencerAPI();
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by day wal");
            final TableToken table = engine.verifyTableName("x");
            for (String sql : new String[]{
                    "insert into x values (1, 1, 1)",
                    "alter table x add column z int",
                    "update x set y = 2",
                    "truncate table x",
                    "create view v as (select ts, x from x)",
                    "alter view v as (select ts, y from x)"
            }) {
                execute(sql);
                final TableToken token = engine.verifyTableName(sql.contains(" view ") ? "v" : "x");
                final long frontier = sequencerAPI.getTxnTracker(token).getLocalDurableSeqTxn();
                final long lastTxn = sequencerAPI.lastTxn(token);
                if (tracked) {
                    // Txn 0 is the creation, which is not a record of the log.
                    Assert.assertTrue(commitMode + ": " + sql + " left txn " + lastTxn + " past the durable frontier " + frontier,
                            lastTxn == 0 || frontier >= lastTxn);
                } else {
                    Assert.assertEquals(commitMode + ": " + sql, -1, frontier);
                }
                Assert.assertEquals(commitMode + ": the registry must report the tracker's frontier",
                        sequencerAPI.getTxnTracker(table).getLocalDurableSeqTxn(),
                        engine.getDurableAckRegistry().getLocalDurableSeqTxn(table.getDirName()));
            }
            drainWalQueue();
            if (!tracked) {
                Assert.assertEquals("the apply must not move the frontier",
                        -1, engine.getDurableAckRegistry().getLocalDurableSeqTxn(table.getDirName()));
            }
        });
    }

    private static void assertNoFlushRace(String what, Runnable ingest) throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> flusherError = new AtomicReference<>();
        final Thread flusherThread = new Thread(() -> {
            try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                while (!stop.get()) {
                    flusher.flushNow();
                }
            } catch (Throwable th) {
                flusherError.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "group-commit-flusher");
        flusherThread.start();
        try {
            ingest.run();
        } finally {
            stop.set(true);
            flusherThread.join();
        }
        if (flusherError.get() != null) {
            throw new AssertionError("background flusher raced " + what, flusherError.get());
        }
        Assert.assertEquals("the background flusher raced " + what + " (fdatasync of a closed fd)",
                0L, engine.getWalGroupCommitFlushQueue().getFailedFlushCount());
    }

    /**
     * A failed sequencer barrier poisons the engine only after the failing writer released the sequencer write
     * lock. A peer barrier in that window fdatasyncs the same file, and since the kernel reports a writeback error
     * once, it returns 0 over the dropped pages. Clearing the poison after the failure reopens that window
     * deterministically. At W=0 the barrier is inline, the failure distresses the sequencer, and the peer
     * reopens it on a fresh fd, so the fence lives on the tracker.
     */
    private static void assertPeerBarrierAfterFailedSequencerBarrierDoesNotAdvanceFrontier(long windowUs) throws Exception {
        setGroupCommit(windowUs, windowUs == 0 ? 0 : 16);
        final FailingSeqFdatasyncFacade ff = new FailingSeqFdatasyncFacade();
        try {
            assertMemoryLeak(ff, () -> {
                // Only the window needs the test clock; W=0 runs on the wall clock.
                if (windowUs > 0) {
                    setCurrentMicros(T0);
                }
                execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("x");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                try (WalWriter a = engine.getWalWriter(tt); WalWriter c = engine.getWalWriter(tt)) {
                    commitRow(a, 0, 1);
                    commitRow(c, 1, 2);
                    final long frontierBefore = tracker.getLocalDurableSeqTxn();
                    Assert.assertTrue("test setup: W=0 publishes every commit, W>0 none yet",
                            windowUs == 0 ? frontierBefore == tracker.getSeqTxn() : frontierBefore < tracker.getSeqTxn() - 1);
                    if (windowUs > 0) {
                        setCurrentMicros(T0 + windowUs + 1);
                    }

                    ff.failNextSequencerFdatasync();
                    assertDataSyncFailure(() -> commitRow(c, 2, 3));
                    Assert.assertTrue("test setup: C's sequencer fdatasync must have failed", ff.injected);
                    Assert.assertTrue(tracker.isDurableFrontierFenced());

                    engine.resetDurabilityFailure();
                    final int seqSyncsBefore = ff.sequencerFdatasyncs();
                    assertDataSyncFailure(() -> commitRow(a, 3, 4));
                    Assert.assertTrue("test setup: A's own sequencer fdatasync must have returned 0",
                            ff.sequencerFdatasyncs() > seqSyncsBefore);
                    Assert.assertEquals("a barrier after the failed one must not move the frontier",
                            frontierBefore, tracker.getLocalDurableSeqTxn());
                    Assert.assertEquals("the durable-ack registry must not report the unproven frontier",
                            -1L, engine.getDurableAckRegistry().getLocalDurableSeqTxn(tt.getDirName()));
                    Assert.assertTrue("the rejected peer must poison the engine itself", engine.isDurabilityFailed());
                }
            });
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    private static SeqTxnTracker createTable(String name) throws Exception {
        setCurrentMicros(T0);
        execute("create table " + name + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        return engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName(name));
    }

    private static WalFdatasyncFacade ffOf() {
        return (WalFdatasyncFacade) ff;
    }

    private static void recoverAndApply(TableToken tt) {
        new RecoveryCoordinator(engine).recover();
        engine.notifyWalTxnRepublisher(tt);
        drainWalQueue();
    }

    private static void releaseAll() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactiveTableSequencers();
    }

    // Sets each property once: the test harness can lose a configuration change when a property is set again.
    private static void setGroupCommit(long windowUs, int seqPartTxnCount) {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, seqPartTxnCount);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(windowUs));
    }

    @FunctionalInterface
    private interface Trigger {
        /**
         * Fires on the held writer; returns false when the writer's release is the trigger.
         */
        boolean fire(WalWriter w, ExposedFlusher flusher, SeqTxnTracker tracker) throws Exception;
    }

    private record TriggerRow(String name, int commits, Trigger trigger) {
    }
}
