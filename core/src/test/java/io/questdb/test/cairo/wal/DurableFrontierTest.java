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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.std.Misc;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.crash.AbstractCrashConsistencyTest;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.ExposedFlusher;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.FailingSeqFdatasyncFacade;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.FailingWalFdatasyncFacade;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.WalFdatasyncFacade;
import io.questdb.test.std.SyncAttributingFilesFacade;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
     * A background flush is not work that should keep the worker spinning: the pass that flushes an idle batch
     * must still return {@code false}, or the worker resets its idle counter and never backs off to its sleep.
     */
    @Test
    public void testBackgroundFlushDoesNotReportBusy() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x")); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                setCurrentMicros(T0 + 1000);
                commitRow(w, 0, 1);
                final long pending = tracker.getSeqTxn();
                Assert.assertTrue("test setup: the batch must be pending", tracker.getLocalDurableSeqTxn() < pending);
                setCurrentMicros(T0 + 1000 + WINDOW_US);
                Assert.assertFalse("a background flush must not report busy work to the worker loop", flusher.flushNow());
                Assert.assertEquals("the pass must flush the idle batch", pending, tracker.getLocalDurableSeqTxn());
            }
        });
    }

    /**
     * The background flusher reads its clock before it sweeps, so a batch can start between the two. The sweep
     * must judge that batch's age by the writer's own clock read, not take the earlier timestamp for a backwards
     * clock step and flush the batch long before the group window.
     */
    @Test
    public void testBackgroundFlusherDoesNotFlushABatchStartedAfterItsClockRead() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x"))) {
                final AtomicBoolean armed = new AtomicBoolean();
                final AtomicBoolean committed = new AtomicBoolean();
                // The flusher's clock read, then a commit one microsecond later, before the flusher sweeps.
                final Clock clock = () -> {
                    final long t = configuration.getMicrosecondClock().getTicks();
                    if (armed.get() && committed.compareAndSet(false, true)) {
                        setCurrentMicros(t + 1);
                        commitRow(w, 0, 1);
                    }
                    return t;
                };
                try (StaleClockFlusher flusher = new StaleClockFlusher(clock)) {
                    armed.set(true);
                    flusher.flushNow();
                    Assert.assertTrue("the commit must land between the clock read and the sweep", committed.get());
                    final long pending = tracker.getSeqTxn();
                    Assert.assertTrue(
                            "a commit 1 us old must wait for the " + WINDOW_US
                                    + " us group window, the durable frontier is " + tracker.getLocalDurableSeqTxn(),
                            tracker.getLocalDurableSeqTxn() < pending
                    );
                    setCurrentMicros(T0 + WINDOW_US + 50_000);
                    flusher.flushNow();
                    Assert.assertEquals("the flusher must flush the batch once the window ends",
                            pending, tracker.getLocalDurableSeqTxn());
                }
            }
        });
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
     * Committers on four tables race two background flushers (one of them also forcing {@code sweep(0)}) on a clock
     * that every commit moves, so registrations land before, during and after the flushers' walks. Once commits
     * stop, one pass after the window must flush every writer: a batch start lost by a walk would leave its
     * writer pending.
     */
    @Test
    public void testConcurrentCommitsAndPassesFlushEveryIdleTail() throws Exception {
        final long windowUs = 1_000;
        final int tables = 4;
        final int commitsPerTable = 100;
        setGroupCommit(windowUs, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker[] trackers = new SeqTxnTracker[tables];
            final WalWriter[] writers = new WalWriter[tables];
            final long failedFlushesBefore = engine.getWalGroupCommitFlushQueue().getFailedFlushCount();
            final AtomicLong clock = new AtomicLong(T0);
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final List<Thread> committers = new ArrayList<>();
            final List<Thread> flushers = new ArrayList<>();
            try {
                for (int t = 0; t < tables; t++) {
                    trackers[t] = createTable("t" + t);
                    writers[t] = engine.getWalWriter(engine.verifyTableName("t" + t));
                }
                setCurrentMicros(clock.get());
                for (int f = 0; f < 2; f++) {
                    final boolean forces = f == 1;
                    final Thread flusherThread = new Thread(() -> {
                        try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                            for (int i = 0; !stop.get(); i++) {
                                flusher.flushNow();
                                if (forces && i % 16 == 0) {
                                    engine.getWalGroupCommitFlushQueue().sweep(0);
                                }
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }, "group-commit-flusher-" + f);
                    flushers.add(flusherThread);
                    flusherThread.start();
                }
                for (int t = 0; t < tables; t++) {
                    final WalWriter w = writers[t];
                    final Thread committer = new Thread(() -> {
                        try {
                            for (int i = 0; i < commitsPerTable; i++) {
                                setCurrentMicros(clock.addAndGet(windowUs / 8));
                                commitRow(w, i * 1_000_000L, i);
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }, "group-commit-committer-" + t);
                    committers.add(committer);
                    committer.start();
                }
                for (int i = 0, n = committers.size(); i < n; i++) {
                    committers.get(i).join();
                }
                stop.set(true);
                for (int i = 0, n = flushers.size(); i < n; i++) {
                    flushers.get(i).join();
                }
                if (error.get() != null) {
                    throw new AssertionError("a committer or flusher failed", error.get());
                }
                // Every batch started at or before the highest clock value set; one pass a window later is due.
                setCurrentMicros(clock.get() + windowUs);
                try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                    flusher.flushNow();
                }
                for (int t = 0; t < tables; t++) {
                    Assert.assertEquals("the pass after the window must flush table t" + t,
                            trackers[t].getSeqTxn(), trackers[t].getLocalDurableSeqTxn());
                }
                Assert.assertEquals("no background flush may fail",
                        failedFlushesBefore, engine.getWalGroupCommitFlushQueue().getFailedFlushCount());
            } finally {
                stop.set(true);
                for (int i = 0, n = committers.size(); i < n; i++) {
                    committers.get(i).join();
                }
                for (int i = 0, n = flushers.size(); i < n; i++) {
                    flushers.get(i).join();
                }
                for (int t = 0; t < tables; t++) {
                    Misc.free(writers[t]);
                }
            }
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
                    execute("insert into x values (60_000_000, 1)");
                    Assert.fail("a poisoned engine must not take a commit");
                } catch (CairoError expected) {
                    TestUtils.assertContains(expected.getMessage(), "engine is poisoned");
                }
            });
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    /**
     * A pass drops a writer whose flush fails without a data sync failure (here a distressed writer), and still
     * keeps a peer it found not yet due: the peer is flushed by the pass after its own window.
     */
    @Test
    public void testDistressedWriterInPassKeepsPeersDue() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            createTable("a");
            final SeqTxnTracker trackerB = createTable("b");
            final long failedFlushesBefore = engine.getWalGroupCommitFlushQueue().getFailedFlushCount();
            try (
                    WalWriter a = engine.getWalWriter(engine.verifyTableName("a"));
                    WalWriter b = engine.getWalWriter(engine.verifyTableName("b"));
                    ExposedFlusher flusher = new ExposedFlusher(engine)
            ) {
                setCurrentMicros(T0 + 1000);
                commitRow(a, 0, 1);
                setCurrentMicros(T0 + 1000 + WINDOW_US / 2);
                commitRow(b, 0, 1);
                final long pendingB = trackerB.getSeqTxn();
                markDistressed(a);
                setCurrentMicros(T0 + 1000 + WINDOW_US);
                flusher.flushNow();
                Assert.assertEquals("test setup: the distressed writer's flush must fail in the pass",
                        failedFlushesBefore + 1, engine.getWalGroupCommitFlushQueue().getFailedFlushCount());
                Assert.assertTrue("a batch younger than the window must wait", trackerB.getLocalDurableSeqTxn() < pendingB);
                setCurrentMicros(T0 + 1000 + WINDOW_US / 2 + WINDOW_US);
                flusher.flushNow();
                Assert.assertEquals("a peer of a writer whose flush failed must be flushed once due",
                        pendingB, trackerB.getLocalDurableSeqTxn());
            }
        });
    }

    @Test
    public void testDropAndViewChangeFlushFailure() throws Exception {
        // DROP TABLE and ALTER VIEW flush the sequencer log in the append (SYNC: msync, ADAPTIVE W=0: fdatasync) or
        // after it (ADAPTIVE W>0). A failed flush must stop the engine, as on the insert path. NOSYNC and ASYNC do not
        // flush the log in the append. Their DROP TABLE flushes it once, at the dropped table's clean sequencer close,
        // where a failed flush is logged and fences the table: the statement succeeds and the engine keeps running.
        // ALTER VIEW does not close its sequencer, so it does not flush.
        final String[][] modes = {
                {"sync", "0"},
                {"adaptive", "0"},
                {"adaptive", String.valueOf(WINDOW_US)},
                {"nosync", "0"},
                {"async", "0"}
        };
        final String[] statements = {"drop table drop_t%d", "alter view v%d as (select ts, y from base_t%d)"};
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        final FailingSeqLogFlushFacade ff = new FailingSeqLogFlushFacade();
        try {
            assertMemoryLeak(ff, () -> {
                final StringBuilder expected = new StringBuilder();
                final StringBuilder actual = new StringBuilder();
                int i = 0;
                for (String[] mode : modes) {
                    setCommitMode(mode);
                    final boolean flushes = !"nosync".equals(mode[0]) && !"async".equals(mode[0]);
                    for (String statement : statements) {
                        execute("create table base_t" + i + " (ts timestamp, x int, y int) timestamp(ts) partition by day wal");
                        execute("create table drop_t" + i + " (ts timestamp, x int) timestamp(ts) partition by day wal");
                        execute("insert into drop_t" + i + " values (1, 1)");
                        execute("create view v" + i + " as (select ts, x from base_t" + i + ")");
                        final String sql = String.format(statement, i, i);
                        final String row = mode[0] + " W=" + mode[1] + ", " + sql + ": ";
                        final String outcome = flushes ? "engine stopped"
                                : statement.startsWith("drop") ? "flush failed, statement succeeded, engine runs" : "no flush";
                        expected.append(row).append(outcome).append('\n');
                        actual.append(row).append(failNextLogFlush(ff, sql, "insert into base_t" + i + " values (1, 1, 1)")).append('\n');
                        engine.resetDurabilityFailure();
                        i++;
                    }
                }
                Assert.assertEquals(expected.toString(), actual.toString());
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
    public void testFailedCreateCleanupFlushFailureKeepsTheCreateError() throws Exception {
        // A WAL table's failed CREATE drops its sequencer entry, which flushes the log in these modes. When that flush
        // fails, the engine stops, and the error the caller gets must still carry the create's own failure.
        final String[][] modes = {{"sync", "0"}, {"adaptive", "0"}, {"adaptive", String.valueOf(WINDOW_US)}};
        final FailingSeqLogFlushFacade ff = new FailingSeqLogFlushFacade();
        try {
            assertMemoryLeak(ff, () -> {
                final StringBuilder expected = new StringBuilder();
                final StringBuilder actual = new StringBuilder();
                for (int i = 0; i < modes.length; i++) {
                    setCommitMode(modes[i]);
                    engine.setDdlListener(new DefaultDdlListener() {
                        @Override
                        public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                            ff.failNextFlush();
                            throw CairoException.critical(0).put("injected create failure");
                        }
                    });
                    final AtomicInteger fatalCalls = new AtomicInteger();
                    engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
                    String outcome = "create succeeded";
                    try {
                        execute("create table cr_t" + i + " (ts timestamp, x int) timestamp(ts) partition by day wal");
                    } catch (Throwable th) {
                        final Throwable[] suppressed = th.getSuppressed();
                        outcome = th.getClass().getSimpleName()
                                + ", engine stopped: " + (engine.isDurabilityFailed() && fatalCalls.get() == 1)
                                + ", create error kept: " + (suppressed.length == 1 && String.valueOf(suppressed[0].getMessage()).contains("injected create failure"));
                    }
                    engine.setDdlListener(DefaultDdlListener.INSTANCE);
                    final String row = modes[i][0] + " W=" + modes[i][1] + ": ";
                    expected.append(row).append("flush failed, CairoError, engine stopped: true, create error kept: true\n");
                    actual.append(row).append(ff.disarm() ? "flush failed, " : "no flush, ").append(outcome).append('\n');
                    engine.resetDurabilityFailure();
                }
                Assert.assertEquals(expected.toString(), actual.toString());
            });
        } finally {
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            engine.resetDurabilityFailure();
        }
    }

    /**
     * A pass whose walk stops at a data sync failure leaves the writers it did not reach due: once the failure is
     * cleared, the next pass flushes them.
     */
    @Test
    public void testFailedPassKeepsUnvisitedWritersDue() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        final FailingSeqFdatasyncFacade ff = new FailingSeqFdatasyncFacade();
        try {
            assertMemoryLeak(ff, () -> {
                final SeqTxnTracker trackerA = createTable("a");
                final SeqTxnTracker trackerB = createTable("b");
                engine.setDurabilityFailureHandler(failure -> {
                });
                try (
                        WalWriter a = engine.getWalWriter(engine.verifyTableName("a"));
                        WalWriter b = engine.getWalWriter(engine.verifyTableName("b"));
                        ExposedFlusher flusher = new ExposedFlusher(engine)
                ) {
                    setCurrentMicros(T0 + 1000);
                    commitRow(a, 0, 1);
                    commitRow(b, 0, 1);
                    final long pendingA = trackerA.getSeqTxn();
                    final long pendingB = trackerB.getSeqTxn();
                    // Runs the purge job's first broad sweep before the failure is armed.
                    flusher.flushNow();
                    setCurrentMicros(T0 + 1000 + WINDOW_US);
                    ff.failNextSequencerFdatasync();
                    assertDataSyncFailure(flusher::flushNow);
                    Assert.assertTrue("test setup: the pass's first flush must have failed", ff.injected);
                    Assert.assertEquals("test setup: the walk must stop at the failed writer", 0,
                            countFlushed(trackerA, pendingA, trackerB, pendingB));
                    engine.resetDurabilityFailure();
                    flusher.flushNow();
                    Assert.assertEquals("the writer the failed pass did not reach must be flushed by the next pass", 1,
                            countFlushed(trackerA, pendingA, trackerB, pendingB));
                }
            });
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    @Test
    public void testFencedSequencerCloseDoesNotFlushTheLogV1() throws Exception {
        // NOSYNC flushes the V1 log only at a clean sequencer close. Once that flush has failed, a later one can return
        // 0 over the pages the kernel dropped, and marking the log flushed would let the next open trust its tail. The
        // fence outlives the sequencer, so a reopened sequencer must neither flush the log at close nor move the hint.
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
        final FailingSeqFdatasyncFacade ff = new FailingSeqFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            setCommitMode(new String[]{"nosync", "0"});
            final SeqTxnTracker tracker = createTable("x");
            final TableToken tt = engine.verifyTableName("x");
            final TableSequencerAPI sequencers = engine.getTableSequencerAPI();
            execute("INSERT INTO x VALUES (1, 1)");
            ff.failNextSequencerFdatasync();
            Assert.assertTrue("test setup: the sequencer must close", sequencers.releaseInactive());
            Assert.assertTrue("test setup: the clean close must have flushed the log", ff.injected);
            Assert.assertTrue("a failed close flush must fence the table", tracker.isDurableFrontierFenced());

            // The insert reopens the sequencer and appends a record past the hint.
            execute("INSERT INTO x VALUES (2, 2)");
            Assert.assertSame(tracker, sequencers.getTxnTracker(tt));
            Assert.assertEquals(2, sequencers.lastTxn(tt));
            final long hint = readSequencerLogHint(tt);
            Assert.assertNotEquals("test setup: the header alone must not skip the flush", 3, hint);
            ff.reset();
            Assert.assertTrue("test setup: the reopened sequencer must close", sequencers.releaseInactive());
            Assert.assertEquals(
                    "a fenced log must not be flushed at close",
                    0,
                    ff.count(p -> p.endsWith(WalUtils.TXNLOG_FILE_NAME))
            );
            Assert.assertEquals("a fenced log must not be marked flushed at close", hint, readSequencerLogHint(tt));
        });
    }

    /**
     * {@code sweep(0)}, the flush a role switch forces, flushes a batch the gated pass leaves inside its window.
     */
    @Test
    public void testForcedSweepFlushesBeforeTheWindow() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x")); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                setCurrentMicros(T0 + 1000);
                commitRow(w, 0, 1);
                final long pending = tracker.getSeqTxn();
                setCurrentMicros(T0 + 2000);
                flusher.flushNow();
                Assert.assertTrue("a batch younger than the window must wait", tracker.getLocalDurableSeqTxn() < pending);
                Assert.assertTrue("a forced sweep must report its flush", engine.getWalGroupCommitFlushQueue().sweep(0));
                Assert.assertEquals("a forced sweep must flush the batch", pending, tracker.getLocalDurableSeqTxn());
            }
        });
    }

    /**
     * The window is applied when the pass runs, not when the batch started: a batch that started under a larger
     * window is flushed by the first pass after the window is lowered past its age.
     */
    @Test
    public void testPassAppliesTheCurrentWindow() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x")); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                setCurrentMicros(T0 + 1000);
                commitRow(w, 0, 1);
                final long pending = tracker.getSeqTxn();
                setCurrentMicros(T0 + 1000 + WINDOW_US / 4);
                flusher.flushNow();
                Assert.assertTrue("a batch younger than the window must wait", tracker.getLocalDurableSeqTxn() < pending);
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US / 10));
                Assert.assertEquals(WINDOW_US / 10, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
                flusher.flushNow();
                Assert.assertEquals("a lowered window must apply to a batch that started under the old one",
                        pending, tracker.getLocalDurableSeqTxn());
            }
        });
    }

    /**
     * A pass before any batch reaches its window must not enter a pending writer's monitor: the test holds the
     * monitor, and the pass must still return. Once the window ends, the next pass flushes the batch.
     */
    @Test
    public void testPassBeforeTheWindowEndsDoesNotEnterWriterMonitor() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("x")); ExposedFlusher flusher = new ExposedFlusher(engine)) {
                setCurrentMicros(T0 + 1000);
                commitRow(w, 0, 1);
                final long pending = tracker.getSeqTxn();
                setCurrentMicros(T0 + 1000 + WINDOW_US / 2);
                // Runs the purge job's first broad sweep, and settles what earlier tests left in the queue.
                flusher.flushNow();
                final CountDownLatch passDone = new CountDownLatch(1);
                final AtomicReference<Throwable> passError = new AtomicReference<>();
                final Thread pass = new Thread(() -> {
                    try {
                        flusher.flushNow();
                    } catch (Throwable th) {
                        passError.set(th);
                    } finally {
                        Path.clearThreadLocals();
                        passDone.countDown();
                    }
                }, "group-commit-pass");
                final boolean returnedWhileHeld;
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (w) {
                    pass.start();
                    returnedWhileHeld = passDone.await(10, TimeUnit.SECONDS);
                }
                pass.join();
                if (passError.get() != null) {
                    throw new AssertionError("the pass failed", passError.get());
                }
                Assert.assertTrue("a pass before any window ends must not enter a pending writer's monitor", returnedWhileHeld);
                Assert.assertTrue("a batch younger than the window must wait", tracker.getLocalDurableSeqTxn() < pending);
                setCurrentMicros(T0 + 1000 + WINDOW_US);
                flusher.flushNow();
                Assert.assertEquals("the pass after the window must flush the batch", pending, tracker.getLocalDurableSeqTxn());
            }
        });
    }

    /**
     * Two writers of one table are two entries in the group-commit registry, so a pass a window after both batches
     * started flushes both, and the frontier passes both commits.
     */
    @Test
    public void testPassFlushesEveryWriterOfOneTable() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = createTable("x");
            final TableToken tt = engine.verifyTableName("x");
            try (
                    WalWriter a = engine.getWalWriter(tt);
                    WalWriter b = engine.getWalWriter(tt);
                    ExposedFlusher flusher = new ExposedFlusher(engine)
            ) {
                Assert.assertNotEquals("test setup: the writers must have their own WALs", a.getWalId(), b.getWalId());
                setCurrentMicros(T0 + 1000);
                commitRow(a, 0, 1);
                commitRow(b, 1, 2);
                final long lastSeqTxn = tracker.getSeqTxn();
                Assert.assertTrue("test setup: both batches must wait for a flush", tracker.getLocalDurableSeqTxn() < lastSeqTxn - 1);
                setCurrentMicros(T0 + 1000 + WINDOW_US);
                flusher.flushNow();
                Assert.assertEquals("the pass must flush the batches of both writers", lastSeqTxn, tracker.getLocalDurableSeqTxn());
            }
        });
    }

    /**
     * The clock steps back below a batch that started after the last pass, but stays above an older batch and
     * above that pass's reading. The newer batch's age is negative, so the pass must flush it at once, as it flushes
     * any batch after a backwards step; the older batch, still inside its window, waits.
     */
    @Test
    public void testPassFlushesABatchStartedAboveABackwardClockStep() throws Exception {
        setGroupCommit(WINDOW_US, 16);
        assertMemoryLeak(() -> {
            final SeqTxnTracker trackerA = createTable("a");
            final SeqTxnTracker trackerB = createTable("b");
            try (
                    WalWriter a = engine.getWalWriter(engine.verifyTableName("a"));
                    WalWriter b = engine.getWalWriter(engine.verifyTableName("b"));
                    ExposedFlusher flusher = new ExposedFlusher(engine)
            ) {
                setCurrentMicros(T0 + 1_000);
                commitRow(a, 0, 1);
                setCurrentMicros(T0 + 10_000);
                flusher.flushNow();
                setCurrentMicros(T0 + 20_000);
                commitRow(b, 0, 1);
                final long pendingA = trackerA.getSeqTxn();
                final long pendingB = trackerB.getSeqTxn();
                setCurrentMicros(T0 + 15_000);
                flusher.flushNow();
                Assert.assertEquals("a batch that started after the clock's new reading must be flushed at once",
                        pendingB, trackerB.getLocalDurableSeqTxn());
                Assert.assertTrue("a batch younger than the window must wait", trackerA.getLocalDurableSeqTxn() < pendingA);
            }
        });
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

                powerLossDroppingPending(a, b);
            } finally {
                a.close();
                b.close();
            }
            // Every handle from before the power loss is released before the rollback: the mark keeps the
            // close-time flushes out of the durable state, and the rollback refuses a mutation after it.
            releaseAll();
            crashFf.crash(engine.getConfiguration().getDbRoot());
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
                // Power loss: drop the pending batch without a flush. The handles are released before the rollback
                // puts every file back to its durable content.
                powerLossDroppingPending(w);
            } finally {
                w.close();
            }
            releaseAll();
            crashFf.crash(engine.getConfiguration().getDbRoot());

            Assert.assertEquals("the unflushed commit is lost: the durable log ends at the first commit",
                    frontierAfterFirst, engine.getTableSequencerAPI().lastTxn(tt));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals(List.of("1"), readColumn("t", V));
        });
    }

    /**
     * W=0: every data commit and every custom event on a reused writer syncs its event file before the sequencer
     * record, and the frontier covers it before the call returns.
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
                    for (int i = 1; i <= 3; i++) {
                        final String commit = table + " commit " + i;
                        ff.reset();
                        final long seqTxn;
                        if (customEvent) {
                            seqTxn = w.appendCustomEvent(CUSTOM_EVENT_TYPE, mem -> mem.putLong(42L));
                        } else {
                            commitRow(w, i * 60_000_000L, i);
                            seqTxn = tracker.getSeqTxn();
                        }
                        Assert.assertTrue(commit + ": the event file must be synced", ff.eventFdatasyncs() > 0);
                        final int eventIdx = ff.firstIndex(WalFdatasyncFacade::isEventFile);
                        final int seqIdx = ff.firstIndex(WalFdatasyncFacade::isSequencerFile);
                        Assert.assertTrue(commit + ": W=0 must sync the sequencer record", seqIdx > -1);
                        Assert.assertTrue(commit + ": event file before sequencer [eventIdx=" + eventIdx + ", seqIdx=" + seqIdx + ']', eventIdx < seqIdx);
                        Assert.assertEquals(commit, seqTxn, tracker.getLocalDurableSeqTxn());
                        Assert.assertEquals(commit, 0, tracker.getPendingWriterPinCount());
                    }
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
        // The queue is the engine's, which every test of the class shares.
        final long failedFlushesBefore = engine.getWalGroupCommitFlushQueue().getFailedFlushCount();
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
                failedFlushesBefore, engine.getWalGroupCommitFlushQueue().getFailedFlushCount());
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

    private static int countFlushed(SeqTxnTracker trackerA, long pendingA, SeqTxnTracker trackerB, long pendingB) {
        return (trackerA.getLocalDurableSeqTxn() >= pendingA ? 1 : 0) + (trackerB.getLocalDurableSeqTxn() >= pendingB ? 1 : 0);
    }

    private static SeqTxnTracker createTable(String name) throws Exception {
        setCurrentMicros(T0);
        execute("create table " + name + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        return engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName(name));
    }

    private static String failNextLogFlush(FailingSeqLogFlushFacade ff, String sql, String nextWrite) throws Exception {
        final AtomicInteger fatalCalls = new AtomicInteger();
        engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
        ff.failNextFlush();
        Throwable thrown = null;
        try {
            execute(sql);
        } catch (Throwable th) {
            thrown = th;
        }
        if (!ff.disarm()) {
            return thrown == null ? "no flush" : "no flush, failed: " + thrown;
        }
        if (thrown == null) {
            if (engine.isDurabilityFailed() || fatalCalls.get() != 0) {
                return "flush failed, statement succeeded, engine stopped";
            }
            execute(nextWrite);
            return "flush failed, statement succeeded, engine runs";
        }
        if (!engine.isDurabilityFailed() || fatalCalls.get() != 1) {
            return "flush failed, engine runs: " + thrown;
        }
        try {
            execute(nextWrite);
            return "poisoned engine took a write";
        } catch (CairoError e) {
            return e.getMessage().contains("engine is poisoned") ? "engine stopped" : "next write failed: " + e;
        }
    }

    private static WalFdatasyncFacade ffOf() {
        return (WalFdatasyncFacade) ff;
    }

    private static long readSequencerLogHint(TableToken tt) throws IOException {
        final String dbRoot = engine.getConfiguration().getDbRoot().toString();
        try (FileChannel channel = FileChannel.open(
                Paths.get(dbRoot, tt.getDirName(), WalUtils.SEQ_DIR, WalUtils.TXNLOG_FILE_NAME),
                StandardOpenOption.READ
        )) {
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            channel.read(buf, TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64);
            return buf.getLong(0);
        }
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

    // The assertions prove that the row runs the mode it names.
    private static void setCommitMode(String[] mode) {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode[0]);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, mode[1]);
        Assert.assertEquals(mode[0], CommitMode.toString(engine.getConfiguration().getCommitMode()));
        Assert.assertEquals(Long.parseLong(mode[1]), engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
    }

    private static void setGroupCommit(long windowUs, int seqPartTxnCount) {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, seqPartTxnCount);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(windowUs));
    }

    /**
     * Fails the next flush of a sequencer file once, after issuing it: an {@code fdatasync}, or a synchronous
     * {@code msync} (the SYNC append's flush).
     */
    private static class FailingSeqLogFlushFacade extends SyncAttributingFilesFacade {
        private boolean armed;
        private boolean injected;

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            failIfArmed(pathOfFd(fd), "fdatasync");
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            if (!async) {
                failIfArmed(pathOfAddress(addr), "msync");
            }
        }

        // Returns whether the armed failure was injected, and disarms.
        boolean disarm() {
            final boolean wasInjected = injected;
            armed = false;
            injected = false;
            return wasInjected;
        }

        void failNextFlush() {
            armed = true;
        }

        private void failIfArmed(String path, String operation) {
            if (armed && WalFdatasyncFacade.isSequencerFile(path)) {
                armed = false;
                injected = true;
                throw CairoException.dataSyncFailure(this, 5, operation, "injected sequencer log flush failure");
            }
        }
    }

    private static class StaleClockFlusher extends WalPurgeJob {
        StaleClockFlusher(Clock clock) {
            super(engine, configuration.getFilesFacade(), clock);
        }

        void flushNow() {
            runSerially();
        }
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
