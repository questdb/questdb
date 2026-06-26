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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deferred 2 — adaptive GROUP-COMMIT (the RPO knob {@code cairo.adaptive.commit.group.window.us}).
 *
 * <p>W=0 (default) is today's synchronous fsync-before-return (zero loss). When {@code W > 0}, the WAL
 * fdatasync (the device flush) is BATCHED across an adaptive table's commits within window {@code W}:
 * {@code commit0} returns after the txn is sequenced (commit-ack = sequenced, msync'd to page cache,
 * NOT yet device-durable), and the fdatasync is performed by a batched flush. {@code localDurableSeqTxn}
 * advances only when the batch fdatasync completes, so the durable-ack only fires after the batch is on
 * disk.
 *
 * <p>These tests pin the MECHANISM:
 * <ul>
 *   <li>(a) the config default is 0;</li>
 *   <li>(b) W=0 issues a WAL fdatasync per commit and advances {@code localDurableSeqTxn} synchronously
 *       (byte-identical to today);</li>
 *   <li>(c) W&gt;0 BATCHES the WAL fdatasync across rapid commits (fdatasync count &lt;&lt; N) and
 *       {@code localDurableSeqTxn} LAGS the committed seqTxn until a flush fires, then catches up;</li>
 *   <li>(d) the next commit after the window elapses flushes the backlog (commit-driven trigger);</li>
 *   <li>(e) the background flusher makes an IDLE writer's last pending commit durable within ≤ W even when
 *       commits STOP (the hard requirement);</li>
 *   <li>(f) writer release/close flushes any pending (clean handoff is durable).</li>
 * </ul>
 * The crash-durability oracle (durable-ack safety + RPO bound) lives in
 * {@code AdaptiveGroupCommitCrashTest}.
 */
public class AdaptiveGroupCommitTest extends AbstractCairoTest {

    private static final long WINDOW_US = 1_000_000L; // 1s window, driven by the test microsecond clock

    @Test
    public void testGroupWindowDefaultsToZero() throws Exception {
        assertMemoryLeak(() -> Assert.assertEquals(
                "cairo.adaptive.commit.group.window.us default must be 0 (synchronous, zero-loss)",
                0L, engine.getConfiguration().getAdaptiveCommitGroupWindowUs()
        ));
    }

    /**
     * (b) W=0: ADAPTIVE issues a WAL fdatasync on EVERY commit and advances {@code localDurableSeqTxn}
     * synchronously in commit0 (today's behaviour — must be unchanged).
     */
    @Test
    public void testWindowZeroFsyncsEveryCommitAndAdvancesDurableSynchronously() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");

        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            // warmup to pre-allocate file pages so only per-commit durability fdatasyncs are measured.
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 0)");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            ff.reset();
            final int n = 5;
            for (int i = 1; i <= n; i++) {
                execute("insert into x values ('2024-01-01T00:0" + i + ":00.000000Z', " + i + ")");
                // W=0: durable frontier advances synchronously with the sequenced txn.
                Assert.assertEquals(
                        "W=0 must advance localDurableSeqTxn synchronously to the committed seqTxn",
                        tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn()
                );
            }
            // One WAL device flush per commit (the events fd is fdatasync'd every commit under W=0).
            Assert.assertTrue(
                    "W=0 must fdatasync the WAL events file on every commit (got " + ff.eventFdatasyncs()
                            + " for " + n + " commits)",
                    ff.eventFdatasyncs() >= n
            );
        });
    }

    /**
     * (c) W&gt;0: rapid commits within the window (on a HELD writer — the real high-rate ingestion shape,
     * where the WAL writer is kept across many commits) BATCH the WAL fdatasync — far fewer WAL fdatasyncs
     * than commits — and {@code localDurableSeqTxn} LAGS the committed seqTxn until a flush fires.
     *
     * <p>NB: each {@code execute("insert")} round-trips the WAL-writer pool and RELEASES the writer, which
     * (correctly) flushes its pending tail on the clean handoff — so batching is only observable while the
     * writer is held. This test holds one {@link WalWriter} and commits on it directly.
     */
    @Test
    public void testWindowPositiveBatchesFsyncAndLagsDurable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));

        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            final int n = 6;
            try (WalWriter w = engine.getWalWriter(tt)) {
                // warmup commit on the HELD writer: allocates all file pages so only per-commit durability
                // fdatasyncs are measured below. The writer is NOT released, so nothing flushes the warmup.
                commitRow(w, 0L, 0L);
                ff.reset();

                // All commits happen WITHIN one window (advance the clock by far less than W between them):
                // the first seeds pendingSince; the rest are within W so NONE triggers a commit-driven flush.
                for (int i = 1; i <= n; i++) {
                    setCurrentMicros(1_000_000L + i * 1000L); // +1ms each, << 1s window
                    commitRow(w, (i) * 60_000_000L, i);
                }

                // MECHANISM 1: the WAL device flush is batched — far fewer WAL fdatasyncs than commits
                // (deferred path issues ZERO per-commit WAL fdatasyncs while the writer is held).
                Assert.assertTrue(
                        "W>0 must BATCH the WAL fdatasync: expected far fewer than " + n + " WAL fdatasyncs, got "
                                + ff.walFdatasyncs(),
                        ff.walFdatasyncs() < n
                );
                // MECHANISM 2: the durable frontier LAGS the sequenced frontier (the un-flushed tail is NOT
                // yet device-durable, so the durable-ack must not have advanced over it).
                Assert.assertTrue(
                        "W>0: localDurableSeqTxn (" + tracker.getLocalDurableSeqTxn() + ") must LAG the committed "
                                + "seqTxn (" + tracker.getSeqTxn() + ") while the batch is unflushed",
                        tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn()
                );
            }
        });
    }

    /**
     * (d) Commit-driven trigger: once the window has elapsed, the NEXT commit flushes the backlog and
     * advances {@code localDurableSeqTxn} up to (at least) the prior pending seqTxn.
     */
    @Test
    public void testNextCommitAfterWindowFlushesBacklog() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));

        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter w = engine.getWalWriter(tt)) {
                // Two rapid commits within the window (writer held): the tail is left pending (unflushed).
                setCurrentMicros(1_000_000L);
                commitRow(w, 0L, 0);
                setCurrentMicros(1_001_000L);
                commitRow(w, 60_000_000L, 1);
                final long pendingSeqTxn = tracker.getSeqTxn();
                Assert.assertTrue("tail must be pending (unflushed)", tracker.getLocalDurableSeqTxn() < pendingSeqTxn);

                // Advance the clock PAST the window, then commit again: this commit must flush the backlog.
                setCurrentMicros(1_000_000L + WINDOW_US + 1000L);
                commitRow(w, 120_000_000L, 2);

                Assert.assertTrue(
                        "the post-window commit must flush the backlog: localDurableSeqTxn ("
                                + tracker.getLocalDurableSeqTxn() + ") must have advanced to >= the prior pending "
                                + "seqTxn (" + pendingSeqTxn + ")",
                        tracker.getLocalDurableSeqTxn() >= pendingSeqTxn
                );
            }
        });
    }

    /**
     * (e) THE HARD REQUIREMENT: an IDLE writer whose last commit is still pending must become durable
     * within ≤ W even though commits STOPPED. The background flusher (a WAL job) forces the writer's
     * deferred fdatasync once {@code now - pendingSince >= W}.
     */
    @Test
    public void testBackgroundFlusherMakesIdleWriterDurableWithinWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));

        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            // Hold the writer (idle ingestion: a writer kept open between bursts) so the ONLY thing that can
            // make its pending tail durable is the background flusher — NOT a release flush.
            try (WalWriter w = engine.getWalWriter(tt)) {
                // One commit, then commits STOP. The tail is pending.
                setCurrentMicros(1_000_000L);
                commitRow(w, 0L, 0);
                final long pendingSeqTxn = tracker.getSeqTxn();
                Assert.assertTrue("tail must be pending (unflushed)", tracker.getLocalDurableSeqTxn() < pendingSeqTxn);

                try (ExposedWalPurgeJob flusher = new ExposedWalPurgeJob(engine)) {
                    // Before the window elapses the flusher must NOT advance durability (RPO still in budget).
                    setCurrentMicros(1_000_000L + WINDOW_US / 2);
                    flusher.flushNow();
                    Assert.assertTrue(
                            "before W elapses the flusher must not have advanced the durable frontier",
                            tracker.getLocalDurableSeqTxn() < pendingSeqTxn
                    );

                    // Once the window has elapsed, the flusher forces the deferred fdatasync -> durable advances.
                    setCurrentMicros(1_000_000L + WINDOW_US + 1000L);
                    flusher.flushNow();
                    Assert.assertTrue(
                            "after W elapses the background flusher must make the idle writer's pending commit "
                                    + "durable: localDurableSeqTxn (" + tracker.getLocalDurableSeqTxn()
                                    + ") must reach the pending seqTxn (" + pendingSeqTxn + ")",
                            tracker.getLocalDurableSeqTxn() >= pendingSeqTxn
                    );
                }
            }
        });
    }

    /**
     * (f) Writer release/close must flush any pending so a clean handoff is durable (the next acquirer,
     * and any durable-ack consumer, sees the frontier on disk).
     */
    @Test
    public void testWriterReleaseFlushesPending() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));

        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            final long pendingSeqTxn;
            WalWriter held = engine.getWalWriter(tt);
            try {
                setCurrentMicros(1_000_000L);
                commitRow(held, 0L, 0);
                pendingSeqTxn = tracker.getSeqTxn();
                Assert.assertTrue("tail must be pending (unflushed) while held",
                        tracker.getLocalDurableSeqTxn() < pendingSeqTxn);
            } finally {
                // Clean handoff: returning the writer to the pool must flush the pending tail.
                held.close();
            }

            Assert.assertTrue(
                    "writer release must flush pending: localDurableSeqTxn (" + tracker.getLocalDurableSeqTxn()
                            + ") must reach the pending seqTxn (" + pendingSeqTxn + ")",
                    tracker.getLocalDurableSeqTxn() >= pendingSeqTxn
            );
        });
    }

    /**
     * (g) ORDERING across a STRUCTURAL change: a structural ALTER device-flushes the sequencer txn log
     * ({@code endMetadataChangeEntry -> fullSync}, an MS_SYNC). Under W&gt;0 the prior DATA commits' column
     * data is only {@code msync(MS_ASYNC)}'d (pending) — so the structural fullSync would make the sequencer
     * durable AHEAD of those columns (a torn data→events→seq order on crash) unless the pending backlog is
     * flushed FIRST. This asserts that applying a structural change flushes the pending group commit, so
     * {@code localDurableSeqTxn} covers every prior data commit before the structural txn is sequenced.
     */
    @Test
    public void testStructuralChangeFlushesPendingFirst() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));

        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter w = engine.getWalWriter(tt)) {
                // Two data commits within the window -> pending (un-flushed).
                setCurrentMicros(1_000_000L);
                commitRow(w, 0L, 0);
                setCurrentMicros(1_001_000L);
                commitRow(w, 60_000_000L, 1);
                final long dataSeqTxn = tracker.getSeqTxn();
                Assert.assertTrue("data commits must be pending before the structural change",
                        tracker.getLocalDurableSeqTxn() < dataSeqTxn);

                // Apply a STRUCTURAL change on the SAME writer (ADD COLUMN). Its fullSync device-flushes the
                // sequencer; the pending data columns must be flushed BEFORE that, so localDurable must reach
                // at least the last data commit by the time the structural change has sequenced.
                setCurrentMicros(1_002_000L);
                addColumn(w, "w", io.questdb.cairo.ColumnType.LONG);

                Assert.assertTrue(
                        "a structural change must flush the pending group commit first: localDurableSeqTxn ("
                                + tracker.getLocalDurableSeqTxn() + ") must cover every prior data commit ("
                                + dataSeqTxn + ") so the sequencer fullSync never outruns the column data",
                        tracker.getLocalDurableSeqTxn() >= dataSeqTxn
                );
            }
        });
    }

    /**
     * (h) CONCURRENCY: the background flusher must not race a SEGMENT ROLL. The flusher's batched fdatasync
     * iterates the writer's column/events fds; a concurrent {@code newRow() -> openNewSegment()} closes and
     * reopens those fds. {@code openNewSegment()} flushes+deregisters pending under the writer monitor first,
     * so a flusher can never fdatasync a closed/reused fd. This stress test ingests rapidly with a TINY
     * segment-rollover (frequent rolls) under W&gt;0 while a second thread spins the flusher; a use-after-close
     * would surface as an exception (EBADF / SIGBUS) or a torn read. It must stay clean and lose no rows.
     */
    @Test
    public void testBackgroundFlusherDoesNotRaceSegmentRoll() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Tiny window so the flusher actually fires (real wall clock here, not the test clock), maximizing
        // the overlap with segment rolls.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "1");
        // Roll the segment every few rows so openNewSegment() (the fd close/reopen) runs constantly.
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, "3");

        final int rows = 600;
        assertMemoryLeak(() -> {
            // NB: real wall clock (no setCurrentMicros) so the flusher's age gate fires against W=1us.
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");

            final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            final java.util.concurrent.atomic.AtomicReference<Throwable> flusherErr = new java.util.concurrent.atomic.AtomicReference<>();
            final ExposedWalPurgeJob flusher = new ExposedWalPurgeJob(engine);
            final Thread flusherThread = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        flusher.flushNow();
                    }
                } catch (Throwable th) {
                    flusherErr.set(th);
                } finally {
                    // Free this spawned thread's thread-local Paths so the leak checker stays clean (managed
                    // pool threads do this automatically; a hand-rolled test thread must do it itself).
                    io.questdb.std.str.Path.clearThreadLocals();
                }
            }, "group-commit-flusher");
            flusherThread.start();

            try (WalWriter w = engine.getWalWriter(tt)) {
                for (int i = 0; i < rows; i++) {
                    commitRow(w, i * 1000L, i);
                }
            } finally {
                stop.set(true);
                flusherThread.join();
                flusher.close();
            }

            if (flusherErr.get() != null) {
                throw new AssertionError("background flusher raced the segment roll", flusherErr.get());
            }
            // The background flush must NEVER hit a stale/closed fd (the use-after-close race the
            // openNewSegment() flush-before-roll guard prevents). The sweep swallows a flush failure (so one
            // bad writer can't wedge it) but counts it — a non-zero count here means the race fired (EBADF on
            // a segment-rolled fd). With the guard this stays 0.
            Assert.assertEquals(
                    "the background flusher raced a segment roll (fdatasync of a closed/reused fd)",
                    0L, engine.getWalGroupCommitFlushQueue().getFailedFlushCount()
            );

            // All committed rows must apply + read back correct (no torn segment from the race).
            drainWalQueue();
            assertQuery("select count() from x").noRandomAccess().expectSize().returns("count\n" + rows + "\n");
            assertQuery("select sum(v) from x").noRandomAccess().expectSize().returns("sum\n" + ((long) (rows - 1) * rows / 2) + "\n");
        });
    }

    /**
     * (i) TEARDOWN RACE: a background sweep that captured a writer reference (weakly-consistent iterator)
     * must not fdatasync a closed fd when the writer is simultaneously closed / distressed. The
     * {@code dropPendingDurable()} helper clears the pending fields AND deregisters under the writer monitor
     * BEFORE {@code doClose} closes any fd, so a concurrent sweep entering the synchronized block finds
     * {@code pendingDurableSeqTxn == -1} and no-ops without touching any fd.
     *
     * <p>This stress test commits rows under W&gt;0 (leaving a pending group-commit tail in the registry),
     * then CLOSES the writer while a background-flusher thread is spinning the sweep concurrently. Any
     * fdatasync-on-closed-fd (use-after-close) shows up as a non-zero {@code failedFlushCount}.
     */
    @Test
    public void testBackgroundFlusherDoesNotRaceTeardown() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Tiny window so the flusher fires immediately — maximising the chance it runs while the
        // writer is mid-close and still in the registry (before dropPendingDurable deregisters it).
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "1");

        final int iterations = 50; // repeat open→commit→close many times to stress the race window
        final int rowsPerIter = 10;
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");

            final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            final ExposedWalPurgeJob flusher = new ExposedWalPurgeJob(engine);
            final Thread flusherThread = new Thread(() -> {
                while (!stop.get()) {
                    flusher.flushNow();
                }
                io.questdb.std.str.Path.clearThreadLocals();
            }, "group-commit-flusher-teardown");
            flusherThread.start();

            try {
                for (int iter = 0; iter < iterations; iter++) {
                    try (WalWriter w = engine.getWalWriter(tt)) {
                        for (int i = 0; i < rowsPerIter; i++) {
                            commitRow(w, (long) (iter * rowsPerIter + i) * 1_000_000L, iter * rowsPerIter + i);
                        }
                        // writer closes here with pending state still in the flush registry
                    }
                }
            } finally {
                stop.set(true);
                flusherThread.join();
                flusher.close();
            }

            // dropPendingDurable() in cleanupBeforeClose/doClose must have prevented any fdatasync on a
            // closed fd. A non-zero count would mean the use-after-close race fired.
            Assert.assertEquals(
                    "background flusher raced teardown (fdatasync on a closed fd)",
                    0L, engine.getWalGroupCommitFlushQueue().getFailedFlushCount()
            );
        });
    }

    /** Append one (ts, v) row to a HELD WalWriter and commit it (one WAL txn, writer NOT released). */
    private static void commitRow(WalWriter w, long ts, long v) {
        TableWriter.Row row = w.newRow(ts);
        row.putLong(1, v);
        row.append();
        w.commit();
    }

    /**
     * Exposes {@link WalPurgeJob}'s package-cadence {@code runSerially()} so the test can drive the
     * background-flush pass deterministically against the test microsecond clock (the flush trigger
     * compares {@code now - pendingSince >= W}; the WalPurgeJob's broad sweep includes the adaptive
     * group-commit flush of idle writers).
     */
    static class ExposedWalPurgeJob extends WalPurgeJob {
        ExposedWalPurgeJob(io.questdb.cairo.CairoEngine engine) {
            super(engine);
        }

        public boolean flushNow() {
            return runSerially();
        }
    }

    /**
     * A FilesFacade that counts {@code fdatasync} calls on WAL-commit files, split into events-file
     * device flushes and total WAL device flushes (segment column data + events + sequencer part/header).
     * Used to prove the W=0 per-commit fdatasync vs the W&gt;0 batched fdatasync.
     */
    static class WalFdatasyncFacade extends TestFilesFacadeImpl {
        private final List<String> fdatasyncPaths = new ArrayList<>();
        private final Map<Long, String> fdToPath = new HashMap<>();

        public int eventFdatasyncs() {
            int c = 0;
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                final String p = fdatasyncPaths.get(i);
                if (p.endsWith(WalUtils.EVENT_FILE_NAME) || p.endsWith(WalUtils.EVENT_FILE_NAME + ".")) {
                    c++;
                }
            }
            return c;
        }

        public void reset() {
            fdatasyncPaths.clear();
        }

        public int walFdatasyncs() {
            int c = 0;
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                if (isWalCommitFile(fdatasyncPaths.get(i))) {
                    c++;
                }
            }
            return c;
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            final String p = fdToPath.get(fd);
            if (p != null) {
                fdatasyncPaths.add(p);
            }
        }

        @Override
        public long openAppend(LPSZ name) {
            return track(super.openAppend(name), name);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            return track(super.openCleanRW(name, size), name);
        }

        @Override
        public long openRO(LPSZ name) {
            return track(super.openRO(name), name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return track(super.openRW(name, opts), name);
        }

        private static boolean isWalCommitFile(String p) {
            final boolean inWalDir = p.contains("/wal") || p.contains("\\wal");
            final boolean isSeqFile = p.contains(WalUtils.TXNLOG_PARTS_DIR)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".");
            return inWalDir || isSeqFile;
        }

        private long track(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
            return fd;
        }
    }
}
