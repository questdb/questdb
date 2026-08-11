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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deferred 2 — adaptive GROUP-COMMIT (the RPO knob {@code cairo.adaptive.commit.group.window}).
 *
 * <p>W=0 is synchronous fsync-before-return (zero loss); the shipped default is W=50ms. When {@code W > 0}, the WAL
 * fdatasync (the device flush) is BATCHED across an adaptive table's commits within window {@code W}:
 * {@code commit0} returns after the txn is sequenced (commit-ack = sequenced, msync'd to page cache,
 * NOT yet device-durable), and the fdatasync is performed by a batched flush. {@code localDurableSeqTxn}
 * advances only when the batch fdatasync completes, so the durable-ack only fires after the batch is on
 * disk.
 *
 * <p>These tests pin the MECHANISM:
 * <ul>
 *   <li>(a) the config default is 50ms;</li>
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
    public void testBackgroundFdatasyncFailurePoisonsEngineWithoutRetryOrFrontierAdvance() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

        final FailingWalFdatasyncFacade ff = new FailingWalFdatasyncFacade();
        try {
            assertMemoryLeak(ff, () -> {
                setCurrentMicros(1_000_000L);
                execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("x");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                final AtomicInteger fatalCalls = new AtomicInteger();
                engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());

                try (WalWriter writer = engine.getWalWriter(tt); ExposedWalPurgeJob purgeJob = new ExposedWalPurgeJob(engine)) {
                    commitRow(writer, 0, 1);
                    final long durableBefore = tracker.getLocalDurableSeqTxn();
                    ff.failNext();
                    setCurrentMicros(1_000_000L + WINDOW_US + 1);

                    try {
                        purgeJob.flushNow();
                        Assert.fail("background fdatasync failure must remain fatal");
                    } catch (CairoError expected) {
                        Assert.assertTrue(CairoException.isDataSyncFailure(expected));
                    }

                    Assert.assertEquals(1, fatalCalls.get());
                    Assert.assertEquals(1, ff.failureAttempts);
                    Assert.assertTrue(engine.isDurabilityFailed());
                    Assert.assertEquals(durableBefore, tracker.getLocalDurableSeqTxn());

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
            resetDurabilityPoisonForTest();
        }
    }

    @Test
    public void testGroupWindowDefaultsTo50Ms() throws Exception {
        assertMemoryLeak(() -> Assert.assertEquals(
                "cairo.adaptive.commit.group.window default must be 50ms (50000us) — RPO<=50ms out of the box",
                50_000L, engine.getConfiguration().getAdaptiveCommitGroupWindowUs()
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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");

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
     * where the WAL writer is kept across many commits) keeps each writer-private WAL durable before
     * sequencing while BATCHING the shared sequencer fdatasync, and {@code localDurableSeqTxn} LAGS the
     * committed seqTxn until that shared-frontier flush fires.
     *
     * <p>NB: each {@code execute("insert")} round-trips the WAL-writer pool and RELEASES the writer, which
     * (correctly) flushes its pending tail on the clean handoff — so batching is only observable while the
     * writer is held. This test holds one {@link WalWriter} and commits on it directly.
     */
    @Test
    public void testWindowPositiveBatchesFsyncAndLagsDurable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

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

                // Safe W>0 fallback: writer-private data/events are fdatasync'd for every commit before
                // sequencing, but the shared sequencer flush is deferred. This prevents one writer's flush
                // from publishing another writer's private, not-yet-durable WAL record.
                Assert.assertTrue(
                        "W>0 must make every writer-private commit durable before sequencing",
                        ff.privateWalFdatasyncs() >= n
                );
                Assert.assertTrue(
                        "W>0 must batch the shared sequencer fdatasync",
                        ff.sequencerFdatasyncs() < n
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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

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

    @Test
    public void testBackgroundFlusherTreatsBackwardClockStepAsExpiredWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

        assertMemoryLeak(() -> {
            setCurrentMicros(2_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter w = engine.getWalWriter(tt); ExposedWalPurgeJob flusher = new ExposedWalPurgeJob(engine)) {
                commitRow(w, 0L, 0L);
                final long pendingSeqTxn = tracker.getSeqTxn();
                Assert.assertTrue(tracker.getLocalDurableSeqTxn() < pendingSeqTxn);

                // The production clock is wall time and can move backwards. Waiting for it to catch up would
                // extend the promised idle-tail RPO by the size of the clock correction.
                setCurrentMicros(1_000_000L);
                flusher.flushNow();
                Assert.assertTrue("a backwards clock step must force the pending durability barrier",
                        tracker.getLocalDurableSeqTxn() >= pendingSeqTxn);
            }
        });
    }

    /**
     * A read-only (soft-linked) attached partition must not stall the epoch on a platform whose syncfs is
     * not filesystem-wide, where {@code fsyncAttachedPartitionFiles} is the epoch's column-flush path.
     * <p>
     * Such a partition is backed by storage this writer never writes, so it holds nothing an epoch could
     * need to flush, and {@code openRW} on its files fails. That failure is not contained: it unwinds into
     * {@code handleBestEffortDurableEpochFailure}, which only logs, so the epoch never reaches
     * {@code setLastEpochTs} — {@code durableEpochSeqTxn} stays 0, {@link WalPurgeJob} pins the purge floor
     * at 0, and the WAL grows without bound while the whole scan is retried every apply batch. Asserting the
     * PUBLISHED epoch, rather than merely "it did not throw", is what separates the fix from the swallow
     * that hid the failure.
     */
    /**
     * The epoch's non-wide-syncfs flush must scale with the UN-EPOCHED WRITE SET, not with table history.
     * Everything older than the previous epoch was made durable by that epoch, so re-walking it buys nothing
     * and costs three syscalls per file per partition, forever, at the epoch cadence.
     * <p>
     * Pins both halves: the first epoch of a writer's life still walks everything (it must cover partitions a
     * previous writer instance left un-epoched), and a second epoch that follows a write to ONE partition
     * opens files under that partition only.
     */
    @Test
    public void testEpochFlushesOnlyTheWriteSetOnNonWideSyncfsPlatform() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "-1");

        final EpochOpenTrackingFacade ff = new EpochOpenTrackingFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");

            // Three days of history.
            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 0L, 1L);
                commitRow(walWriter, Micros.DAY_MICROS, 2L);
                commitRow(walWriter, 2 * Micros.DAY_MICROS, 3L);
            }
            drainWalQueue();

            try (TableWriter tableWriter = getWriter(tt)) {
                Assert.assertEquals(3, tableWriter.getTxWriter().getPartitionCount());

                // First epoch of this writer's life: the write set cannot be vouched for, so everything is
                // walked. This is the fail-safe that covers a previous writer instance's un-epoched work.
                ff.reset();
                tableWriter.advanceDurableEpoch(1L);
                Assert.assertTrue(
                        "the first epoch after open must walk every partition, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-01")
                                && ff.openedPartitionDirs().contains("1970-01-02")
                                && ff.openedPartitionDirs().contains("1970-01-03")
                );

                // Second epoch, with nothing written since the first: no partition owes a flush.
                ff.reset();
                tableWriter.advanceDurableEpoch(2L);
                Assert.assertTrue(
                        "an epoch with an empty write set must open no partition file, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().isEmpty()
                );
            }

            // Write to the LAST partition only, then epoch again: history must not be re-walked.
            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 2 * Micros.DAY_MICROS + 1, 4L);
            }
            drainWalQueue();

            try (TableWriter tableWriter = getWriter(tt)) {
                ff.reset();
                tableWriter.advanceDurableEpoch(3L);
                Assert.assertTrue(
                        "the epoch must touch the written partition, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-03")
                );
                Assert.assertFalse(
                        "the epoch must NOT re-walk untouched history, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-01")
                );
                Assert.assertFalse(
                        "the epoch must NOT re-walk untouched history, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-02")
                );
            }
        });
    }

    @Test
    public void testEpochSkipsReadOnlyPartitionsOnNonWideSyncfsPlatform() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "-1");

        final ReadOnlyPartitionSyncfsFacade ff = new ReadOnlyPartitionSyncfsFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            // Two partitions, so one can be flagged read-only while the other still carries the epoch.
            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 0L, 1L);
                commitRow(walWriter, Micros.DAY_MICROS, 2L);
            }
            drainWalQueue();

            final long appliedSeqTxn = tracker.getWriterTxn();
            try (TableWriter tableWriter = getWriter(tt)) {
                Assert.assertEquals(2, tableWriter.getTxWriter().getPartitionCount());
                // Flag the FIRST partition read-only exactly as attachPartition does for a soft link, and
                // make its files unopenable for write exactly as the read-only mount behind one would.
                tableWriter.getTxWriter().setPartitionReadOnlyByTimestamp(0L, true);
                ff.denyWritesUnder("1970-01-01");

                tableWriter.advanceDurableEpoch(1L);
            }

            Assert.assertEquals(
                    "the epoch must publish despite the read-only partition",
                    appliedSeqTxn,
                    tracker.getDurableEpochSeqTxn()
            );
            Assert.assertEquals(
                    "no file under a read-only partition may be opened for write by the epoch",
                    0,
                    ff.deniedOpenAttempts()
            );
        });
    }

    @Test
    public void testEpochCannotPublishAheadOfDeferredSequencerOnNonWideSyncfsPlatform() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "-1");

        final NonWideSyncfsWalFdatasyncFacade ff = new NonWideSyncfsWalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 0L, 1L);
                drainWalQueue();
                final long appliedSeqTxn = tracker.getWriterTxn();
                Assert.assertTrue("the W>0 sequencer barrier must still be pending",
                        tracker.getLocalDurableSeqTxn() < appliedSeqTxn);

                ff.reset();
                try (TableWriter tableWriter = getWriter(tt)) {
                    tableWriter.advanceDurableEpoch(1L);
                }

                Assert.assertTrue("a non-wide epoch must fdatasync the sequencer before publishing",
                        ff.sequencerFdatasyncs() > 0);
                Assert.assertTrue("the epoch cut must be inside the local durable WAL prefix",
                        tracker.getLocalDurableSeqTxn() >= appliedSeqTxn);
                Assert.assertEquals(appliedSeqTxn, tracker.getDurableEpochSeqTxn());
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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "1");
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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "1");

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

    /**
     * (j) CONTIGUOUS DURABLE PREFIX across concurrent writers (CRITICAL 2 durable-ack safety). Two WalWriters
     * of ONE adaptive W&gt;0 table share one {@code SeqTxnTracker}. Writer A commits the LOWER seqTxn (N) and
     * writer B the HIGHER (N+1); both are pending (msync'd, not device-durable). B then flushes its own batch
     * OUT OF ORDER (commit-driven trigger) while A's txn N is still only in the page cache.
     *
     * <p>The durable-ack frontier ({@code localDurableSeqTxn}) must advance only to the CONTIGUOUS durable
     * prefix — i.e. it must stay {@code < N} because A's txn N is the oldest un-flushed HOLE — even though B's
     * (higher) txn is now on disk. The pre-fix bug advances the shared frontier to B's own seqTxn
     * ({@code setLocalDurableSeqTxn(flushTo)}), falsely claiming A's non-durable txn N as durable, so a QWP
     * durable-ack would lie and acknowledged data would be lost on a power cut. A counting facade proves A's
     * segment fdatasync never ran (its commit is genuinely non-durable) while B's did.
     */
    @Test
    public void testTwoWritersOutOfOrderFlushKeepsFrontierAtContiguousPrefix() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));

        final WalFdatasyncFacade ff = new WalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter a = engine.getWalWriter(tt); WalWriter b = engine.getWalWriter(tt)) {
                Assert.assertNotEquals("two concurrently-held writers must be distinct WALs sharing one tracker",
                        a.getWalId(), b.getWalId());

                // A commits FIRST -> lower seqTxn N. Deferred (pending, un-flushed).
                setCurrentMicros(1_000_000L);
                commitRow(a, 0L, 10);
                final long seqA = tracker.getSeqTxn();

                // B commits -> higher seqTxn N+1. Deferred (pending, un-flushed).
                setCurrentMicros(1_001_000L);
                commitRow(b, 60_000_000L, 11);
                final long seqB = tracker.getSeqTxn();
                Assert.assertEquals("B must have sequenced immediately after A", seqA + 1, seqB);

                // Nothing flushed yet: the durable frontier lags both writers.
                Assert.assertTrue("tail must be pending before any flush", tracker.getLocalDurableSeqTxn() < seqA);
                ff.reset();

                // Advance PAST the window, then commit AGAIN on B: the commit-driven trigger flushes B's batch
                // ONLY (A is a different writer with its own monitor + pending state; the flush is per-writer
                // with no cross-writer barrier).
                setCurrentMicros(1_000_000L + WINDOW_US + 1000L);
                commitRow(b, 120_000_000L, 12);

                // B's segment IS now device-durable (its batch fdatasync ran) ...
                Assert.assertTrue(
                        "B's own segment must have been fdatasync'd by its commit-driven flush",
                        ff.walDirFdatasyncs(b.getWalId()) > 0
                );
                // ... but A NEVER flushed: A's txn N is a HOLE, its segment fdatasync has NOT run.
                Assert.assertEquals(
                        "A's segment must NOT have been fdatasync'd — its txn " + seqA + " is still page-cache only",
                        0, ff.walDirFdatasyncs(a.getWalId())
                );

                // THE DURABLE-ACK CONTRACT: the frontier may only cover the contiguous durable prefix. A's txn
                // N is the oldest un-flushed hole, so the frontier MUST stay < N. The bug advances it to B's
                // own (higher) seqTxn, over-claiming A's non-durable txn N as durable.
                Assert.assertTrue(
                        "durable-ack OVER-CLAIM: localDurableSeqTxn (" + tracker.getLocalDurableSeqTxn()
                                + ") must NOT reach A's un-flushed seqTxn (" + seqA + "); it may only cover the "
                                + "contiguous durable prefix (< " + seqA + ")",
                        tracker.getLocalDurableSeqTxn() < seqA
                );
            }
        });
    }

    /**
     * A writer torn down WITHOUT a device flush must ORPHAN its contiguous-prefix pin, not hold it forever.
     *
     * <p>The pin exists so a peer's flush cannot over-claim this writer's still-page-cache-only sequencer
     * records as durable (CRITICAL-2). Removing it at teardown would be exactly that over-claim, so teardown
     * must keep the floor. But nothing ever calls {@code markWriterDurable(walId)} for a DEAD writer, so
     * simply keeping it froze the shared frontier at {@code min(pending) - 1} for the rest of the process
     * lifetime: every later QWP durable-ack for the table stalled, rescued only incidentally (and up to a
     * full epoch interval later) by the apply-side epoch jumping the frontier -- and never at all for a table
     * that received no further applies.
     *
     * <p>The orphan mechanism keeps the honest floor at teardown and reaps it on the next PEER flush. That is
     * sound because a W&gt;0 batch defers ONLY the shared sequencer barrier: every writer fdatasyncs its
     * private segment columns and events file BEFORE sequencing, so one
     * {@code sequencer.fdatasyncTxnLog()} by any writer makes the dead writer's already-sequenced txns
     * device-durable too.
     *
     * <p>Both halves are asserted, in order: the floor HOLDS immediately after the simulated power loss
     * (a test that only checked the release would also pass on a naive "just drop the pin at teardown"
     * implementation, which reintroduces the over-claim), and it is RELEASED by the peer flush.
     */
    @Test
    public void testTornDownWriterOrphansPinAndPeerFlushReapsIt() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        // Keep the apply-side epoch out of the way: it advances localDurableSeqTxn by a DIFFERENT route
        // (setLocalDurableSeqTxn bypasses the pin map), which would mask whether the pin itself was reaped.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");

        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            // ---- writer A commits inside the window, then "loses power" before its batch fdatasync ----
            final long deadSeqTxn;
            try (WalWriter a = engine.getWalWriter(tt)) {
                commitRow(a, 0L, 0);
                deadSeqTxn = tracker.getSeqTxn();
                Assert.assertEquals("test setup: A's commit must be pending, holding exactly one pin",
                        1, tracker.getPendingWriterPinCount());
                Assert.assertTrue("test setup: A's tail must not be durable yet",
                        tracker.getLocalDurableSeqTxn() < deadSeqTxn);

                a.simulatePowerLossDropPending();
            }

            // HALF 1 -- the floor must HOLD. A's sequencer record was never device-flushed, so the frontier
            // must still be honestly behind it; the pin survives teardown, merely marked orphaned.
            Assert.assertEquals("a torn-down writer's pin must be ORPHANED, not removed",
                    1, tracker.getOrphanedWriterPinCount());
            Assert.assertEquals("the orphaned pin must still be held",
                    1, tracker.getPendingWriterPinCount());
            Assert.assertTrue(
                    "the durable frontier must NOT advance over a batch that was never device-flushed",
                    tracker.getLocalDurableSeqTxn() < deadSeqTxn
            );

            // ---- writer B commits and its batch flush lands ----
            try (WalWriter b = engine.getWalWriter(tt)) {
                setCurrentMicros(2_000_000L);
                commitRow(b, 60_000_000L, 1);
                final long liveSeqTxn = tracker.getSeqTxn();

                // Push the clock past W and drive the background flusher: B's fdatasyncTxnLog covers the
                // whole shared sequencer log, A's orphaned records included.
                setCurrentMicros(2_000_000L + WINDOW_US + 1000L);
                try (ExposedWalPurgeJob flusher = new ExposedWalPurgeJob(engine)) {
                    flusher.flushNow();
                }

                // HALF 2 -- the orphan must be REAPED and the frontier freed.
                Assert.assertEquals("a peer's device flush must reap the orphaned pin",
                        0, tracker.getOrphanedWriterPinCount());
                Assert.assertEquals("no pin may remain once every live writer has flushed",
                        0, tracker.getPendingWriterPinCount());
                Assert.assertTrue(
                        "the durable frontier must be freed past the dead writer's floor: localDurableSeqTxn ("
                                + tracker.getLocalDurableSeqTxn() + ") must reach the committed seqTxn ("
                                + liveSeqTxn + ")",
                        tracker.getLocalDurableSeqTxn() >= liveSeqTxn
                );
            }
        });
    }

    /**
     * Append one (ts, v) row to a HELD WalWriter and commit it (one WAL txn, writer NOT released).
     */
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
    private void resetDurabilityPoisonForTest() throws Exception {
        final java.lang.reflect.Field field = CairoEngine.class.getDeclaredField("durabilityFailure");
        field.setAccessible(true);
        ((AtomicReference<?>) field.get(engine)).set(null);
        engine.setDurabilityFailureHandler(failure -> {
        });
    }

    static class FailingWalFdatasyncFacade extends WalFdatasyncFacade {
        private boolean armed;
        private boolean failNext;
        private int failureAttempts;

        @Override
        public void fdatasync(long fd) {
            if (armed) {
                failureAttempts++;
            }
            if (failNext) {
                failNext = false;
                throw CairoException.dataSyncFailure(5, "fdatasync").put("injected WAL fdatasync failure");
            }
            super.fdatasync(fd);
        }

        void failNext() {
            armed = true;
            failNext = true;
        }
    }

    static class NonWideSyncfsWalFdatasyncFacade extends WalFdatasyncFacade {
        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }
    }


    /**
     * Non-wide syncfs plus a record of which partition directories the epoch opened files under.
     */
    static class EpochOpenTrackingFacade extends WalFdatasyncFacade {
        private final java.util.Set<String> openedPartitionDirs = java.util.concurrent.ConcurrentHashMap.newKeySet();

        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            for (int day = 1; day <= 3; day++) {
                final String dir = "1970-01-0" + day;
                if (Utf8s.containsAscii(name, dir)) {
                    openedPartitionDirs.add(dir);
                }
            }
            return super.openRW(name, opts);
        }

        public java.util.Set<String> openedPartitionDirs() {
            return openedPartitionDirs;
        }

        public void reset() {
            openedPartitionDirs.clear();
        }
    }

    /**
     * Non-wide syncfs plus a partition directory whose files refuse to open for write, standing in for the
     * read-only mount behind a soft-linked attached partition.
     */
    static class ReadOnlyPartitionSyncfsFacade extends WalFdatasyncFacade {
        private final AtomicInteger deniedOpenAttempts = new AtomicInteger();
        private volatile String deniedDir;

        public int deniedOpenAttempts() {
            return deniedOpenAttempts.get();
        }

        public void denyWritesUnder(String partitionDirName) {
            deniedDir = partitionDirName;
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final String denied = deniedDir;
            if (denied != null && Utf8s.containsAscii(name, denied)) {
                deniedOpenAttempts.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }
    }

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

        public int privateWalFdatasyncs() {
            int c = 0;
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                final String p = fdatasyncPaths.get(i);
                if ((p.contains("/wal") || p.contains("\\wal")) && !isSequencerFile(p)) {
                    c++;
                }
            }
            return c;
        }

        public int sequencerFdatasyncs() {
            int c = 0;
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                if (isSequencerFile(fdatasyncPaths.get(i))) {
                    c++;
                }
            }
            return c;
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

        /**
         * Count fdatasync calls that landed on a file inside the {@code walN} directory of the given WAL id
         * (the writer's own segment column data + events file). The directory boundary is matched exactly
         * ({@code /walN/} not {@code /walN0/}) so wal id 2 is not confused with wal id 20. Used to prove which
         * writer's segment was actually device-flushed when two writers of one table flush out of order.
         */
        public int walDirFdatasyncs(int walId) {
            int c = 0;
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                final String p = fdatasyncPaths.get(i);
                if (p.contains("/wal" + walId + "/") || p.contains("\\wal" + walId + "\\")) {
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

        private static boolean isSequencerFile(String p) {
            return p.contains(WalUtils.TXNLOG_PARTS_DIR)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".");
        }

        private static boolean isWalCommitFile(String p) {
            final boolean inWalDir = p.contains("/wal") || p.contains("\\wal");
            return inWalDir || isSequencerFile(p);
        }

        private long track(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
            return fd;
        }
    }
}
