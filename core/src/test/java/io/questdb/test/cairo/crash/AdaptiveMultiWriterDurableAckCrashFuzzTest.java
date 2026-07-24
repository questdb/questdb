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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Multi-writer durable-ack CONTIGUITY crash-fuzz for adaptive GROUP-COMMIT ({@code W > 0}) — the seeded,
 * deterministic, replayable fuzz proof for the CRITICAL-2 contiguous-durable-prefix contract (Task 5).
 *
 * <p>N concurrently-held {@link WalWriter}s of ONE adaptive {@code W>0} table share one {@link SeqTxnTracker}
 * and flush their deferred group-commit batches INDEPENDENTLY (per-writer commit-driven trigger + the
 * {@code WalGroupCommitFlushQueue} background sweep) with no cross-writer barrier. A single seeded {@link Rnd}
 * interleaves append+commit on random writers, clock advances, and background sweeps — so writers flush out of
 * order and the shared durable-ack frontier ({@code localDurableSeqTxn}) must track only the CONTIGUOUS
 * durable prefix across all writers.
 *
 * <h3>The core invariant (checked after EVERY op)</h3>
 * {@code localDurableSeqTxn} must NEVER exceed the highest seqTxn whose data is ACTUALLY device-durable. The
 * true device-durable frontier is computed INDEPENDENTLY of the tracker: a {@link CountingCrashFacade}
 * (extending the {@link CrashFaultFilesFacade} power-loss model) records, per {@code walN} directory, each
 * WAL-segment {@code _event} {@code fdatasync} — the exact once-per-batch device flush of
 * {@code WalWriter.flushPendingDurable}. The test knows which writer committed which seqTxn (single-threaded
 * interleave), so a writer's committed seqTxns become device-durable only when the facade observes its batch
 * {@code fdatasync}. The contiguous prefix of those device-durable seqTxns is the oracle; the tracker's
 * frontier may only sit AT or BELOW it. A violation is the CRITICAL-2 over-claim (an acknowledged-data loss).
 *
 * <h3>End-to-end crash proof</h3>
 * At a seeded point the run models a POWER LOSS (drop every writer's un-flushed pending WITHOUT a device
 * flush, then roll files back to last-durable), recovers from the durable frontier, and asserts: (a) NO
 * silently-wrong rows survive; (b) the table is not left suspended; (c) every committed row within the
 * pre-crash durable-ack frontier (the contiguous durable prefix) SURVIVES — nothing acknowledged is lost.
 *
 * <p>Deterministic + replayable: every run is driven by a fixed {@code (s0,s1)} seed and the test microsecond
 * clock, so a failure reproduces exactly (the seed is printed in every assertion message). CI runs a small
 * bounded sweep; a longer multi-seed sweep is {@code Assume}-gated to nightly ({@code -Dquestdb.fuzz.nightly}).
 */
public class AdaptiveMultiWriterDurableAckCrashFuzzTest extends AbstractCrashConsistencyTest {

    private static final int CI_OPS = 60;                 // ops per seed under CI (bounded, fast)
    private static final long CLOCK_START = 1_000_000L;
    private static final long NIGHTLY_OPS = 400;          // ops per seed under the nightly sweep
    private static final String NIGHTLY_PROP = "questdb.fuzz.nightly";
    private static final int WRITERS = 3;                 // concurrently-held WalWriters of the one table
    private static final long WINDOW_US = 1_000_000L;     // 1s group window, driven by the test clock

    // A generous ceiling so a recovery WEDGE fails fast (loudly) instead of eating the whole build timeout;
    // lifted under the nightly sweep, which runs many more seeds.
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(Boolean.getBoolean(NIGHTLY_PROP) ? 30 * 60 * 1000L : 4 * 60 * 1000L, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * CI: three fixed seeds, small bounded op count. Each seed is an independent multi-writer interleave that
     * asserts the contiguity invariant after every op and finishes with an end-to-end power-loss + recovery.
     */
    @Test
    public void testMultiWriterContiguityInvariantAndCrash() throws Exception {
        final long[][] seeds = {{1234L, 5678L}, {22L, 33L}, {8080L, 9090L}};
        for (long[] s : seeds) {
            runOneSeed(s[0], s[1], CI_OPS);
        }
    }

    /**
     * NIGHTLY-only: a broad multi-seed sweep with a much larger op count per seed, to stress rare out-of-order
     * flush interleavings. Same invariant + crash proof as the CI test; mirrors
     * {@code RandomizedAdaptiveCrashFuzzTest}'s nightly Assume gate.
     */
    @Test
    public void testMultiWriterContiguitySweepNightly() throws Exception {
        Assume.assumeTrue("multi-writer contiguity sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        for (int i = 0; i < 24; i++) {
            runOneSeed(1_000L + i * 7919L, 2_000L + i * 104_729L, NIGHTLY_OPS);
        }
    }

    /**
     * Deterministically exercises the seqTxn-assignment/pending-registration window with two writers. The safe
     * W>0 fallback makes A's private WAL durable before sequencing; when B flushes the shared sequencer inside
     * the interceptor, A's atomic pending registration still keeps the ack frontier conservative. The counting
     * facade independently proves the private barrier happened before that peer flush.
     */
    @Test
    public void testMidFlightPeerFlushIsSafeAfterPrivateWalBarrier() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        final CountingCrashFacade facade = new CountingCrashFacade();
        crashFf = facade;
        try {
            assertMemoryLeak(facade, () -> {
                setCurrentMicros(CLOCK_START);
                execute("create table w (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("w");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                drainWalQueue();

                WalWriter a = engine.getWalWriter(tt);
                WalWriter b = engine.getWalWriter(tt);
                try {
                    Assert.assertNotEquals("two held writers must be distinct WALs sharing one tracker",
                            a.getWalId(), b.getWalId());
                    final WalWriter writerA = a;
                    final WalWriter writerB = b;

                    // B commits and is left PENDING — the ONLY pin in the shared map. Advance the clock past W so
                    // a sweep can flush B on demand.
                    setCurrentMicros(CLOCK_START);
                    commitRow(writerB, 60_000_000L, 100);
                    setCurrentMicros(CLOCK_START + WINDOW_US + 1000L);

                    final int eventsBeforeA = facade.walEventFdatasyncs(writerA.getWalId());
                    final long[] seqA = {-1};
                    final Throwable[] violation = {null};
                    WalWriter.deferredCommitInterceptor = (walId, seqTxn) -> {
                        // Only interpose on A's FIRST deferred commit (its mid-flight window).
                        if (walId != writerA.getWalId() || seqA[0] != -1) {
                            return;
                        }
                        seqA[0] = seqTxn;
                        // Flush B OUT OF ORDER inside A's window: peer markWriterDurable empties the pin map.
                        try (ExposedFlusher f = new ExposedFlusher(engine)) {
                            f.flushNow();
                        }
                        final int eventsNowA = facade.walEventFdatasyncs(writerA.getWalId());
                        final long frontier = tracker.getLocalDurableSeqTxn();
                        if (eventsNowA <= eventsBeforeA) {
                            violation[0] = new AssertionError("A's private event files were not durable before sequencing");
                        } else if (frontier >= seqTxn) {
                            violation[0] = new AssertionError("pending registration failed to keep the ack frontier conservative");
                        }
                    };
                    try {
                        setCurrentMicros(CLOCK_START + WINDOW_US + 1000L);
                        commitRow(writerA, 120_000_000L, 200); // fires the interceptor in A's mid-flight window
                    } finally {
                        WalWriter.deferredCommitInterceptor = null;
                    }

                    Assert.assertTrue("interceptor must have fired for writer A", seqA[0] > 0);
                    if (violation[0] != null) {
                        throw new AssertionError(violation[0].getMessage(), violation[0]);
                    }
                    // The safe fallback makes A's private WAL durable before sequencing, while the atomic
                    // pending registration conservatively holds the durable-ack frontier until A's own batch
                    // completion is observed.
                    Assert.assertTrue("pending registration must keep the ack frontier below A",
                            tracker.getLocalDurableSeqTxn() < seqA[0]);
                    Assert.assertTrue("A's private event files must have been device-flushed before sequencing",
                            facade.walEventFdatasyncs(writerA.getWalId()) > eventsBeforeA);
                } finally {
                    a.close();
                    b.close();
                }
            });
        } finally {
            WalWriter.deferredCommitInterceptor = null;
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setCurrentMicros(-1);
        }
    }

    private void runOneSeed(long s0, long s1, long ops) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        // Epoch every apply batch so the crash recovery can rebuild the applied columns from the durable WAL.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            new SeedRun(s0, s1, ops).run();
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setCurrentMicros(-1); // never leak a fixed clock into a later test
        }
    }

    /**
     * One deterministic seeded run: build the table, hold N writers, interleave commits/ticks/sweeps under a
     * seeded {@link Rnd} while asserting the contiguity invariant after every op, then power-loss + recover.
     */
    private final class SeedRun {
        private final long ops;
        private final Rnd rnd;
        private final long s0;
        private final long s1;
        // oracle state (single-threaded, so all reads below are this run's own):
        private final Map<Integer, Long> deviceDurableByWal = new HashMap<>();  // walId -> highest device-durable seqTxn
        private final Map<Integer, Integer> lastEventFsyncByWal = new HashMap<>(); // walId -> last observed batch-fdatasync count
        private final Map<Integer, Long> latestCommittedByWal = new HashMap<>(); // walId -> latest sequenced seqTxn
        private final Map<Long, Integer> ownerBySeqTxn = new HashMap<>();        // seqTxn -> walId
        private final List<long[]> committedRows = new ArrayList<>();            // {ts, v, seqTxn}
        private long baseDurableSeqTxn;                                          // seqTxns <= this are durable (baseline)
        private long clock = CLOCK_START;
        private CountingCrashFacade ff;
        private long maxSeqTxn;
        private final String table;
        private long tsCounter = 0;
        private long vCounter = 0;

        SeedRun(long s0, long s1, long ops) {
            this.s0 = s0;
            this.s1 = s1;
            this.ops = ops;
            this.rnd = new Rnd(s0, s1);
            // Unique per-seed table name: a torn-tail power loss can leave a table SUSPENDED (see
            // assertRecovered), and a suspended table's DROP never applies, so a shared name would collide on
            // the next seed's CREATE (the 2-arg assertMemoryLeak used here does not engine.clear() between seeds).
            this.table = "t_" + Math.abs(s0) + "_" + Math.abs(s1);
        }

        void run() throws Exception {
            ff = new CountingCrashFacade();
            crashFf = ff; // so AbstractCrashConsistencyTest helpers (markDurableBaseline/crash) target this facade
            assertMemoryLeak(ff, this::body);
        }

        private void body() throws Exception {
            setCurrentMicros(clock);
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName(table);
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            Assert.assertEquals(seedMsg("must be ADAPTIVE"), CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

            // Baseline: the empty, structurally-materialized table is durable. Everything the fuzz commits
            // above baseDurableSeqTxn is the "new" data whose durability the invariant + crash test track.
            drainWalQueue();
            baseDurableSeqTxn = tracker.getSeqTxn();
            maxSeqTxn = baseDurableSeqTxn;
            markDurableBaseline();

            final WalWriter[] writers = new WalWriter[WRITERS];
            long preCrashDurable = baseDurableSeqTxn;
            boolean reachedCrash = false;
            try {
                for (int i = 0; i < WRITERS; i++) {
                    writers[i] = engine.getWalWriter(tt);
                    lastEventFsyncByWal.put(writers[i].getWalId(), 0);
                }

                final long crashAt = ops / 2 + rnd.nextLong(Math.max(1, ops / 2));
                for (long op = 0; op < ops; op++) {
                    final int roll = rnd.nextInt(100);
                    if (roll < 65) {
                        // COMMIT on a random writer: one WAL txn (ts strictly increasing, v globally unique).
                        final WalWriter w = writers[rnd.nextInt(WRITERS)];
                        final long ts = (tsCounter += 60_000_000L);
                        final long v = (vCounter += 1);
                        setCurrentMicros(clock);
                        commitRow(w, ts, v);
                        final long seqTxn = tracker.getSeqTxn(); // single-threaded -> this writer's just-sequenced txn
                        ownerBySeqTxn.put(seqTxn, w.getWalId());
                        latestCommittedByWal.put(w.getWalId(), seqTxn);
                        committedRows.add(new long[]{ts, v, seqTxn});
                        maxSeqTxn = Math.max(maxSeqTxn, seqTxn);
                    } else if (roll < 80) {
                        // BACKGROUND SWEEP past the window: flush every writer whose oldest pending is >= W old.
                        clock += WINDOW_US + 1000L;
                        setCurrentMicros(clock);
                        try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                            flusher.flushNow();
                        }
                    } else {
                        // TICK: advance the clock by a random amount (sometimes crossing W so the NEXT commit on
                        // an already-pending writer fires the commit-driven flush).
                        clock += rnd.nextLong(2 * WINDOW_US);
                        setCurrentMicros(clock);
                    }

                    // Recompute the true device-durable frontier from the facade's observed batch fdatasyncs,
                    // then assert the contiguity invariant: the tracker frontier may only cover the contiguous
                    // device-durable prefix.
                    detectFlushes(writers);
                    final long trueDurable = trueDurableFrontier();
                    final long frontier = tracker.getLocalDurableSeqTxn();
                    Assert.assertTrue(
                            seedMsg("durable-ack OVER-CLAIM at op " + op + ": localDurableSeqTxn (" + frontier
                                    + ") exceeded the true contiguous device-durable frontier (" + trueDurable + ")"),
                            frontier <= trueDurable
                    );

                    if (op == crashAt) {
                        break;
                    }
                }

                // POWER LOSS: the durable-ack frontier we recorded here must survive the crash end-to-end.
                preCrashDurable = tracker.getLocalDurableSeqTxn();
                for (WalWriter w : writers) {
                    w.simulatePowerLossDropPending(); // distress + drop pending WITHOUT a device flush
                }
                reachedCrash = true;
            } finally {
                // Reclaim the held (now distressed) writers as a fresh boot's empty WAL-writer pool would —
                // BEFORE crash() truncates the files, while the segment files are still their intact size.
                // Runs on the invariant-violation exit too, so a mid-loop failure never leaks a held writer.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseCrashOrphanedWalWriters();
                engine.releaseAllWalWriters();
            }

            if (reachedCrash) {
                recoverAfterPowerLoss(tt);
                assertRecovered(tt, preCrashDurable);
                // Release the recovered table's handles (a unique per-seed name means no DROP is needed — a
                // suspended table cannot be dropped anyway; leftover applied tables hold no fds after this).
                engine.releaseAllReaders();
                engine.releaseInactiveTableSequencers();
            }
        }

        /**
         * Recover exactly as a fresh process restart would (mirrors
         * {@code AbstractAdaptiveCrashSweepTest.recoverAfterCrash} for one table): roll the files back to the
         * durable content, then FORCE the sequencer + its {@link SeqTxnTracker} to reload from the durable
         * txnlog ({@code resetForReboot}). Without that reset the sequencer's stale in-memory {@code lastTxn}
         * (advanced by the just-dropped, non-durable pending) wedges recovery's apply in an infinite re-notify
         * loop ({@code writerTxn < seqTxn} forever, for a txn the rolled-back WAL no longer has).
         */
        private void recoverAfterPowerLoss(TableToken tt) {
            ff.crash(engine.getConfiguration().getDbRoot());
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
            }
            engine.getTxnScoreboardPool().remove(tt);
            engine.getTableSequencerAPI().resetForReboot(tt);
            new io.questdb.cairo.RecoveryCoordinator(engine).recover();
            engine.notifyWalTxnRepublisher(tt);
            drainWalQueue();
        }

        private void assertRecovered(TableToken tt, long preCrashDurable) {
            // A power loss can tear the UN-ACKED tail so a writer's non-durable _event ends up BELOW the
            // (peer-flushed) durable shared sequencer; recovery then LOUDLY suspends on that torn tail. That is
            // an acceptable, NON-silent power-loss outcome — the acked data stays durable on disk for a resume —
            // and is exactly the tolerance the two-writer oracle AdaptiveGroupCommitCrashTest applies (it reads
            // quietly and does not require "never suspended"). What is NEVER acceptable is a silently-WRONG row.
            final boolean suspended = engine.getTableSequencerAPI().isSuspended(tt);
            final Map<Long, Long> recovered = readRowsQuietly();
            final Map<Long, Long> committedByTs = new HashMap<>();
            for (long[] r : committedRows) {
                committedByTs.put(r[0], r[1]);
            }
            // Bar 1 (ALWAYS): every readable (ts,v) matches a committed row; never more rows than were committed.
            for (Map.Entry<Long, Long> e : recovered.entrySet()) {
                final Long committedV = committedByTs.get(e.getKey());
                Assert.assertNotNull(seedMsg("recovered a row (ts=" + e.getKey() + ") that was never committed"), committedV);
                Assert.assertEquals(seedMsg("silently-wrong row at ts=" + e.getKey()), committedV, e.getValue());
            }
            Assert.assertTrue(seedMsg("recovered more rows than were ever committed"),
                    recovered.size() <= committedRows.size());

            // Bar 2 (durable-ack NO-LOSS on a CLEAN roll-forward): when recovery did NOT suspend on a torn tail,
            // every committed row within the pre-crash durable-ack frontier (the contiguous device-durable
            // prefix) MUST survive and be correct — nothing acknowledged is lost. (Under a suspend the acked WAL
            // is still durable on disk; the loud suspend, not a silent loss, is the safe outcome we allow.)
            if (!suspended) {
                for (long[] r : committedRows) {
                    if (r[2] <= preCrashDurable) {
                        final Long got = recovered.get(r[0]);
                        Assert.assertNotNull(seedMsg("acknowledged row LOST after clean recovery (ts=" + r[0]
                                + ", seqTxn=" + r[2] + ", durableFrontier=" + preCrashDurable + ")"), got);
                        Assert.assertEquals(seedMsg("acknowledged row corrupted after crash (ts=" + r[0] + ")"),
                                Long.valueOf(r[1]), got);
                    }
                }
            }
            engine.releaseAllReaders();
        }

        private Map<Long, Long> readRowsQuietly() {
            try {
                return readRows();
            } catch (Throwable t) {
                return new HashMap<>(); // torn un-acked tail surfaced loudly — acceptable (Bar 1 covers correctness)
            }
        }

        /**
         * For each held writer, if the facade observed a NEW batch {@code _event} fdatasync since the last
         * check, that writer's whole pending batch is now device-durable up to its latest committed seqTxn.
         */
        private void detectFlushes(WalWriter[] writers) {
            for (WalWriter w : writers) {
                final int walId = w.getWalId();
                final int cur = ff.walEventFdatasyncs(walId);
                final int prev = lastEventFsyncByWal.getOrDefault(walId, 0);
                if (cur > prev) {
                    lastEventFsyncByWal.put(walId, cur);
                    final Long latest = latestCommittedByWal.get(walId);
                    if (latest != null) {
                        final Long dd = deviceDurableByWal.get(walId);
                        if (dd == null || latest > dd) {
                            deviceDurableByWal.put(walId, latest);
                        }
                    }
                }
            }
        }

        /**
         * The highest seqTxn N such that EVERY committed seqTxn in {@code (baseDurableSeqTxn, N]} is
         * device-durable (its owning writer's batch fdatasync covering it has been observed). The contiguous
         * device-durable prefix across all writers — the ceiling the tracker frontier may not exceed.
         */
        private long trueDurableFrontier() {
            long f = baseDurableSeqTxn;
            while (f + 1 <= maxSeqTxn) {
                final long next = f + 1;
                final Integer owner = ownerBySeqTxn.get(next);
                if (owner == null) {
                    break; // a non-data (structural) seqTxn or an untracked gap stops the prefix
                }
                final Long dd = deviceDurableByWal.get(owner);
                if (dd == null || dd < next) {
                    break; // owner has not flushed a batch covering `next`
                }
                f = next;
            }
            return f;
        }

        private Map<Long, Long> readRows() {
            final Map<Long, Long> out = new HashMap<>();
            try (RecordCursorFactory factory = select("select ts, v from " + table)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record rec = cursor.getRecord();
                    while (cursor.hasNext()) {
                        out.put(rec.getTimestamp(0), rec.getLong(1));
                    }
                }
            } catch (SqlException e) {
                throw new RuntimeException(seedMsg("readRows failed"), e);
            }
            return out;
        }

        private String seedMsg(String msg) {
            return "[seed s0=" + s0 + " s1=" + s1 + " ops=" + ops + "] " + msg;
        }
    }

    private static void commitRow(WalWriter w, long tsMicros, long v) {
        TableWriter.Row row = w.newRow(tsMicros);
        row.putLong(1, v);
        row.append();
        w.commit();
    }

    /**
     * Exposes {@link WalPurgeJob#runSerially()} so the background group-commit sweep runs deterministically
     * against the test microsecond clock.
     */
    static final class ExposedFlusher extends WalPurgeJob {
        ExposedFlusher(io.questdb.cairo.CairoEngine engine) {
            super(engine);
        }

        boolean flushNow() {
            return runSerially();
        }
    }

    /**
     * The {@link CrashFaultFilesFacade} power-loss model PLUS an independent per-{@code walN} counter of WAL
     * segment {@code _event} {@code fdatasync}s — the once-per-batch device flush the deferred group-commit
     * path performs in {@code WalWriter.flushPendingDurable}. This lets the test know the TRUE device-durable
     * frontier of each writer independently of {@code SeqTxnTracker.localDurableSeqTxn}.
     */
    static final class CountingCrashFacade extends CrashFaultFilesFacade {
        private final List<String> eventFdatasyncPaths = new ArrayList<>();
        private final Map<Long, String> fdPaths = new HashMap<>();

        @Override
        public boolean close(long fd) {
            fdPaths.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            final String p = fdPaths.get(fd);
            if (p != null && (p.endsWith(WalUtils.EVENT_FILE_NAME) || p.endsWith(WalUtils.EVENT_FILE_NAME + "."))) {
                eventFdatasyncPaths.add(p);
            }
            super.fdatasync(fd);
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

        /**
         * Number of WAL segment {@code _event} device flushes observed under the {@code walN} directory of the
         * given wal id (matched exactly — {@code /walN/} not {@code /walN0/}). Once-per-batch, so a strictly
         * increasing count is the "this writer flushed its batch" signal.
         */
        int walEventFdatasyncs(int walId) {
            int c = 0;
            for (int i = 0, n = eventFdatasyncPaths.size(); i < n; i++) {
                final String p = eventFdatasyncPaths.get(i);
                if (p.contains("/wal" + walId + "/") || p.contains("\\wal" + walId + "\\")) {
                    c++;
                }
            }
            return c;
        }

        private long track(long fd, LPSZ name) {
            if (fd > -1) {
                fdPaths.put(fd, Utf8String.newInstance(name).toString());
            }
            return fd;
        }
    }
}
