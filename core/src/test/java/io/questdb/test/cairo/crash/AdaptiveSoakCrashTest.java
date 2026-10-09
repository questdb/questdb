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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.datetime.MicrosecondClock;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Adaptive crash-recovery soak: sustained adaptive WAL ingest with a periodic simulated power loss and recovery,
 * over many cycles on one persistent db root that is never reset between cycles. The sweeps
 * ({@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint}, {@link RandomizedAdaptiveCrashFuzzTest})
 * rebuild a fresh table at every crash point to isolate one crash; this soak recovers and continues on the same
 * accumulating table to catch defects that only surface over many cycles:
 * <ul>
 *   <li>fd and mmap leaks across cycles;</li>
 *   <li>unbounded growth of WAL segment dirs (a WAL purge floor that never advances);</li>
 *   <li>unbounded growth of save point files;</li>
 *   <li>stale partition-version dirs the recovery rewind never purges;</li>
 *   <li>durable-frontier drift, or a committed count that truncates instead of accumulating.</li>
 * </ul>
 * It reuses the base classes' {@link CrashFaultFilesFacade}, the reboot model {@link #recoverAfterCrash}, the
 * per-cycle handle release and fd reclamation ({@link #releaseEngineHandles},
 * {@link #reclaimLingeringNonCacheFds}) and {@code anyTableSuspended}, driven per cycle rather than through the
 * reset-per-point {@code forEachAdaptiveCrashPoint} driver.
 *
 * <h3>Deterministic workload</h3>
 * One adaptive WAL table {@code t (id long, v long, s symbol index, ts timestamp)} partitioned by DAY. Row
 * {@code i} is a pure function of {@code i}: {@code id=i}, {@code v=i*2654435761L} (Knuth hash),
 * {@code s=SYMBOLS[i % 4]} (indexed symbol), {@code ts=BASE_TS + i*TS_STRIDE}. The 8h {@code TS_STRIDE} rolls the
 * DAY partition every few rows, exercising multi-partition commit and recovery at smoke scale. The committed state
 * is therefore always the identity prefix {@code {0..count-1}}; each cycle re-ingests contiguously from the
 * recovered frontier, as a WAL producer resumes from its last durable offset, so the membership oracle is the
 * exact-prefix check {@code id[i]==i}, {@code v[i]==i*MULT}, {@code s[i]==SYMBOLS[i%4]}.
 *
 * <h3>Per-cycle shape</h3>
 * <ol>
 *   <li><b>Durable prefix</b>: {@code PREFIX_ROWS} rows committed on a writer that is then closed (a clean handoff
 *       device-flushes it in {@code WalWriter.cleanupBeforeClose}) and drained, so they are materialized, durable
 *       and acknowledged. {@link #markDurableBaseline() markDurableBaseline} anchors the whole accumulated on-disk
 *       state as the durable floor.</li>
 *   <li><b>Crashable tail</b> on a held writer:
 *     <ul>
 *       <li><b>W=0</b> (synchronous): commit {@code TAIL_ROWS} rows (each {@code fdatasync}'d durable WAL, left
 *           unapplied, a lazy gap) and {@code armCrashAt} a durability op mid-tail, tearing the in-flight commit.
 *           Recovery rolls the durable tail forward.</li>
 *       <li><b>W&gt;0</b> (group commit, 50ms): commit the tail pending ({@code msync(MS_ASYNC)}, not
 *           device-durable), assert the durable-ack lags ({@code localDurableSeqTxn < seqTxn}), then
 *           {@code simulatePowerLossDropPending}: the unflushed tail is lost (RPO &le; W).</li>
 *     </ul></li>
 *   <li>{@link #recoverAfterCrash}, then the oracle, then continue on the same db root.</li>
 * </ol>
 *
 * <h3>Oracle, every cycle</h3>
 * (1) no silent corruption: recovered rows are an exact identity prefix; (2) no suspend after recovery; (3) every
 * row of the acknowledged prefix, captured before the crash, survives; (4) forward progress: the recovered count
 * is non-decreasing across cycles. W=0 also requires zero loss (the durable-ack never lags a completed commit);
 * W&gt;0 uses membership and the durable-ack bar (an unflushed tail &le;W may be lost).
 *
 * <h3>Leak and growth invariants over the whole soak</h3>
 * <ul>
 *   <li><b>fd/mmap</b>: the enclosing {@code assertMemoryLeak} (via {@link #runWithCrashFacade}) asserts open OS
 *       and cached fd counts return exactly to baseline at teardown and native memory is balanced. The soak also
 *       asserts the open OS fd count does not grow across cycles ({@code <= firstCycle + FD_SLACK}) and bounds the
 *       {@code fd/mmap reuse} deltas by a generous linear ceiling.</li>
 *   <li><b>WAL segments</b>: the real {@link io.questdb.cairo.wal.WalPurgeJob} broad sweep runs each cycle
 *       ({@link #forceWalPurge}). The wal dir count stays within one per cycle (see
 *       {@code WAL_DIRS_PER_CYCLE_SLACK} for the dir each in-process reboot leaves), which catches super-linear
 *       growth, and the per-wal segment high-water stays within {@code SEG_PER_WAL_CAP}, which catches a purge
 *       floor that does not advance within a wal's life.</li>
 *   <li><b>Save point files</b>: exactly two ({@code _epoch.manifest.0} and {@code _epoch.manifest.1}) and no
 *       other epoch file; a growing count is a leak.</li>
 *   <li><b>Partition versions</b>: tail-append never supersedes a partition, so the count of stale versioned
 *       partition dirs ({@code <day>.<n>}) stays 0; a recovery rewind that leaked a partition copy trips it.</li>
 * </ul>
 *
 * <h3>Budget</h3>
 * The default is a small smoke ({@link #SMOKE_CYCLES} cycles) that runs as an ordinary unit test.
 * {@code -Dsoak.cycles=<n>} or {@code -Dsoak.minutes=<m>} override it; under {@code -Dquestdb.fuzz.nightly=true}
 * the default becomes {@link #NIGHTLY_CYCLES} and the {@code @Rule} timeout lifts, as in
 * {@link RandomizedAdaptiveCrashFuzzTest}.
 */
public class AdaptiveSoakCrashTest extends AbstractAdaptiveCrashSweepTest {

    private static final String NIGHTLY_PROP = "questdb.fuzz.nightly";

    // Deterministic workload.
    private static final long BASE_TS = 1_704_067_200_000_000L;           // 2024-01-01T00:00:00.000000Z (micros)
    private static final long TS_STRIDE = 8L * 3_600L * 1_000_000L;       // 8h/row -> DAY rolls every 3 rows
    private static final long V_MULT = 2_654_435_761L;                    // Knuth multiplicative hash
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta"};

    private static final int PREFIX_ROWS = 4;   // durable prefix committed + applied + epoch'd each cycle
    private static final int TAIL_ROWS = 6;     // crashable tail on a held writer each cycle
    // W=0: arm the crash this many durability ops into the tail commits: small enough to always fire within
    // TAIL_ROWS commits, large enough that the first tail commit's ops elapse, so recovery rolls part of the durable
    // tail forward. The oracle asserts the crash fired, so a mis-tuned offset fails instead of skipping the crash.
    private static final int W0_CRASH_OFFSET = 3;
    private static final int WN_WINDOW_US = 50_000; // W>0 group-commit window (RPO knob a deployment trades)

    private static final int SMOKE_CYCLES = 8;      // default dev smoke (runs without the nightly flag)
    private static final int NIGHTLY_CYCLES = 240;  // default under -Dquestdb.fuzz.nightly=true

    // Growth and leak ceilings (see the class javadoc), above the healthy steady state and well below what the
    // corresponding leak would produce.
    //
    // WAL dirs: the adaptive purge floor does not advance across an in-process resetForReboot, so each cycle keeps
    // one fresh single-segment walN dir (purgeable, but broadSweep's cursor-derived nextToApply keeps it; the data
    // is applied and correct). The healthy rate is one wal dir per cycle; the bound catches super-linear growth (a
    // cycle leaking several wals). AdaptiveRebootOrphanReclaimCrashTest shows a real restart retains the same dir
    // and the next purge after the first post-reboot epoch reclaims it.
    private static final int WAL_DIRS_PER_CYCLE_SLACK = 2; // walDirs <= cycle + this (observed: == cycle)
    private static final int SEG_PER_WAL_CAP = 4;          // intra-wal segment high-water (observed: 1)
    private static final int FD_SLACK = 8;                 // open-OS-fd drift allowed across cycles (steady state ~0)

    // Per-cycle fd hygiene, as in the sweep driver: reclaim any non-cached fd a fault-interrupted fsync left open,
    // which a real power loss's process death would reclaim but the live JVM cannot. The held-writer crash path
    // below leaks none, so this is a guard that keeps a future change to the crash path leak-clean, not the
    // control that shows the growth checks can fail.
    private static final boolean RECLAIM_FDS_EACH_CYCLE = true;

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(Boolean.getBoolean(NIGHTLY_PROP) ? 3 * 60 * 60 * 1000L : 20 * 60 * 1000L, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * W=0 (synchronous) soak: every completed commit is device-durable, so recovery loses nothing: each cycle the
     * recovered count covers the whole acknowledged prefix and whatever tail commits {@code fdatasync}'d before the
     * crash (a lazy gap recovery rolls forward), and the durable-ack never lags a completed commit.
     */
    @Test
    public void testSoakW0() throws Exception {
        runSoak(0);
    }

    /**
     * W&gt;0 (group commit, 50ms) soak: the tail is committed pending and lost on the power loss (RPO &le; W), so the
     * zero-loss bar does not apply; recovery still lands on a consistent committed prefix, never suspends, and keeps
     * the acknowledged prefix. The clock is pinned so the group-commit window does not depend on load, as in
     * {@code DurableFrontierTest}.
     */
    @Test
    public void testSoakWN() throws Exception {
        runSoak(WN_WINDOW_US);
    }

    private void runSoak(int windowUs) throws Exception {
        final boolean nightly = Boolean.getBoolean(NIGHTLY_PROP);
        final long soakMinutes = Long.getLong("soak.minutes", 0L);
        final int soakCycles = Integer.getInteger("soak.cycles", nightly ? NIGHTLY_CYCLES : SMOKE_CYCLES);
        Assume.assumeTrue("soak budget must be positive", soakCycles > 0 || soakMinutes > 0);

        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(windowUs));
        // Epoch on every apply batch keeps both generations of _meta/_txn/_cv payloads refreshed, makes the
        // growth invariant meaningful, and makes recovery's roll-forward anchor deterministic.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        if (windowUs > 0) {
            // Pin the microsecond clock (see DurableFrontierTest, testConvertPartitionCrashSafeWN): under W>0 the
            // group-commit deferral trigger and the WAL-apply time quota both read the engine clock, so a fixed clock
            // keeps every commit inside the window (deterministic pending tail) and the apply quota effectively
            // unbounded (single pass), independent of load. Restored in the finally.
            setCurrentMicros(1_000_000L);
        }
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                final long fdReuse0 = Files.getFdReuseCount();
                final long mmapReuse0 = Files.getMmapReuseCount();

                execute("drop table if exists t");
                drainWalQueue();
                execute("create table t (id long, v long, s symbol index, ts timestamp) timestamp(ts) "
                        + "partition by day wal");
                TableToken tt = engine.verifyTableName("t");
                drainWalQueue(); // materialize the create; first durable epoch
                final TableToken[] tokens = {tt};

                final Set<Long> nonCacheFdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());

                int nextId = 0;        // global contiguous id cursor; = recovered count after each cycle
                int prevRecovered = 0; // forward-progress floor
                long openFdFloor = -1;  // open-OS-fd count captured after cycle 1 (steady state)

                final long deadlineMs = soakMinutes > 0 ? System.currentTimeMillis() + soakMinutes * 60_000L : Long.MIN_VALUE;
                int cycle = 0;
                while (deadlineMs != Long.MIN_VALUE ? System.currentTimeMillis() < deadlineMs : cycle < soakCycles) {
                    cycle++;

                    // 1. Durable prefix: rows [nextId, nextId+PREFIX) committed, closed (device-flushed), applied and
                    //    with an epoch, so durable and acknowledged.
                    try (WalWriter w = engine.getWalWriter(tt)) {
                        for (int j = 0; j < PREFIX_ROWS; j++) {
                            appendRow(w, nextId++);
                        }
                    } // clean close flushes pending-durable (cleanupBeforeClose) -> prefix is device-durable
                    drainWalQueue();
                    final int durableAckRows = nextId; // the whole prefix is durable & acked; MUST survive

                    final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                    if (windowUs == 0) {
                        // Zero loss: under W=0 the durable-ack never lags a completed commit.
                        Assert.assertEquals(
                                "cycle " + cycle + ": W=0 durable-ack must equal the committed frontier after a clean prefix",
                                tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn()
                        );
                    }

                    // Anchor the whole accumulated on-disk state as the durable floor (no reset: it accumulates).
                    markDurableBaseline();

                    // 2. Crashable tail on a held writer (mode-specific), then reboot and recover. The tail appends
                    // from
                    //    nextId; `recovered` below supersedes whatever it committed.
                    if (windowUs == 0) {
                        crashTailW0(tt, tokens, nextId);
                    } else {
                        crashTailWN(tt, tokens, tracker, nextId);
                    }

                    // 3. Oracle, every bar.
                    Assert.assertFalse("cycle " + cycle + ": table must NOT be suspended after recovery",
                            anyTableSuspended(tt)); // (2)
                    final int prefixLen = assertIdentityPrefixAllowTorn(cycle); // (1) membership / no silent corruption
                    final int recovered = (int) rowCount();
                    Assert.assertTrue("cycle " + cycle + ": a torn read cannot expose MORE identity rows than committed",
                            prefixLen <= recovered);
                    Assert.assertTrue(
                            "cycle " + cycle + ": durable-ack VIOLATED — the flushed prefix (" + durableAckRows
                                    + " rows) must survive, recovered=" + recovered, // (3)
                            recovered >= durableAckRows
                    );
                    Assert.assertTrue(
                            "cycle " + cycle + ": forward progress VIOLATED — recovered count went backwards ("
                                    + prevRecovered + " -> " + recovered + "); the soak must accumulate, not truncate", // (4)
                            recovered >= prevRecovered
                    );
                    prevRecovered = recovered;
                    nextId = recovered; // resume ingest contiguously from the durable frontier (re-fill lost tail)

                    // 4. Per-cycle WAL purge, handle release and fd reclaim, then the leak and growth asserts. Purge
                    //    before releasing the sequencer so broadSweep's forAllWalTables sees the table.
                    forceWalPurge();       // run the real WalPurgeJob broad-sweep (production maintenance)
                    releaseEngineHandles();
                    if (RECLAIM_FDS_EACH_CYCLE) {
                        // Close the per-cycle delta of non-cached fds a fault-interrupted fsync left open (a real power
                        // loss's process death reclaims them; the live JVM cannot). A no-op in the held-writer crash
                        // path.
                        reclaimLingeringNonCacheFds(nonCacheFdBaseline);
                    }

                    final StringBuilder walDbg = new StringBuilder();
                    final int[] wal = walStats(tt, walDbg); // {walDirs, totalSegs, maxSegsPerWal}
                    final int epochCopies = countEpochCopies(tt);
                    final StringBuilder partDbg = new StringBuilder();
                    final int[] part = partitionDirStats(tt, partDbg); // {totalDirs, distinctDays, staleExtras}
                    final long openFds = Files.getOpenFileCount();
                    LOG.info().$("[adaptive-soak W=").$(windowUs).$("us] cycle=").$(cycle)
                            .$(" recovered=").$(recovered)
                            .$(" walDirs=").$(wal[0]).$(" walSegs=").$(wal[1]).$(" maxSeg/wal=").$(wal[2])
                            .$(" {").$(walDbg.toString()).$("}")
                            .$(" epochCopies=").$(epochCopies)
                            .$(" partDirs=").$(part[0]).$(" days=").$(part[1]).$(" staleExtras=").$(part[2])
                            .$(" openFds=").$(openFds)
                            .$(" fdReuseD=").$(Files.getFdReuseCount() - fdReuse0)
                            .$(" mmapReuseD=").$(Files.getMmapReuseCount() - mmapReuse0).I$();

                    // WAL growth: at most one reboot-orphan wal dir per cycle (the adaptive purge-floor artifact), and
                    // no intra-wal segment explosion. Super-linear dir growth or a growing per-wal segment count (a WAL
                    // purge floor not advancing within a wal's life) trips these.
                    Assert.assertTrue(
                            "cycle " + cycle + ": WAL dir growth SUPER-LINEAR — walDirs=" + wal[0]
                                    + " > cycle(" + cycle + ") + slack(" + WAL_DIRS_PER_CYCLE_SLACK
                                    + ") — a cycle leaked more than the one reboot-orphan wal",
                            wal[0] <= cycle + WAL_DIRS_PER_CYCLE_SLACK
                    );
                    Assert.assertTrue(
                            "cycle " + cycle + ": intra-wal segment growth UNBOUNDED — maxSegmentsPerWal=" + wal[2]
                                    + " > cap " + SEG_PER_WAL_CAP + " (WAL-purge floor not advancing within a wal?)",
                            wal[2] <= SEG_PER_WAL_CAP
                    );
                    Assert.assertEquals(
                            "cycle " + cycle + ": the save point must be EXACTLY one file for each of "
                                    + "generations 0 and 1; a different count is an artifact leak",
                            2, epochCopies
                    );
                    // Partition-version leak: exactly one live directory per committed day. A recovery rewind or
                    // O3/append that leaks a stale superseded partition-version copy leaves more than one dir for some
                    // day (staleExtras>0). Day partitions grow with the data and are not bounded; only per-day
                    // duplication is.
                    Assert.assertEquals(
                            "cycle " + cycle + ": STALE partition-version dirs leaked (" + part[2] + " extra beyond "
                                    + part[1] + " distinct days) — a recovery rewind left an orphan partition copy: "
                                    + partDbg,
                            0, part[2]
                    );
                    if (openFdFloor < 0) {
                        openFdFloor = openFds; // establish steady state after the first full cycle
                    } else {
                        Assert.assertTrue(
                                "cycle " + cycle + ": open-OS-fd COUNT grew across cycles (fd leak) — floor="
                                        + openFdFloor + " now=" + openFds + " slack=" + FD_SLACK,
                                openFds <= openFdFloor + FD_SLACK
                        );
                    }
                }

                // Soak end: the run accumulated rows rather than truncating to a single cycle's worth.
                Assert.assertTrue("soak did not accumulate: recovered=" + prevRecovered + " after " + cycle + " cycles",
                        prevRecovered > PREFIX_ROWS);

                // Secondary churn guard: reuse counters are cache-hit signals that grow with work, so bound them by a
                // generous linear-in-cycles ceiling that only catches pathological growth. The fd and mmap leak
                // guarantee is the enclosing assertMemoryLeak's exact open-fd return and the per-cycle openFds ceiling
                // above.
                final long fdReuseDelta = Files.getFdReuseCount() - fdReuse0;
                final long mmapReuseDelta = Files.getMmapReuseCount() - mmapReuse0;
                final long reuseCeil = 5_000_000L * Math.max(1, cycle);
                Assert.assertTrue("fd reuse count blew up: " + fdReuseDelta + " over " + cycle + " cycles",
                        fdReuseDelta >= 0 && fdReuseDelta <= reuseCeil);
                Assert.assertTrue("mmap reuse count blew up: " + mmapReuseDelta + " over " + cycle + " cycles",
                        mmapReuseDelta >= 0 && mmapReuseDelta <= reuseCeil);
                LOG.info().$("[adaptive-soak W=").$(windowUs).$("us] DONE cycles=").$(cycle)
                        .$(" finalRecovered=").$(prevRecovered)
                        .$(" fdReuseDelta=").$(fdReuseDelta).$(" mmapReuseDelta=").$(mmapReuseDelta).I$();
            });
        } finally {
            if (windowUs > 0) {
                setCurrentMicros(-1); // -1 => real clock (harness default); never leak a fixed clock into a later test
            }
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * W=0 crashable tail: commit TAIL_ROWS on a held writer (each {@code fdatasync}'d durable WAL, left unapplied so
     * recovery rolls it forward), with a crash armed mid-tail that tears the in-flight commit. The completed tail
     * commits are durable and survive; the torn one is rolled back. Then the reboot.
     */
    private void crashTailW0(TableToken tt, TableToken[] tokens, int startId) {
        final int base = crashFf.durabilityOpCount();
        crashFf.armCrashAt(base + W0_CRASH_OFFSET);
        boolean fired = false;
        WalWriter w = engine.getWalWriter(tt);
        try {
            int id = startId;
            for (int j = 0; j < TAIL_ROWS; j++) {
                appendRow(w, id++); // each commit fdatasync's (W=0) -> durable WAL, left un-applied (a lazy gap)
            }
        } catch (CrashSimulationError propagated) {
            fired = true; // WAL-commit fsync path: the armed crash propagated out of commit()
        } finally {
            try {
                w.close(); // distressed close discards the torn tail without a false durable-ack
            } catch (CrashSimulationError closeTimeCrash) {
                fired = true;
            }
        }
        if (!fired) {
            crashFf.armCrashAt(-1); // disarm so a still-armed crash can't fire during recovery
        }
        Assert.assertTrue("W=0 crash never fired — W0_CRASH_OFFSET=" + W0_CRASH_OFFSET
                + " exceeded the tail's durability-op count; lower it", fired);
        recoverAfterCrash(tokens);
    }

    /**
     * W&gt;0 crashable tail: commit TAIL_ROWS pending on a held writer ({@code msync(MS_ASYNC)}, not device-durable),
     * assert the durable-ack lags the committed frontier (the RPO precondition), then
     * {@code simulatePowerLossDropPending}: the unflushed tail is lost. Then the reboot.
     */
    private void crashTailWN(TableToken tt, TableToken[] tokens, SeqTxnTracker tracker, int startId) {
        WalWriter w = engine.getWalWriter(tt);
        try {
            int id = startId;
            for (int j = 0; j < TAIL_ROWS; j++) {
                appendRow(w, id++); // PENDING under W>0 (msync(MS_ASYNC), not device-durable)
            }
            Assert.assertTrue("W>0 tail must be PENDING (durable-ack must lag the committed frontier)",
                    tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());
            w.simulatePowerLossDropPending(); // drop the un-flushed tail; no false durable-ack advance
        } finally {
            w.close();
        }
        recoverAfterCrash(tokens); // recoverAfterCrash performs crash(dbRoot) -> rolls back the non-durable tail
    }

    /**
     * Runs the WAL broad-sweep purge to completion. A stock {@link WalPurgeJob} broad-sweeps only when
     * {@code last + checkInterval < clock.getTicks()} and inits {@code last = now - checkInterval/2}, so a
     * same-instant drain (the harness default under a real or a pinned clock) never purges, and
     * {@code drainPurgeJob} leaves applied but inactive WAL dirs behind. A strictly increasing clock makes the broad
     * sweep fire regardless of the configured interval and of whether the engine clock is pinned (W&gt;0) or real.
     */
    private void forceWalPurge() {
        // All WAL writers are released here (recoverAfterCrash), so nothing live is at risk. This reclaims a wal's
        // applied earlier segments, but not the reboot-orphaned whole-wal dirs; see WAL_DIRS_PER_CYCLE_SLACK.
        engine.releaseAllWalWriters();
        final long step = engine.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
        final long[] tick = {1L};
        final MicrosecondClock incClock = () -> (tick[0] += step);
        try (WalPurgeJob job = new WalPurgeJob(engine, engine.getConfiguration().getFilesFacade(), incClock)) {
            job.run();
            job.run();
        }
    }

    /**
     * Appends one deterministic row for the given {@code id} to a held WalWriter and commits it (one WAL txn).
     */
    private void appendRow(WalWriter w, int id) {
        final TableWriter.Row row = w.newRow(BASE_TS + (long) id * TS_STRIDE);
        row.putLong(0, id);                             // id
        row.putLong(1, (long) id * V_MULT);             // v = Knuth hash
        row.putSym(2, SYMBOLS[id % SYMBOLS.length]);    // s (indexed symbol)
        // col 3: ts is the designated timestamp set by newRow(ts)
        row.append();
        w.commit();
    }

    /**
     * Membership oracle: reads {@code select id, v, s from t order by id} (torn-tolerant) and asserts the surviving
     * rows are an exact identity prefix: {@code id[i]==i}, {@code v[i]==i*V_MULT}, {@code s[i]==SYMBOLS[i%4]}. A
     * visible torn read is an acceptable crash outcome (only the prefix read before the tear is checked); a wrong or
     * absent id, v or symbol fails. Returns the prefix length observed.
     */
    private int assertIdentityPrefixAllowTorn(int cycle) {
        int i = 0;
        try (RecordCursorFactory f = select("select id, v, s from t order by id")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    final long id = r.getLong(0);
                    final long v = r.getLong(1);
                    final CharSequence s = r.getSymA(2);
                    Assert.assertEquals("cycle " + cycle + " row " + i + ": id not an identity prefix (corruption)",
                            (long) i, id);
                    Assert.assertEquals("cycle " + cycle + " row " + i + ": v silently WRONG (data corruption)",
                            (long) i * V_MULT, v);
                    Assert.assertEquals("cycle " + cycle + " row " + i + ": symbol silently WRONG (dict/index corruption)",
                            SYMBOLS[i % SYMBOLS.length], s == null ? null : s.toString());
                    i++;
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // Acceptable: corruption detected visibly; the prefix read before the tear is validated above.
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return i;
    }

    /**
     * count(*): the committed row count from metadata, reliable even if a column read would tear.
     */
    private long rowCount() {
        try (RecordCursorFactory f = select("select count() from t")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : 0L;
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private File tableDir(TableToken tt) {
        return new File(engine.getConfiguration().getDbRoot().toString(), tt.getDirName());
    }

    /**
     * WAL directory stats: {@code {walDirCount, totalSegmentDirs, maxSegmentsPerWal}}. Each crash-recover cycle
     * leaves one fresh single-segment {@code walN} dir (the reboot orphan the adaptive purge floor retains, see the
     * class javadoc), so the counts grow about one per cycle; {@code maxSegmentsPerWal} is the intra-wal segment
     * high-water, which a purge-floor failure within a wal would inflate.
     */
    private int[] walStats(TableToken tt, StringBuilder dbg) {
        int walDirs = 0, totalSegs = 0, maxSegsPerWal = 0;
        final File[] top = tableDir(tt).listFiles();
        if (top != null) {
            for (File e : top) {
                if (e.isDirectory() && e.getName().startsWith("wal") && isAllDigits(e.getName().substring(3))) {
                    walDirs++;
                    final File[] segDirs = e.listFiles();
                    int here = 0;
                    if (segDirs != null) {
                        for (File s : segDirs) {
                            if (s.isDirectory() && isAllDigits(s.getName())) {
                                totalSegs++;
                                here++;
                            }
                        }
                    }
                    maxSegsPerWal = Math.max(maxSegsPerWal, here);
                    if (dbg != null) {
                        dbg.append(e.getName()).append('/').append(here).append("seg ");
                    }
                }
            }
        }
        return new int[]{walDirs, totalSegs, maxSegsPerWal};
    }

    /**
     * Counts save point files ({@code _epoch.manifest.{0,1}}) and any other {@code *.epoch*} file.
     */
    private int countEpochCopies(TableToken tt) {
        final File[] files = tableDir(tt).listFiles();
        int n = 0;
        if (files != null) {
            for (File file : files) {
                final String name = file.getName();
                if (name.startsWith("_epoch.manifest.") || name.contains(".epoch")) {
                    n++;
                }
            }
        }
        return n;
    }

    /**
     * Partition-directory stats: {@code {totalPartitionDirs, distinctDays, staleExtras}}. A DAY partition dir is named
     * {@code YYYY-MM-DD}, optionally with a {@code .<txn>} version suffix (the live boundary partition can carry one).
     * {@code staleExtras} is the count of directories beyond one per day ({@code totalPartitionDirs - distinctDays}):
     * superseded partition-version copies that a healthy purge retires. Tail-append plus per-cycle purge keeps
     * {@code staleExtras == 0}; a leaked copy makes it grow.
     */
    private int[] partitionDirStats(TableToken tt, StringBuilder dbg) {
        final Set<String> days = new HashSet<>();
        int total = 0;
        final File[] top = tableDir(tt).listFiles();
        if (top != null) {
            for (File e : top) {
                if (!e.isDirectory() || !isDatePrefixedDir(e.getName())) {
                    continue;
                }
                total++;
                days.add(e.getName().substring(0, 10)); // YYYY-MM-DD day key
                if (dbg != null) {
                    dbg.append(e.getName()).append(' ');
                }
            }
        }
        return new int[]{total, days.size(), total - days.size()};
    }

    /**
     * True if {@code name} begins with a {@code YYYY-MM-DD} day key (a DAY partition dir, versioned or not).
     */
    private static boolean isDatePrefixedDir(String name) {
        if (name.length() < 10) {
            return false;
        }
        for (int i = 0; i < 10; i++) {
            final char ch = name.charAt(i);
            if (i == 4 || i == 7) {
                if (ch != '-') {
                    return false;
                }
            } else if (!Character.isDigit(ch)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllDigits(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
