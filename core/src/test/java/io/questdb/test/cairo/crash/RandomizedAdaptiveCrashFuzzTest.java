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
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Randomized adaptive crash-fuzz (SP-D increment D2). Builds on {@link AbstractAdaptiveCrashSweepTest}'s
 * exhaustive per-op sweep driver with a randomized/fuzzed workload harness (see
 * {@link io.questdb.test.cairo.fuzz.FuzzRunner} / {@link io.questdb.test.fuzz.FuzzTransaction}) — this
 * task (1 of 7) establishes only the base class and the two primitives every later task in this file
 * relies on: a canonical committed-state {@link #fingerprint} and a seqTxn-bound membership check
 * over recorded committed-state history.
 */
public class RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest {

    private final FuzzRunner fuzzer = new FuzzRunner();

    // testFullLibraryW0 sweeps the full op library at N≈648 durability ops/seed; a full untruncated crash
    // sweep (assertW0Bars requires cap≥N) is ~85 min/seed, so it is NIGHTLY-only and gets a longer ceiling
    // than the 20-min default. CI runs the lean testLeanLibraryW0 instead (small N, same op library).
    private static final String NIGHTLY_PROP = "questdb.fuzz.nightly";

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(Boolean.getBoolean(NIGHTLY_PROP) ? 3 * 60 * 60 * 1000L : 20 * 60 * 1000L, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Before
    public void setUpFuzzer() {
        fuzzer.withDb(engine, sqlExecutionContext);
        fuzzer.clearSeeds();
    }

    @After
    public void tearDownFuzzer() {
        fuzzer.after();
    }

    // Default = full destructive op library; the machinery self-check (Task 3) flips this to run a
    // minimal insert/O3 profile. Field lives here; Task 3 only toggles it.
    private boolean fuzzOverrideMinimal = false;

    // Canonical committed-state fingerprint: full ordered dump to a String.
    private String fingerprint(String table) throws SqlException {
        StringSink fp = new StringSink();
        printSql("select * from " + table + " order by ts", fp);
        return fp.toString();
    }

    private static final class CommittedState {
        private final String fingerprint;
        private final long seqTxn;

        private CommittedState(long seqTxn, String fingerprint) {
            this.seqTxn = seqTxn;
            this.fingerprint = fingerprint;
        }
    }

    // Match both causal progress and logical state. Destructive transactions can recreate an earlier
    // fingerprint at a later seqTxn, so fingerprint-only "last match" is not an ordering oracle.
    private static int matchingState(ObjList<CommittedState> history, long seqTxn, CharSequence state) {
        for (int i = history.size() - 1; i >= 0; i--) {
            final CommittedState candidate = history.getQuick(i);
            if (candidate.seqTxn == seqTxn && TestUtils.equals(candidate.fingerprint, state)) {
                return i;
            }
        }
        return -1;
    }

    private long tableSeqTxn(String table) {
        final TableToken token = engine.verifyTableName(table);
        return engine.getTableSequencerAPI().getTxnTracker(token).getWriterTxn();
    }

    // Uses the 22-arg overload (FuzzRunner.java:757) — the ONLY one that enables partitionToParquet
    // (12th), partitionToNative (13th), setParquetEncoding (20th), and addCoveringIndex (22nd). The
    // 16-arg overload silently leaves those at 0, so parquet/covering-index would NOT be exercised.
    private void configureFuzz() {
        if (fuzzOverrideMinimal) {
            //   cancel notSet null  rollbk cAdd cRem cRen cTyp  data eqTs pDrop pPq  pNat trunc tDrop ttl  repl symV qry  pEnc tFmt cIdx
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0, 0, 0, 0, 0, 0.6, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        } else {
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0,       // cancelRows, notSet, nullSet, rollback(=0: clean seqTxn map)
                    0.1, 0.05, 0.05, 0.05,      // colAdd, colRemove, colRename, colTypeChange
                    0.5, 0.0, 0.05, 0.03,       // dataAdd, equalTsRows(=0: canonical dump), partitionDrop, partitionToParquet
                    0.03, 0.05, 0.0, 0.05,      // partitionToNative, truncate, tableDrop(=0), setTtl
                    0.1, 0.0, 0.0, 0.02,        // replaceInsert(dedup), symbolAccessValidation, query, setParquetEncoding
                    0.0, 0.03);                 // setTableFormat(=0), addCoveringIndex
        }
        //                     isO3, fuzzRowCount, txns, strLen, symStrLen, symCount, initialRows=0, partitions
        fuzzer.setFuzzCounts(true, 200, 20, 4, 4, 4, 0, 3);
    }

    private ObjList<FuzzTransaction> generateTxns(Rnd rnd, String walTableName) throws Exception {
        configureFuzz();
        fuzzer.createInitialTableWal(walTableName, 0);   // 0 initial rows → deterministic (no nondeterministic data_temp seed)
        return fuzzer.generateTransactions(walTableName, rnd);
    }

    private ObjList<CommittedState> buildTwinFingerprints(String twinName, ObjList<FuzzTransaction> txns, Rnd applyRnd) throws Exception {
        fuzzer.createInitialTableWal(twinName, 0);
        // createInitialTableWal queues its unconditional "column top" ALTERs via WAL but never drains them,
        // so an undrained fp[0] would show the PRE-alter (fewer-column) schema -- a state no crash recovery
        // can ever land on, since those structural commits are already durable (markDurableBaseline runs
        // AFTER setup(), i.e. after this point) well before the swept transaction's own crash point. Drain
        // once here so fp[0] reflects the fully-materialized schema with 0 data rows, matching the
        // legitimate committed state a crash during the FIRST transaction's own commit recovers to.
        drainWalQueue();
        ObjList<CommittedState> history = new ObjList<>();
        history.add(new CommittedState(tableSeqTxn(twinName), fingerprint(twinName))); // empty, schema materialized
        final ObjList<FuzzTransaction> one = new ObjList<>();
        for (int i = 0, n = txns.size(); i < n; i++) {
            one.clear();
            one.add(txns.getQuick(i));
            fuzzer.applyToWal(one, twinName, 1, applyRnd);
            drainWalQueue();
            history.add(new CommittedState(tableSeqTxn(twinName), fingerprint(twinName))); // state after txn i
        }
        execute("drop table " + twinName);                          // crash(dbRoot) must not see the twin
        return history;
    }

    private static final String WAL_TABLE = "cf_wal";
    private static final String TWIN_TABLE = "cf_twin";

    private final class FuzzCrashWorkload implements AdaptiveCrashWorkload {
        private final long s0, s1;
        private ObjList<FuzzTransaction> txns;
        private ObjList<CommittedState> fp;   // built lazily, once
        private TableToken walToken;

        FuzzCrashWorkload(long s0, long s1) {
            this.s0 = s0;
            this.s1 = s1;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            execute("drop table if exists " + WAL_TABLE);
            // Every crash point recreates cf_wal with a new physical token. Purge the dropped token now so
            // the crash facade does not retain and re-snapshot hundreds of obsolete, preallocated column
            // files on every later syncfs (quadratic work and multi-GB heap growth in the nightly sweep).
            forceWalPurge();
            txns = generateTxns(new Rnd(s0, s1), WAL_TABLE);          // recreates cf_wal (0 rows), same txns
            walToken = engine.verifyTableName(WAL_TABLE);
            // createInitialTableWal QUEUES its unconditional "column top" ALTERs in cf_wal's WAL but does not
            // drain them, so without this drain they remain part of commit()'s crashable WAL. A crash then
            // rolls the table back to the PRE-alter base schema (a valid empty table, but one the twin never
            // snapshots — the twin drains these ALTERs before fp[0], Task 2/3), yielding a spurious
            // membershipP=-1 and a suspend on the torn ALTER segment. Draining here (before the driver's
            // markDurableBaseline, which runs after setup) makes the durable baseline the fully-materialized
            // 14-col empty state == fp[0], symmetric with the twin, so the sweep crashes ONLY the fuzz
            // workload and every recovery lands on a real fp[] snapshot.
            drainWalQueue();
            if (fp == null) {
                fp = buildTwinFingerprints(TWIN_TABLE, txns, new Rnd(s0, s1)); // once; drops the twin
            }
            expectedFullCommitSeqTxn = (int) fp.getLast().seqTxn;
            return new TableToken[]{walToken};
        }

        @Override
        public void commit() throws Exception {
            fuzzer.applyToWal(txns, WAL_TABLE, 1, new Rnd(s0, s1));
            drainWalQueue();
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            Assert.assertFalse("table left suspended after recovery at k=" + k, anyTableSuspended(walToken)); // bar 2
            String recovered = fingerprint(WAL_TABLE);
            final long recoveredSeqTxn = tableSeqTxn(WAL_TABLE);
            int p = matchingState(fp, recoveredSeqTxn, recovered);   // bar 1: causal cut + membership
            Assert.assertTrue("recovered state at k=" + k + " matches NO committed seqTxn/state (corruption?) "
                    + "[seqTxn=" + recoveredSeqTxn + "]\n" + recovered, p >= 0);
            Assert.assertTrue("recovered seqTxn exceeds int test range", recoveredSeqTxn <= Integer.MAX_VALUE);
            return (int) recoveredSeqTxn;
        }
    }

    /**
     * Force a WAL broad sweep independently of the engine clock. The W&gt;0 tests deliberately pin that clock
     * below the normal purge interval, where TestUtils.drainPurgeJob() only sweeps the group-commit queue and
     * never enters WalPurgeJob.broadSweep(). Two increasing ticks cross the cadence gate deterministically.
     */
    private void forceWalPurge() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
        engine.releaseInactiveTableSequencers();
        final long step = engine.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
        final long[] tick = {1L};
        final MicrosecondClock incClock = () -> (tick[0] += step);
        try (WalPurgeJob job = new WalPurgeJob(engine, engine.getConfiguration().getFilesFacade(), incClock)) {
            // First broad sweep removes WAL dirs and pings ApplyWal2TableJob to finish a dropped table.
            job.run();
            job.run();
            drainWalQueue();
            // The apply pass can hold the table writer while finalizing the drop. Release it, then sweep once
            // more so both the physical table tree and the facade's path-keyed snapshots are reclaimed.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            engine.releaseInactiveTableSequencers();
            job.run();
            job.run();
        }
    }

    private SweepResult runSeedSweep(long s0, long s1, int windowUs) throws Exception {
        return runSeedSweep(s0, s1, windowUs, DEFAULT_ADAPTIVE_CRASH_POINT_CAP);
    }

    private SweepResult runSeedSweep(long s0, long s1, int windowUs, int cap) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // 1h >> any test commit() duration: fresh-table lastEpochTs==0 fires exactly one deterministic
        // epoch on batch 1 and nothing can reach 1h to fire a second -- see Budget & runtime / Mechanism
        // in the plan. A small interval is wall-clock-timing-flaky against the sweep's `fired` guard.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, windowUs);
        if (windowUs == 0) {
            // Pin beyond the one-hour threshold: the first batch deterministically advances one epoch while
            // the fixed clock makes ApplyWal2TableJob's time quota deterministic across every crash point.
            // With the real clock, load jitter changed batch boundaries and therefore changed durability-op
            // identity between adjacent k values, invalidating the W=0 monotone-staircase oracle.
            setCurrentMicros(3_600_001_000L);
        }
        try {
            return forEachAdaptiveCrashPoint(new FuzzCrashWorkload(s0, s1), cap);
        } finally {
            if (windowUs == 0) {
                setCurrentMicros(-1);
            }
        }
    }

    private void assertW0MonotonePrefix(SweepResult r) {
        int[] p = r.recoveredByK();
        int prev = -1;
        for (int k = 1; k <= r.sweptPoints; k++) {
            Assert.assertTrue("staircase non-monotone at k=" + k + " (" + p[k] + " < " + prev + ")", p[k] >= prev);
            prev = p[k];
        }
    }

    // W=0 exact RPO: monotone staircase over all swept points + full recovery at the last atomic point.
    private void assertW0Bars(SweepResult r) {
        Assert.assertFalse("sweep truncated (N=" + r.n + " > cap): size counts so N <= cap, else full history "
                + "is never checked", r.truncated);
        assertW0MonotonePrefix(r);
        // r.n is the number of durability operations; the oracle returns the materialized sequencer cut.
        Assert.assertEquals("last atomic crash point must recover the full committed history",
                expectedFullCommitSeqTxn, r.recoveredByK()[r.sweptPoints]);
    }

    @Test
    public void testForceWalPurgeRemovesDroppedTableSnapshots() throws Exception {
        // Match the W>0 nightly clock: it is intentionally below the normal WAL-purge cadence gate.
        setCurrentMicros(1_000_000L);
        try {
            runWithCrashFacade(() -> {
                final int baselineTracked = crashFf.trackedFileCount();
                for (int i = 0; i < 3; i++) {
                    execute("create table cf_purge (ts timestamp, v long) timestamp(ts) partition by day wal");
                    execute("insert into cf_purge values ('2024-01-01T00:00:00.000000Z', " + i + ")");
                    drainWalQueue();
                    final TableToken token = engine.verifyTableName("cf_purge");

                    try (Path tablePath = new Path().of(engine.getConfiguration().getDbRoot()).concat(token)) {
                        Assert.assertTrue(crashFf.exists(tablePath.$()));
                        Assert.assertTrue("table creation must populate the crash content model",
                                crashFf.trackedFileCountUnder(tablePath.toString()) > 0);
                        execute("drop table cf_purge");
                        forceWalPurge();
                        Assert.assertFalse("forced purge must remove the dropped physical token", crashFf.exists(tablePath.$()));
                        Assert.assertEquals("forced purge must evict every dropped-token crash snapshot",
                                0, crashFf.trackedFileCountUnder(tablePath.toString()));
                    }
                }
                Assert.assertEquals("repeated drop/purge must keep retained crash state bounded",
                        baselineTracked, crashFf.trackedFileCount());
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testFingerprintMembershipPrimitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fp (ts timestamp, v long) timestamp(ts) partition by day wal");
            ObjList<CommittedState> history = new ObjList<>();
            history.add(new CommittedState(tableSeqTxn("fp"), fingerprint("fp"))); // empty
            for (int i = 0; i < 3; i++) {
                execute("insert into fp values (" + (i * 1_000_000L) + ", " + i + ")");
                drainWalQueue();
                history.add(new CommittedState(tableSeqTxn("fp"), fingerprint("fp"))); // states 1..3
            }
            // the current (full) state must match the last snapshot
            CommittedState full = history.getQuick(3);
            Assert.assertEquals(3, matchingState(history, full.seqTxn, fingerprint("fp")));
            // an intermediate snapshot is found only with both its seqTxn and fingerprint
            CommittedState intermediate = history.getQuick(1);
            Assert.assertEquals(1, matchingState(history, intermediate.seqTxn, intermediate.fingerprint));
            Assert.assertEquals(-1, matchingState(history, intermediate.seqTxn, "not a real dump"));

            // A later destructive transaction may recreate identical rows. Distinct seqTxns must remain
            // distinguishable instead of being misreported as non-monotone causal progress.
            int coincidentIndex = history.size();
            history.add(new CommittedState(intermediate.seqTxn + 100, intermediate.fingerprint));
            Assert.assertEquals(1, matchingState(history, intermediate.seqTxn, intermediate.fingerprint));
            Assert.assertEquals(coincidentIndex,
                    matchingState(history, intermediate.seqTxn + 100, intermediate.fingerprint));
        });
    }

    @Test
    public void testTwinFingerprintsDeterministic() throws Exception {
        assertMemoryLeak(() -> {
            final long s0 = 42L, s1 = 99L;
            ObjList<CommittedState> h1 = runTwinOnce("wal_a", "twin_a", s0, s1);
            ObjList<CommittedState> h2 = runTwinOnce("wal_b", "twin_b", s0, s1);
            Assert.assertEquals("fp history length must be deterministic", h1.size(), h2.size());
            for (int i = 0; i < h1.size(); i++) {
                Assert.assertEquals("seqTxn[" + i + "] must be identical across runs",
                        h1.getQuick(i).seqTxn, h2.getQuick(i).seqTxn);
                Assert.assertTrue("fp[" + i + "] must be identical across two runs of the same seed",
                        TestUtils.equals(h1.getQuick(i).fingerprint, h2.getQuick(i).fingerprint));
            }
            Assert.assertTrue("fp history must be non-trivial", h1.size() > 3);
        });
    }

    private ObjList<CommittedState> runTwinOnce(String walName, String twinName, long s0, long s1) throws Exception {
        Rnd genRnd = new Rnd(s0, s1);
        ObjList<FuzzTransaction> txns = generateTxns(genRnd, walName);
        return buildTwinFingerprints(twinName, txns, new Rnd(s0, s1));
    }

    // NIGHTLY-only: even the minimal (inserts + O3) profile writes the full ~14-column fuzz schema. Sweep a
    // large prefix to validate the machinery and W=0 monotonicity without duplicating the full-history bar
    // covered by testFullLibraryW0. CI validates the machinery with testConvertPartitionCrashSafeW0 instead.
    @Test
    public void testSelfCheckW0MinimalProfile() throws Exception {
        Assume.assumeTrue("minimal-profile crash sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        runWithCrashFacade(() -> {
            fuzzOverrideMinimal = true;               // inserts + O3 only, to validate the sweep machinery
            try {
                assertW0MonotonePrefix(runSeedSweep(1234L, 5678L, 0, 800));
            } finally {
                fuzzOverrideMinimal = false;
            }
        });
    }

    private int expectedFullCommitSeqTxn;

    private static final long[] FIXED_SEEDS0 = {1234L, 22L, 8080L};
    private static final long[] FIXED_SEEDS1 = {5678L, 33L, 9090L};

    // NIGHTLY-only (run with -Dquestdb.fuzz.nightly=true): the full op library currently gives N≈828
    // durability ops and assertW0Bars requires a FULL untruncated sweep (cap≥N), so this is ~85 min for one
    // seed — far past the regular CI ceiling (the @Rule Timeout above lifts to 3h under the nightly flag).
    // One representative seed; cap 900 avoids truncation. CI covers the ordering fix via the deterministic
    // testConvertPartitionCrashSafeW0.
    @Test
    public void testFullLibraryW0() throws Exception {
        Assume.assumeTrue("full-library crash sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        runWithCrashFacade(() -> assertW0Bars(runSeedSweep(FIXED_SEEDS0[0], FIXED_SEEDS1[0], 0, 900)));
    }

    // CI-fast regression guard for the applyNonStructural events-before-sequencer ordering fix. Deterministic
    // and tiny (2-column, 2-partition table + one CONVERT PARTITION), so N is small and the FULL untruncated
    // crash-point sweep runs in seconds. Before the fix a crash between the convert's sequencer msync and its
    // events.sync suspended the table (bar 2); after it, every crash point recovers the committed data cleanly.
    @Test
    public void testConvertPartitionCrashSafeW0() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        runWithCrashFacade(() -> {
            SweepResult r = forEachAdaptiveCrashPoint(new ConvertPartitionWorkload());
            Assert.assertEquals("W=0 convert atomic boundary drifted", 11, r.atomicCommitDurabilityOpCount);
            Assert.assertFalse("convert-partition sweep truncated (atomic ops > cap) — raise the cap", r.truncated);
        });
    }

    // Group-commit window for the W>0 (WN) crash tests: 50ms — matches the RPO knob a deployment trades for
    // throughput. Under W>0 a crash mid-window rolls back the un-flushed tail (RPO <= W), so the W=0
    // zero-loss tighteners (assertW0Bars) do NOT apply; the per-crash-point oracle (membership + no-suspend)
    // still must hold at EVERY point = the "clean rollback, no corruption" guarantee. The complementary
    // durable-ack RPO bar (an ACK'd txn always survives) is proven by AdaptiveGroupCommitCrashTest.
    private static final int GROUP_WINDOW_WN_US = 50_000;

    // CI-fast W>0 regression guard: the group-commit (W=50ms) counterpart of testConvertPartitionCrashSafeW0.
    // The convert is data-preserving, so whether a crash lands BEFORE the batch fdatasync (convert lost ->
    // pre-convert rows) or AFTER (convert applied), recovery must land on the SAME logical rows with no
    // corruption/suspend. Exercises the crash sweep + oracle through the DEFERRED group-commit flush path.
    //
    // Under W>0 a swept crash can fire on the deferred close-time fdatasync (flushPendingDurable inside
    // WalWriter.cleanupBeforeClose), which distresses the WAL WRITER but not the sequencer — so the sweep's
    // recoverAfterCrash reclaims the resulting owned/distressed pool orphan (releaseCrashOrphanedWalWriters,
    // else "left behind on pool shutdown") AND reboots the table sequencer (resetForReboot, else the
    // still-open sequencer's stale in-memory lastTxn wedges recovery's apply in an infinite re-notify loop).
    // The W>0 durable-ack crash GUARANTEE itself is separately covered by AdaptiveGroupCommitCrashTest.
    @Test
    public void testConvertPartitionCrashSafeWN() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, GROUP_WINDOW_WN_US);
        // Pin the microsecond clock. Under W>0 BOTH the group-commit deferral trigger
        // (WalWriter.recordPendingDurable) and the WAL-apply time quota (ApplyWal2TableJob's per-batch
        // timeLimit) read the engine's microsecond clock, so with the REAL clock a heavy-load run's wall-clock
        // jitter makes the deferred-flush crash point and recovery's apply batching non-deterministic —
        // intermittently wedging recovery's drainWalQueue. A fixed clock keeps every commit inside the window
        // (deterministic deferral, still exercising the close-time flush crash path) and the apply quota
        // effectively unbounded (single-pass apply), independent of load. This mirrors how the sibling W>0
        // oracle AdaptiveGroupCommitCrashTest drives its window off a controlled clock. Restore the real clock
        // in the finally so a fixed clock never leaks into a later test (currentMicros is a static not reset
        // by the harness between tests).
        setCurrentMicros(1_000_000L);
        try {
            runWithCrashFacade(() -> {
                SweepResult r = forEachAdaptiveCrashPoint(new ConvertPartitionWorkload());
                Assert.assertFalse("convert-partition WN sweep truncated (N > cap) — raise the cap", r.truncated);
            });
        } finally {
            setCurrentMicros(-1); // -1 => real clock (the harness default); do not leak a fixed clock
        }
    }

    // NIGHTLY-only: group-commit (W=50ms) counterpart of testFullLibraryW0 — the randomized full-op-library
    // crash-fuzz under W>0. A crash mid-window may lose the un-flushed tail (loss permitted, RPO <= W), so the
    // W=0 staircase/full-at-N bars are intentionally NOT applied. runSeedSweep asserts the per-crash-point
    // oracle at EVERY point: recovery lands on a CONSISTENT committed prefix (membership) with no
    // corruption/suspend — the broad "clean rollback under group commit" guarantee across the whole op library.
    @Test
    public void testFullLibraryWN() throws Exception {
        Assume.assumeTrue("full-library WN crash sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        // Pin the microsecond clock for load-independent W>0 determinism (see testConvertPartitionCrashSafeWN):
        // the group-commit deferral trigger and the WAL-apply time quota both read the engine clock, so the
        // real clock makes this nightly-scale sweep intermittently wedge under load. Restore in the finally.
        setCurrentMicros(1_000_000L);
        try {
            runWithCrashFacade(() -> {
                SweepResult r = runSeedSweep(FIXED_SEEDS0[0], FIXED_SEEDS1[0], GROUP_WINDOW_WN_US, 700);
                Assert.assertTrue("sweep must exercise >= 1 crash point under W>0", r.sweptPoints >= 1);
            });
        } finally {
            setCurrentMicros(-1); // -1 => real clock (the harness default); do not leak a fixed clock
        }
    }

    private static final String CVT_TABLE = "cf_cvt";

    // Deterministic non-structural-SQL crash workload: seed a 2-partition table, then CONVERT PARTITION TO
    // PARQUET — a non-structural WAL command routed through WalWriter.applyNonStructural. The convert preserves
    // the logical rows, so EVERY crash-recovered state must equal the seeded data and the table must never be
    // suspended: exactly the bar the applyNonStructural events-before-sequencer ordering fix restores.
    private final class ConvertPartitionWorkload implements AdaptiveCrashWorkload {
        private String fp;           // the single valid post-commit fingerprint (convert is data-preserving)
        private TableToken token;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            execute("drop table if exists " + CVT_TABLE);
            execute("create table " + CVT_TABLE + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into " + CVT_TABLE + " values" +
                    " ('2024-01-01T00:00:00.000000Z', 1)," +
                    " ('2024-01-01T06:00:00.000000Z', 2)," +
                    " ('2024-01-02T00:00:00.000000Z', 3)," +
                    " ('2024-01-02T06:00:00.000000Z', 4)");
            drainWalQueue();                                   // durable baseline == the only valid recovered state
            token = engine.verifyTableName(CVT_TABLE);
            if (fp == null) {
                fp = fingerprint(CVT_TABLE);
            }
            return new TableToken[]{token};
        }

        @Override
        public int atomicCommitDurabilityOpCount(int countedOps) {
            // Both W=0 and the safe W>0 fallback have N=26: ops 1-6 the WAL event msync/fdatasync pairs,
            // 7-8 the sequencer txn-log entry (msync + fdatasync) -- where the transaction becomes DURABLE --
            // 9-11 parquet production (data.parquet, _pm, the partition dir), 12-13 the apply's async
            // column-flush kicks, then 14-26 the epoch tail (syncfs, _cv, _txn, the three .epoch copies,
            // the manifest, the table-dir fsync, _snapshot).
            // Everything from op 9 on is MATERIALIZATION, re-derivable from the WAL that op 8 made durable;
            // ops 1-11 must nevertheless fail loudly, so the boundary is the last parquet-production op.
            // Op 12 starts the explicitly best-effort tail: a fault there is allowed to be consumed because
            // the conversion transaction and logical rows are already durable, and the oracle still verifies
            // no suspension or data change after reboot.
            // Drift history: N was 26, then 27 when metadata-bound epochs added a third writeEpochCopy to the
            // tail, and 26 again once commit pointers started honouring the table's EFFECTIVE mode -- under
            // ADAPTIVE the apply's _txn commit is lazy and the epoch covers it, so its msync, which used to
            // sit between parquet production and the tail, is correctly gone. The boundary moved with it
            // (12 -> 11): one fewer op before the tail, the same op SET.
            // This equality is a drift tripwire, not the load-bearing bound. The message prints the phase's
            // ops so the next drift says WHICH barrier moved instead of just a number.
            Assert.assertEquals(
                    "unexpected convert durability-op count; this phase's ops were:\n" + phaseDurabilityOps(),
                    26,
                    countedOps
            );
            return 11;
        }

        @Override
        public void commit() throws Exception {
            // Non-structural WAL command -> applyNonStructural (appendSql -> events.sync -> getSequencerTxn).
            execute("alter table " + CVT_TABLE + " convert partition to parquet" +
                    " where ts > '2023-12-31T23:00:00.000000Z' and ts < '2024-01-01T23:00:00.000000Z'");
            drainWalQueue();
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            Assert.assertFalse("table suspended after convert-partition crash at k=" + k, anyTableSuspended(token));
            String recovered = fingerprint(CVT_TABLE);
            Assert.assertTrue("convert-partition crash at k=" + k + " changed committed data:\n" + recovered,
                    TestUtils.equals(fp, recovered));
            return 0;                                          // data-preserving op: single valid snapshot
        }
    }

    // Deterministic REBASE-WAL crash workload: seed a table, then ALTER TABLE ... REBASE WAL. The rebase mints
    // a new table dir, renames it into place, drops the old table from the registry and re-points the name to
    // the new token, and only THEN calls WalWriter.commitRebaseSeed() to write two empty seed txns. So by the
    // time the seeds are written the new table is ALREADY the live table. commitRebaseSeed loops
    // appendData -> getSequencerTxn with NO events.sync between (unlike its sibling truncateSoft), so under
    // ADAPTIVE W=0 a crash between the seed's sequencer msync and its (absent) events flush leaves the live
    // rebased table with a durable sequencer pointing past a non-durable _event -> recovery SUSPENDS it.
    // Rebase is data-preserving (partitions are hard-linked into the new dir), so every crash-recovered state
    // must expose the seeded rows and never be suspended: the bar the commitRebaseSeed events-before-sequencer
    // ordering fix restores.
    @Test
    public void testRebaseWalCrashSafeW0() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true"); // SUSPEND WAL (rebase precondition) is dev-mode gated
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true"); // REBASE WAL demands this
        runWithCrashFacade(() -> {
            SweepResult r = forEachAdaptiveCrashPoint(new RebaseWalWorkload());
            Assert.assertFalse("rebase-wal sweep truncated (N > cap) — raise the cap", r.truncated);
            // A swept crash can fire INSIDE a TableWriter.close()/commit (the facade throws to simulate power
            // loss). The pooled WRITER it orphans is force-reclaimed per cycle by the base (releaseEngineHandles
            // -> WriterPool.releaseCrashOrphanedWriters), and the honest cross-table-leak guard runs per cycle
            // in RebaseWalWorkload.teardown() (assertNoForeignOwnedWriters). The same interruption can also
            // leave CACHED fds open under a cf_rbs_* dir (clone copy / interrupted close); every cf_rbs table is
            // dropped by now, so reclaim those dangling fds here — path-scoped, so it can never touch a live
            // engine-root fd (tables.d etc.) — before assertMemoryLeak's open-fd check. Process death reclaims
            // all of this on a real power loss; the live JVM cannot.
            crashFf.reclaimCachedFdsUnder(RBS_TABLE);
        });
    }

    private static final String RBS_TABLE = "cf_rbs";

    /**
     * Honest cross-table-leak guard for the rebase sweep — the meaningful half of "assert dropped-tail
     * instead of {@code busy writers == 0}", kept in the workload (the base's releaseEngineHandles() does the
     * actual reclaim via {@link WriterPool#releaseCrashOrphanedWriters}).
     * <p>
     * A swept crash can fire INSIDE a {@link TableWriter#close()}/commit (the facade throws to simulate power
     * loss), leaving the pool entry owned. {@code CairoEngine.rebaseWalTable0} is proven real-leak-free (every
     * acquire is finally/try-with guarded), so any writer this single-threaded sweep leaves owned MUST belong
     * to one of OUR {@code cf_rbs_*} tables — an owned writer on any other table would be a real leak. A crash
     * mid-close leaves either a still-checked-out writer (table token available) or a locked entry whose writer
     * {@code lock()} already nulled (fall back to the pool map key, the dir name); a {@code cf_rbs_*} table name
     * and a {@code cf_rbs_*~<id>} dir name both start with {@link #RBS_TABLE}. Checked before the base reclaims.
     */
    private void assertNoForeignOwnedWriters() {
        final long mainThread = Thread.currentThread().threadId();
        for (Map.Entry<CharSequence, WriterPool.Entry> me : engine.getWriterPoolEntries().entrySet()) {
            final WriterPool.Entry e = me.getValue();
            if (e.getOwnerThread() != mainThread) {
                continue; // not one of this single-threaded sweep's own orphans (UNALLOCATED / other)
            }
            final TableToken t = e.getTableToken();
            final CharSequence id = t != null ? t.getTableName() : me.getKey();
            Assert.assertTrue("rebase sweep left a writer owned on a non-sweep table/dir (real leak): " + id,
                    id != null && id.toString().startsWith(RBS_TABLE));
        }
    }

    private final class RebaseWalWorkload implements AdaptiveCrashWorkload {
        private String fp;           // the single valid recovered fingerprint (rebase is data-preserving)
        private String tableName;    // per-iteration table name — a fresh identity each crash point (see setup)
        private TableToken token;    // pre-rebase token; the rebase drops it, so recovery's re-publish of it is a harmless hint

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            // Each crash point gets a FRESH table name (cf_rbs_<iteration>). REBASE mints a new durable table dir
            // every run, and on a crash BEFORE the swap the old table wins so the new dir leaks as an unadopted
            // orphan; with a fixed name those orphans — and dropped-but-not-yet-purged dirs — collide with a later
            // create's reused tableId ("name is reserved"). Distinct names give each iteration its own dirs, so
            // recoveries are independent and collisions are impossible by construction.
            tableName = RBS_TABLE + '_' + iteration;
            // Model a fresh restart's name-registry reload: rebuild the in-memory maps from tables.d so a prior
            // iteration's crashed rebase leaves no stale in-memory entries behind (a real reboot does this).
            engine.getTableNameRegistry().reload();
            execute("create table " + tableName + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into " + tableName + " values" +
                    " ('2024-01-01T00:00:00.000000Z', 1)," +
                    " ('2024-01-01T06:00:00.000000Z', 2)," +
                    " ('2024-01-02T00:00:00.000000Z', 3)," +
                    " ('2024-01-02T06:00:00.000000Z', 4)");
            drainWalQueue();                                   // durable baseline == the only valid recovered state
            token = engine.verifyTableName(tableName);
            if (fp == null) {
                fp = fingerprint(tableName);                   // data-preserving + name-independent -> constant across iterations
            }
            // REBASE WAL is a recovery op — permitted only on a suspended table. Suspend it here (dev-mode
            // gated) so the swept commit can rebase. Suspend is data-preserving; the seeded rows stay the only
            // valid recovered state.
            execute("alter table " + tableName + " suspend wal");
            return new TableToken[]{token};
        }

        @Override
        public void commit() throws Exception {
            // ALTER TABLE ... REBASE WAL -> CairoEngine.rebaseWalTable -> WalWriter.commitRebaseSeed().
            execute("alter table " + tableName + " rebase wal");
            drainWalQueue();
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // The registry swap is crash-atomic (CairoEngine.rebaseWalTable0 -> swapTable -> logSwapTable), so
            // recovery never needs orphan-adoption: the live name resolves either to the old table (crash before
            // the swap) or to the new one (crash after), consistently in memory and on disk — no reload() here,
            // which would mask the atomicity by falling back to reloadFromRootDirectory. Resolve by name (the
            // rebase mints the new token in commit(), unknown at setup()) and re-publish it, since recoverAfterCrash
            // only re-published the pre-rebase token and any suspend we hunt lives on the live one.
            TableToken live = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("rebased table vanished after crash at k=" + k, live);
            // recoverAfterCrash clears the apply job's transient catch(Throwable) suspend only for the PRE-rebase
            // token; the rebase mints a NEW token (resolved above) that a crash during the seed's apply can leave
            // transiently suspended. Model the fresh restart for it too — then the re-publish + re-apply below
            // exercises the DURABLE state: were the WAL/sequencer actually torn, the re-apply re-suspends (bar 2)
            // or the data check fails, so this clears only the live-JVM artifact, never a real durability suspend.
            if (engine.getTableSequencerAPI().isSuspended(live)) {
                engine.getTableSequencerAPI().getTxnTracker(live).setUnsuspended();
            }
            engine.notifyWalTxnRepublisher(live);
            drainWalQueue();
            Assert.assertFalse("table suspended after rebase-wal crash at k=" + k, anyTableSuspended(live)); // bar 2
            String recovered = fingerprint(tableName);
            Assert.assertTrue("rebase-wal crash at k=" + k + " changed committed data:\n" + recovered,
                    TestUtils.equals(fp, recovered));
            return 0;                                          // data-preserving op: single valid snapshot
        }

        @Override
        public void teardown() throws Exception {
            // Honest cross-table-leak guard, BEFORE the base's releaseEngineHandles() reclaims this cycle's
            // close()-time writer orphan (if any): whatever is left owned must be one of OUR cf_rbs_* tables.
            assertNoForeignOwnedWriters();
            // Per-iteration hygiene: drop THIS iteration's table right after its oracle so each crash point is
            // independent (bounds registry/recovery/dir churn to one table). This does NOT reclaim a writer a
            // crash orphaned mid-close() — that entry is owned, and a plain drop cannot release it; the sweep's
            // close()-time writer orphans are force-reclaimed per cycle by the base (WriterPool
            // .releaseCrashOrphanedWriters). Unregistered crash-before-swap orphan dirs hold no writer and are
            // the accepted (dir-only) leak.
            try {
                execute("drop table if exists " + tableName);
                drainWalQueue();
                drainPurgeJob();
            } catch (Exception e) {
                LOG.info().$("[rebase-wal sweep] teardown drop skipped for ").$(tableName).$(": ").$(e.getMessage()).$();
            }
        }
    }
}
