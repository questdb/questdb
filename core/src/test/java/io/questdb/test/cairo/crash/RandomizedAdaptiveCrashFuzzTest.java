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
 * Randomized adaptive crash fuzz: {@link AbstractAdaptiveCrashSweepTest}'s per-op sweep driver over randomized
 * workloads from the fuzz harness ({@link io.questdb.test.cairo.fuzz.FuzzRunner},
 * {@link io.questdb.test.fuzz.FuzzTransaction}). The oracle is a canonical committed-state {@link #fingerprint}
 * and a seqTxn-bound membership check against the recorded committed-state history: every recovery lands on a
 * state that really existed.
 */
public class RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest {

    private final FuzzRunner fuzzer = new FuzzRunner();

    // testFullLibraryW0 sweeps the full op library and runs on the soak pipeline, with one fresh seed per run and the
    // class re-run back to back, on a longer ceiling than the 20-minute default. PR CI covers this family via
    // testConvertPartitionCrashSafeW0.
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

    // Default: the full destructive op library. The machinery self-check sets this to run a minimal insert/O3
    // profile.
    private boolean fuzzOverrideMinimal = false;

    // Inserts and column RENAME only. Isolates the structural path whose segment _meta publish must follow the file
    // rename (see testRenameColumnCrashSafeW0); the full library reaches it only for seeds that emit a rename.
    private boolean fuzzOverrideRenameOnly = false;

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

    private String suspendReason(TableToken token) {
        final SeqTxnTracker t = engine.getTableSequencerAPI().getTxnTracker(token);
        return "[errorTag=" + t.getErrorTag() + ", errorMessage=" + t.getErrorMessage()
                + ", writerTxn=" + t.getWriterTxn() + ", seqTxn=" + t.getSeqTxn() + "]";
    }

    private long tableSeqTxn(String table) {
        final TableToken token = engine.verifyTableName(table);
        return engine.getTableSequencerAPI().getTxnTracker(token).getWriterTxn();
    }

    // Uses the 22-arg overload, the only one that enables partitionToParquet (12th), partitionToNative (13th),
    // setParquetEncoding (20th) and addCoveringIndex (22nd). The 16-arg overload leaves those at 0, so Parquet and
    // covering indexes would not be exercised.
    private void configureFuzz() {
        if (fuzzOverrideMinimal) {
            //   cancel notSet null  rollbk cAdd cRem cRen cTyp  data eqTs pDrop pPq  pNat trunc tDrop ttl  repl symV qry  pEnc tFmt cIdx
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0, 0, 0, 0, 0, 0.6, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        } else if (fuzzOverrideRenameOnly) {
            //   cancel notSet null  rollbk cAdd cRem cRen cTyp  data eqTs pDrop pPq  pNat trunc tDrop ttl  repl symV qry  pEnc tFmt cIdx
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0, 0.1, 0, 0.15, 0, 0.6, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        } else {
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0,       // cancelRows, notSet, nullSet, rollback(=0: clean seqTxn map)
                    0.1, 0.05, 0.05, 0.05,      // colAdd, colRemove, colRename, colTypeChange
                    0.5, 0.0, 0.05, 0.03,       // dataAdd, equalTsRows(=0: canonical dump), partitionDrop, partitionToParquet
                    0.03, 0.05, 0.0, 0.05,      // partitionToNative, truncate, tableDrop(=0), setTtl
                    // replaceInsert(dedup) = 0: incompatible with partitionToParquet below. QuestDB does not support
                    // replace-range commits over Parquet partitions, so once a partition is converted, a later replace
                    // over it cannot be applied: in-order WAL replay hits "commit replace mode is not supported for
                    // Parquet partitions" and suspends the table. An uncrashed run usually hides this, because
                    // ApplyWal2TableJob skips a replace that a later txn supersedes. After a crash the sequencer log is
                    // shorter, nothing supersedes it, the replace is applied and the table suspends, which looks like a
                    // durability defect. Re-enable once the generator refuses to emit a replace range overlapping a
                    // partition it converted.
                    0.0, 0.0, 0.0, 0.02,        // replaceInsert(dedup), symbolAccessValidation, query, setParquetEncoding
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
        // createInitialTableWal queues its unconditional "column top" ALTERs via WAL but never drains them, so an
        // undrained fp[0] would show the pre-alter (fewer-column) schema, a state no crash recovery can land on: those
        // structural commits are durable (markDurableBaseline runs after setup()) before the swept transaction's crash
        // point. Drain once here so fp[0] is the fully materialized schema with 0 data rows, the committed state a
        // crash during the first transaction's own commit recovers to.
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
        public boolean rejectWhenCountPassSuspends() {
            return true;   // randomized: a suspend with no fault injected means the SEED is unusable
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            execute("drop table if exists " + WAL_TABLE);
            // Every crash point recreates cf_wal with a new physical token. Purge the dropped token now, so the crash
            // facade does not keep and re-snapshot hundreds of obsolete, preallocated column files on every later
            // syncfs (quadratic work and multi-GB heap growth in the nightly sweep).
            forceWalPurge();
            txns = generateTxns(new Rnd(s0, s1), WAL_TABLE);          // recreates cf_wal (0 rows), same txns
            walToken = engine.verifyTableName(WAL_TABLE);
            // createInitialTableWal queues its unconditional "column top" ALTERs in cf_wal's WAL without draining them,
            // so without this drain they stay part of commit()'s crashable WAL. A crash then rolls the table back to
            // the pre-alter base schema, an empty table the twin never snapshots (the twin drains these ALTERs before
            // fp[0]), giving a spurious membershipP=-1 and a suspend on the torn ALTER segment. Draining before the
            // driver's markDurableBaseline makes the durable baseline the fully materialized empty state, equal to
            // fp[0], so the sweep crashes only the fuzz workload and every recovery lands on a real fp[] snapshot.
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
            // Bar 2: name the barrier and the sequencer's own reason; a crash point index alone says nothing about
            // which op failed to roll forward or why.
            Assert.assertFalse("table left suspended after recovery at k=" + k
                    + " [op=" + phaseDurabilityOp(k) + "] " + suspendReason(walToken), anyTableSuspended(walToken));
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
     * Forces a WAL broad sweep independently of the engine clock. The W&gt;0 tests pin that clock below the normal
     * purge interval, where TestUtils.drainPurgeJob() only sweeps the group-commit queue and never enters
     * WalPurgeJob.broadSweep(). Two increasing ticks cross the cadence gate deterministically.
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

    /**
     * Sweeps one seed, sizing the cap to the workload drawn.
     * <p>
     * assertW0Bars requires an untruncated sweep, so a fixed cap turns "this seed generated more work than the
     * last one" into a failure that looks like a durability defect. Count N first, then sweep at
     * {@code max(capFloor, N)}. Returns {@code null} if N exceeds {@code hardLimit}, and the caller redraws.
     */
    private SweepResult runSeedSweepAutoCap(long s0, long s1, int windowUs, int capFloor, int hardLimit) throws Exception {
        applySweepConfig(windowUs);
        try {
            final FuzzCrashWorkload workload = new FuzzCrashWorkload(s0, s1);
            final int n = probeCommitDurabilityOps(workload);
            if (n > hardLimit) {
                LOG.info().$("[seed] REJECTED seed ").$(s0).$("L, ").$(s1)
                        .$("L: N=").$(n).$(" > hard limit ").$(hardLimit).$(" — redrawing").I$();
                System.out.printf("[seed] REJECTED %dL, %dL: N=%d > hardLimit=%d — redrawing%n", s0, s1, n, hardLimit);
                return null;
            }
            final int cap = Math.max(capFloor, n);
            LOG.info().$("[seed] sweeping seed ").$(s0).$("L, ").$(s1).$("L: N=").$(n).$(", cap=").$(cap).I$();
            return forEachAdaptiveCrashPoint(workload, cap);
        } finally {
            if (windowUs == 0) {
                setCurrentMicros(-1);
            }
        }
    }

    private SweepResult runSeedSweep(long s0, long s1, int windowUs, int cap) throws Exception {
        applySweepConfig(windowUs);
        try {
            return forEachAdaptiveCrashPoint(new FuzzCrashWorkload(s0, s1), cap);
        } finally {
            if (windowUs == 0) {
                setCurrentMicros(-1);
            }
        }
    }

    private void applySweepConfig(int windowUs) {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // One hour is far beyond any test commit() duration: on a fresh table lastEpochTs==0 fires exactly one
        // deterministic epoch on batch 1, and nothing reaches an hour to fire a second. A small interval would make the
        // sweep's `fired` guard depend on wall-clock timing.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, windowUs);
        if (windowUs == 0) {
            // Pin beyond the one-hour threshold: the first batch advances one epoch, and the fixed clock makes
            // ApplyWal2TableJob's time quota the same at every crash point. With the real clock, load jitter moves
            // batch boundaries and with them the durability-op identity between adjacent k values, which breaks the
            // W=0 monotone-staircase oracle.
            setCurrentMicros(3_600_001_000L);
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
        // Match the W>0 nightly clock, which sits below the normal WAL-purge cadence gate.
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
            // The current (full) state matches the last snapshot.
            CommittedState full = history.getQuick(3);
            Assert.assertEquals(3, matchingState(history, full.seqTxn, fingerprint("fp")));
            // An intermediate snapshot is found only with both its seqTxn and fingerprint.
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

    /**
     * The in-segment ALTER COLUMN TYPE path creates a converted column's files before it publishes the segment
     * {@code _meta} that names them.
     * <p>
     * Converting fixed-size to var-size makes apply demand an aux (.i) vector that only {@code openColumnFiles}
     * creates. Publishing first opens a window where durable segment metadata describes a VARCHAR column whose
     * {@code .i} file does not exist; a crash inside it (the {@code _meta.swp} barrier) makes the segment
     * unappliable ("WAL segment column too short for committed row range [... actual=-1]", actual=-1 being a
     * missing file) and suspends the table while the sequencer runs ahead.
     * <p>
     * This seed reproduces it at the durability op {@code msync <table>/_meta.swp}. It is a pinned seed rather than a
     * unit test because the branch needs {@code !rollSegmentOnNextRow && segmentRowCount == 0}, a WalWriter state a
     * direct unit test does not reach (the switch is deferred to the next row write). It fails if
     * {@code WalWriter.changeColumnType} publishes the segment {@code _meta} before it creates the files.
     */
    @Test
    public void testChangeColumnTypeSegmentPublishOrderCrashSafeW0() throws Exception {
        Assume.assumeTrue("crash sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        runWithCrashFacade(() -> runSeedSweep(-6969352248487674537L, -4282943605332762306L, 0, 600));
    }

    // Nightly only: even the minimal (inserts + O3) profile writes the full ~14-column fuzz schema. Sweeps a large
    // prefix to validate the machinery and W=0 monotonicity, without repeating testFullLibraryW0's full-history
    // bar. CI validates the machinery with testConvertPartitionCrashSafeW0 instead.
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

    // One fresh seed per run. A fixed pair replays one workload forever, so the sweep stops being a fuzzer, and
    // one 20-txn workload cannot emit all 13 enabled op types.
    //
    // Coverage accumulates across runs: the soak pipeline re-runs this class back to back, so a new workload per
    // invocation is what makes continuous fuzzing worth it. Several seeds inside one invocation would only make each
    // run longer and each failure slower to surface.
    //
    // PR CI and local runs keep the fixed pair, so a failure there is reproducible from the PR alone. Replay a soak
    // failure with -Dquestdb.fuzz.seed=<s0>,<s1>; TestUtils.generateRandom prints the pair, it is echoed per run
    // below, and the failure message names it.
    private static final String SEED_PROP = "questdb.fuzz.seed";
    // A drawn workload's N varies, and assertW0Bars needs cap >= N. The floor keeps every seed sweeping at least
    // this many points; the hard limit rejects an outlier that would stretch one soak iteration far past the others
    // (a redraw costs one count pass).
    private static final int SWEEP_CAP_FLOOR = 900;
    private static final int SWEEP_CAP_HARD_LIMIT = 2000;
    private static final int SEED_DRAW_ATTEMPTS = 8;

    /**
     * The seed for this run: the replay override if set, else the fixed pair when the soak flag is off, else a
     * freshly drawn pair.
     */
    private long[] drawSeed(Rnd redraw) {
        final String replay = System.getProperty(SEED_PROP);
        if (replay != null) {
            final int comma = replay.indexOf(',');
            Assert.assertTrue("-D" + SEED_PROP + " must be '<s0>,<s1>' (was '" + replay + "')", comma > 0);
            return new long[]{
                    Long.parseLong(replay.substring(0, comma).trim()),
                    Long.parseLong(replay.substring(comma + 1).trim())
            };
        }
        if (redraw == null) {
            return new long[]{FIXED_SEEDS0[0], FIXED_SEEDS1[0]};
        }
        return new long[]{redraw.nextLong(), redraw.nextLong()};
    }

    /**
     * Runs {@code body} on one seed, redrawing if the drawn workload exceeds the hard limit, and naming the seed on
     * failure, since a soak failure whose workload cannot be reproduced is of little use.
     */
    private void withSweepSeed(SeedSweep body) throws Exception {
        final boolean randomized = System.getProperty(SEED_PROP) == null && Boolean.getBoolean(NIGHTLY_PROP);
        final Rnd redraw = randomized ? TestUtils.generateRandom(LOG) : null;
        long[] seed = drawSeed(redraw);
        SweepResult r = null;
        for (int attempt = 0; attempt < SEED_DRAW_ATTEMPTS && r == null; attempt++) {
            if (attempt > 0) {
                Assert.assertNotNull("pinned seed exceeded the N<=" + SWEEP_CAP_HARD_LIMIT + " hard limit and "
                        + "cannot be redrawn — raise the limit or shrink the workload", redraw);
                seed = drawSeed(redraw);
            }
            try {
                r = body.run(seed[0], seed[1]);
            } catch (WorkloadNotCrashTestableException notTestable) {
                // The drawn workload suspends with no fault injected, a property of the generator (such as a
                // replace-range insert over a Parquet partition, which is unsupported), not a durability defect.
                // Redraw rather than sweep it: every crash point would fail on that condition. Logged, so a generator
                // that starts producing these constantly is visible.
                System.out.printf("[seed] REJECTED %dL, %dL: %s%n", seed[0], seed[1], notTestable.getMessage());
                Assert.assertNotNull("a PINNED seed produced a workload that cannot be crash-tested; fix the "
                                + "seed or the fuzz probabilities rather than redrawing: " + notTestable.getMessage(),
                        redraw);
                r = null;
                continue;
            } catch (AssertionError e) {
                throw new AssertionError("seed " + seed[0] + "L, " + seed[1] + "L (replay with -D" + SEED_PROP
                        + "=" + seed[0] + "," + seed[1] + "): " + e.getMessage(), e);
            }
        }
        Assert.assertNotNull("no seed under the N<=" + SWEEP_CAP_HARD_LIMIT + " hard limit after "
                + SEED_DRAW_ATTEMPTS + " draws — the op library got much heavier; re-check the limit", r);
        System.out.printf("[seed] PASSED with seeds %dL, %dL (N=%d, swept=%d)%n",
                seed[0], seed[1], r.n, r.sweptPoints);
    }

    @FunctionalInterface
    private interface SeedSweep {
        SweepResult run(long s0, long s1) throws Exception;
    }

    // Nightly only (-Dquestdb.fuzz.nightly=true). assertW0Bars requires a full untruncated sweep (cap >= N), so
    // the cap is sized to the seed drawn rather than pinned to one workload's N. One fresh seed per run; PR CI
    // keeps the fixed pair (see drawSeed).
    @Test
    public void testFullLibraryW0() throws Exception {
        // Redundant with AbstractAdaptiveCrashSweepTest#assumeNightlySweep, which gates every method in this family;
        // it keeps this sweep nightly-only if the class-level gate is relaxed to run the cheap sweeps more often.
        Assume.assumeTrue("full-library crash sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        runWithCrashFacade(() -> withSweepSeed((s0, s1) -> {
            SweepResult r = runSeedSweepAutoCap(s0, s1, 0, SWEEP_CAP_FLOOR, SWEEP_CAP_HARD_LIMIT);
            if (r != null) {
                assertW0Bars(r);
            }
            return r;
        }));
    }

    /**
     * Data before pointer for a WAL segment column RENAME.
     * <p>
     * {@code metadata.switchTo} publishes a segment {@code _meta} that names the column by its new name, and apply
     * resolves that name to a file. Publishing before {@code renameColumnFiles} opens a window where the durable
     * metadata names {@code new_col_N.d} while the file on disk is still under its old name; a crash on the
     * {@code _meta.swp} barrier makes the segment unappliable and recovery suspends the table with
     * {@code "WAL segment column too short for committed row range [... actual=-1]"}, actual=-1 being a missing
     * file.
     * <p>
     * The sweep's oracle is the bar: at every crash point the table comes back unsuspended (bar 2) on a committed
     * state that really existed (bar 1). The add+rename profile concentrates the sweep on this path instead of
     * depending on a seed that emits a rename.
     * <p>
     * This is coverage, not a regression pin: it also passes on this seed when the publish precedes the rename. The
     * known reproducer is the full-library seed {@code -Dquestdb.fuzz.seed=-5599363307717330847,-4230914800796809183},
     * which also hits a separate failure (the renamed column's contents are not durable when the pointer commits:
     * {@code required=128, actual=0}), so it cannot serve as a control.
     */
    @Test
    public void testRenameColumnCrashSafeW0() throws Exception {
        runWithCrashFacade(() -> {
            fuzzOverrideRenameOnly = true;
            try {
                SweepResult r = runSeedSweep(1234L, 5678L, 0, 1400);
                Assert.assertFalse("rename sweep truncated (N > cap) — raise the cap", r.truncated);
                Assert.assertTrue("sweep must exercise crash points", r.sweptPoints > 1);
            } finally {
                fuzzOverrideRenameOnly = false;
            }
        });
    }

    // CI-fast guard for the events-before-sequencer order in applyNonStructural. Deterministic and tiny (2-column,
    // 2-partition table and one CONVERT PARTITION), so N is small and the full untruncated sweep runs in seconds.
    // A crash between the convert's sequencer msync and its events.sync must not suspend the table (bar 2); every
    // crash point recovers the committed data.
    @Test
    public void testConvertPartitionCrashSafeW0() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        runWithCrashFacade(() -> {
            SweepResult r = forEachAdaptiveCrashPoint(new ConvertPartitionWorkload());
            Assert.assertTrue("W=0 convert sweep must stop before the best-effort tail", r.stoppedAtDeclaredBoundary);
            Assert.assertFalse("convert-partition sweep truncated (atomic ops > cap) — raise the cap", r.truncated);
        });
    }

    // Group-commit window for the W>0 (WN) crash tests: 50ms, a typical RPO setting. Under W>0 a crash mid-window
    // rolls back the unflushed tail (RPO <= W), so the W=0 zero-loss bars (assertW0Bars) do not apply; the
    // per-crash-point oracle (membership, no suspend) still holds at every point: clean rollback, no corruption. The
    // durable-ack bar (an acknowledged txn always survives) is covered by DurableFrontierTest.
    private static final int GROUP_WINDOW_WN_US = 50_000;

    // CI-fast W>0 guard: the group-commit (W=50ms) counterpart of testConvertPartitionCrashSafeW0. The convert
    // preserves data, so whether a crash lands before the batch fdatasync (convert lost, pre-convert rows) or after
    // it (convert applied), recovery lands on the same logical rows with no corruption or suspend. Exercises the
    // sweep and oracle through the deferred group-commit flush path.
    //
    // Under W>0 a swept crash can fire on the deferred close-time fdatasync (flushPendingDurable inside
    // WalWriter.cleanupBeforeClose), which distresses the WAL writer but not the sequencer, so the sweep's
    // recoverAfterCrash reclaims the distressed pool orphan (releaseCrashOrphanedWalWriters, else "left behind on
    // pool shutdown") and reboots the table sequencer (resetForReboot, else the open sequencer's stale in-memory
    // lastTxn makes recovery's apply re-notify forever).
    @Test
    public void testConvertPartitionCrashSafeWN() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, GROUP_WINDOW_WN_US);
        // Pin the microsecond clock. Under W>0 both the group-commit deferral trigger
        // (WalWriter.recordPendingDurable) and the WAL-apply time quota (ApplyWal2TableJob's per-batch timeLimit)
        // read the engine clock, so with the real clock load jitter makes the deferred-flush crash point and
        // recovery's apply batching non-deterministic and can wedge recovery's drainWalQueue. A fixed clock keeps
        // every commit inside the window (deterministic deferral, still exercising the close-time flush crash path)
        // and the apply quota effectively unbounded (single-pass apply). DurableFrontierTest drives its window off a
        // controlled clock the same way. The finally restores the real clock, since currentMicros is a static the
        // harness does not reset between tests.
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

    // Nightly only: the group-commit (W=50ms) counterpart of testFullLibraryW0, the randomized full-op-library
    // crash fuzz under W>0. A crash mid-window may lose the unflushed tail (RPO <= W), so the W=0 staircase and
    // full-at-N bars do not apply. runSeedSweep asserts the per-crash-point oracle at every point: recovery lands
    // on a consistent committed prefix (membership) with no corruption or suspend.
    @Test
    public void testFullLibraryWN() throws Exception {
        Assume.assumeTrue("full-library WN crash sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        // Pin the microsecond clock for load-independent W>0 determinism (see testConvertPartitionCrashSafeWN): the
        // group-commit deferral trigger and the WAL-apply time quota both read the engine clock, so the real clock can
        // wedge this sweep under load. Restored in the finally.
        setCurrentMicros(1_000_000L);
        try {
            // Fresh seeds per nightly run, as in W0. Truncation is tolerated here (the W>0 oracle is per crash
            // point, not a full-history bar), so the cap stays a fixed 700, a heavier seed sweeps its first 700
            // points, and the driver logs the truncation.
            runWithCrashFacade(() -> withSweepSeed((s0, s1) -> {
                SweepResult r = runSeedSweep(s0, s1, GROUP_WINDOW_WN_US, 700);
                Assert.assertTrue("sweep must exercise >= 1 crash point under W>0", r.sweptPoints >= 1);
                return r;
            }));
        } finally {
            setCurrentMicros(-1); // -1 => real clock (the harness default); do not leak a fixed clock
        }
    }

    private static final String CVT_TABLE = "cf_cvt";

    // Deterministic non-structural SQL crash workload: seed a 2-partition table, then CONVERT PARTITION TO PARQUET,
    // a non-structural WAL command routed through WalWriter.applyNonStructural. The convert preserves the logical
    // rows, so every crash-recovered state equals the seeded data and the table is never suspended; the
    // events-before-sequencer order in applyNonStructural guarantees this.
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
            // The phase runs the WAL event barriers (_event, _event.i), the sequencer txn-log entry (where the
            // transaction becomes durable), then Parquet production (data.parquet, _pm, the new partition
            // directory), then the apply's async column-flush kicks and the epoch tail (syncfs, _cv, _txn, the
            // save point file twice, the table directory).
            // Everything after the txn-log entry is materialization, re-derivable from the WAL; Parquet production
            // still fails visibly, so the boundary is its last op, the partition directory fsync. A fault after it may
            // be consumed, because the conversion transaction and logical rows are already durable, and the oracle
            // still checks for no suspension or data change after reboot. The boundary is found by operation kind, so a
            // barrier added to or removed from either side does not move it; the assertion fails, printing the phase's
            // ops, only if the order it relies on changes.
            final String ops = phaseDurabilityOps();
            final String[] lines = ops.split("\n");
            int lastWalOp = -1;
            int txnLogOp = -1;
            int parquetDataOp = -1;
            int parquetMetaOp = -1;
            int partitionDirOp = -1;
            for (int i = 0; i < lines.length; i++) {
                // <global op number> <kind> <db-root-relative path>
                final String[] op = lines[i].split(" ");
                final String path = op[2];
                if (path.contains("/wal") || path.contains("/txn_seq/")) {
                    lastWalOp = i + 1;
                    if (path.endsWith("/_txnlog")) {
                        txnLogOp = i + 1;
                    }
                } else if (path.endsWith("/data.parquet")) {
                    parquetDataOp = i + 1;
                } else if (path.endsWith("/_pm")) {
                    parquetMetaOp = i + 1;
                } else if (partitionDirOp < 0 && parquetMetaOp > 0 && op[1].endsWith("-dir") && path.contains("/2024-01-01.")) {
                    partitionDirOp = i + 1;
                }
            }
            Assert.assertEquals(ops, countedOps, lines.length);
            Assert.assertTrue(
                    "convert durability ops out of the expected order; this phase's ops were:\n" + ops,
                    txnLogOp > 0 && txnLogOp == lastWalOp && txnLogOp < parquetDataOp
                            && parquetDataOp < parquetMetaOp && parquetMetaOp < partitionDirOp
            );
            return partitionDirOp;
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

    // Deterministic REBASE WAL crash workload: seed a table, then ALTER TABLE ... REBASE WAL. The rebase mints a
    // new table dir, renames it into place, drops the old table from the registry, re-points the name to the new
    // token, and only then calls WalWriter.commitRebaseSeed() to write two empty seed txns, so the new table is
    // already the live table when the seeds are written. commitRebaseSeed loops appendData -> getSequencerTxn;
    // unlike its sibling truncateSoft it must sync events before the sequencer, or under adaptive W=0 a crash
    // between the seed's sequencer msync and its events flush leaves the live table with a durable sequencer
    // pointing past a non-durable _event, and recovery suspends it. Rebase preserves data (partitions are
    // hard-linked into the new dir), so every crash-recovered state exposes the seeded rows and is never suspended.
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
            // A swept crash can fire inside a TableWriter.close()/commit (the facade throws to simulate power loss).
            // The base force-reclaims the pooled writer it orphans per cycle (releaseEngineHandles ->
            // WriterPool.releaseCrashOrphanedWriters), and RebaseWalWorkload.teardown() runs the cross-table leak guard
            // per cycle (assertNoForeignOwnedWriters). The same interruption can leave cached fds open under a cf_rbs_*
            // dir (clone copy, interrupted close); every cf_rbs table is dropped by now, so reclaim those fds here,
            // path-scoped so it never touches a live engine-root fd (tables.d and others), before assertMemoryLeak's
            // open-fd check. Process death reclaims all of this on a real power loss; the live JVM cannot.
            crashFf.reclaimCachedFdsUnder(RBS_TABLE);
        });
    }

    private static final String RBS_TABLE = "cf_rbs";

    /**
     * Cross-table leak guard for the rebase sweep, kept in the workload; the base's releaseEngineHandles() does the
     * reclaim via {@link WriterPool#releaseCrashOrphanedWriters}.
     * <p>
     * A swept crash can fire inside a {@link TableWriter#close()}/commit (the facade throws to simulate power loss),
     * leaving the pool entry owned. Every acquire in {@code CairoEngine.rebaseWalTable0} is finally/try-with
     * guarded, so any writer this single-threaded sweep leaves owned belongs to one of its {@code cf_rbs_*} tables;
     * an owned writer on any other table is a real leak. A crash mid-close leaves either a checked-out writer (table
     * token available) or a locked entry whose writer {@code lock()} already nulled (fall back to the pool map key,
     * the dir name); a {@code cf_rbs_*} table name and a {@code cf_rbs_*~<id>} dir name both start with
     * {@link #RBS_TABLE}. Checked before the base reclaims.
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
            // Each crash point gets a fresh table name (cf_rbs_<iteration>). REBASE mints a new durable table dir every
            // run, and on a crash before the swap the old table wins and the new dir stays as an unadopted orphan; with
            // a fixed name those orphans, and dropped but not yet purged dirs, collide with a later create's reused
            // tableId ("name is reserved"). Distinct names give each iteration its own dirs, so recoveries are
            // independent.
            tableName = RBS_TABLE + '_' + iteration;
            // Model a fresh restart's name-registry reload: rebuild the in-memory maps from tables.d, so an earlier
            // iteration's crashed rebase leaves no stale in-memory entries behind.
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
            // REBASE WAL is a recovery op, permitted only on a suspended table. Suspend it here (dev-mode gated) so the
            // swept commit can rebase. Suspend preserves data; the seeded rows stay the only valid recovered state.
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
            // The registry swap is crash-atomic (CairoEngine.rebaseWalTable0 -> swapTable -> logSwapTable), so recovery
            // never needs orphan adoption: the live name resolves to the old table (crash before the swap) or the new
            // one (crash after), consistently in memory and on disk. No reload() here, which would mask the atomicity
            // by falling back to reloadFromRootDirectory. Resolve by name (the rebase mints the new token in commit(),
            // unknown at setup()) and re-publish it, since recoverAfterCrash re-published only the pre-rebase token.
            TableToken live = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("rebased table vanished after crash at k=" + k, live);
            // recoverAfterCrash clears the apply job's transient suspend only for the pre-rebase token; the rebase
            // mints a new token (resolved above) that a crash during the seed's apply can leave transiently suspended.
            // Model the fresh restart for it too. The re-publish and re-apply below then exercise the durable state: a
            // torn WAL or sequencer re-suspends (bar 2) or fails the data check, so this clears only the live-JVM
            // artifact.
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
            // Cross-table leak guard, before the base's releaseEngineHandles() reclaims this cycle's close()-time
            // writer orphan: whatever is left owned must be one of this sweep's cf_rbs_* tables.
            assertNoForeignOwnedWriters();
            // Drop this iteration's table right after its oracle so each crash point is independent. A plain drop
            // cannot release a writer a crash orphaned mid-close(); the base force-reclaims those per cycle
            // (WriterPool.releaseCrashOrphanedWriters). Unregistered crash-before-swap orphan dirs hold no writer and
            // are an accepted directory-only leak.
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
