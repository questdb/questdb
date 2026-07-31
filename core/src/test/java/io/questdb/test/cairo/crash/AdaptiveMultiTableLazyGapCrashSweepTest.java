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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SP-D Task W3: the ADAPTIVE <b>multi-table simultaneous</b> lazy-gap crash sweep — TWO independently
 * adaptive tables crashed together and recovered in ONE {@link io.questdb.cairo.RecoveryCoordinator#recover()}
 * pass, exercising the per-table loop inside {@code RecoveryCoordinator.recoverTable()} that no single-table
 * crash test (W0/W1/W1-INV/W2) can reach.
 *
 * <h3>Why plain (non-O3, non-symbol) schema</h3>
 * W1/W1-INV already isolate the O3-merge-path variable and W2 the indexed-symbol variable, each against a
 * SINGLE table. Conflating either with the multi-table variable this task targets would make a future RED
 * result ambiguous (an O3/symbol recovery bug vs a cross-table recovery-loop bug). Both {@code t1} and
 * {@code t2} therefore use the simplest possible schema — {@code (ts timestamp, v long)} with plain
 * ascending per-hour timestamps (a pure tail append every commit, single {@code partition by day} bucket,
 * per-table independent {@code v} namespace 0..{@code ROWS-1}) — so a bug found here is unambiguously about
 * SIMULTANEOUS multi-table recovery, not about O3 or symbols.
 *
 * <h3>Why lazy-gap, not epoch-every-batch</h3>
 * Mirrors {@link AdaptiveO3LazyGapCrashSweepTest} (W1-INV) and {@link AdaptiveIndexedSymbolLazyGapCrashSweepTest}
 * (W2) structurally: EACH table independently gets a durable epoch at its own {@code seqTxn=LAZY_K} (epoch
 * ENABLED for the K-row prefix), then the epoch is disabled and the driver's swept commit phase applies
 * {@code LAZY_M} MORE rows to EACH table, round-robin interleaved ({@code t1, t2, t1, t2, ...}), LAZILY (no
 * further durable cut) — the sustained lazy gap adaptive recovery exists for, now built on BOTH tables at
 * once so a crash lands mid-flight on whichever table's insert/apply step the armed durability op happens to
 * fall in, while the OTHER table's own already-applied-but-still-lazy rows are equally exposed to the crash.
 *
 * <h3>The multi-table question this answers</h3>
 * {@code setup()} returns BOTH table tokens, so the driver's {@code recoverAfterCrash} — already a
 * first-class multi-table capability (it releases handles, evicts the pooled scoreboard PER token, then runs
 * ONE {@code RecoveryCoordinator.recover()} call that recovers ALL tables via its internal per-table loop,
 * then republishes + drains PER token) — recovers {@code t1} and {@code t2} TOGETHER. The oracle then asks,
 * at every crash point {@code k}: does the per-table loop treat both tables fairly? A bug where recovering
 * {@code t1} corrupts or skips {@code t2} (shared mutable state leaking across tokens, the loop stopping
 * after the first table, or one table's epoch-cut rewind clobbering the other's) would show up as EITHER
 * table failing its OWN identity-prefix/not-suspended oracle at some {@code k} — independently asserted and
 * independently tracked (see {@code t1RecoveredByK}/{@code t2RecoveredByK}) so a fault is pinned to a
 * specific table at a specific crash point, not just visible in a summed count that could mask one table's
 * dip behind the other's rise.
 *
 * <h3>Relationship to {@link PerTableAdaptiveIsolationCrashTest}</h3>
 * That test deliberately stayed single-table for its CRASH half because a nosync SIBLING's {@code txn_seq}/
 * columns are genuinely torn by the simulated power cut and the harness cannot cleanly close a torn nosync
 * sequencer on teardown. Both tables here are ALL-ADAPTIVE (no nosync sibling), so that specific teardown
 * hazard should not apply — but see the report for whether 2-table teardown is empirically stable across a
 * full sweep (many crash/recover/drop cycles) regardless.
 */
public class AdaptiveMultiTableLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    private static final String T1 = "mt_lazygap_1";
    private static final String T2 = "mt_lazygap_2";

    /**
     * Pre-epoch rows PER TABLE: a durable epoch is taken after these (interval=0 on the first applied batch).
     */
    private static final int LAZY_K = 4;
    /**
     * Post-epoch rows PER TABLE applied LAZILY (epoch disabled) — the sustained lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed rows PER TABLE once the whole gap is applied (each table's own independent v: 0..ROWS-1).
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * THE HEADLINE, multi-table lazy-gap sweep. EACH table builds its own durable epoch at seqTxn=LAZY_K,
     * both epochs are then disabled, and the driver sweeps a crash across EVERY durability op of the
     * round-robin-interleaved LAZY_M-per-table commit phase, running the full recovery triple (ONE {@code
     * recover()} call covering BOTH tables) and the per-table oracle at each. Asserts, at every crash point k:
     * <ol>
     *   <li>NEITHER table is suspended after recovery;</li>
     *   <li>no silent corruption in EITHER table — surviving rows ordered by {@code v} are an exact identity
     *       prefix {@code {0..m-1}} independently for {@code t1} and {@code t2} (a loud torn read tolerated;
     *       a wrong/absent value or a zero-fill is a FAILURE);</li>
     *   <li>a follow-up write+read succeeds on BOTH recovered tables;</li>
     *   <li>EACH table's recovered count is independently monotonic non-decreasing in k (not just the sum),
     *       rising from a short prefix to the full set, and equals ALL {@code ROWS} at k=N for BOTH tables.</li>
     * </ol>
     * A suspended table, a zero-fill/gap in EITHER table, or an asymmetric outcome where one table recovers
     * cleanly while the other does not, would be a real cross-table adaptive recovery bug.
     */
    @Test
    public void testMultiTableLazyGapSweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final LazyGapMultiTableWorkload workload = new LazyGapMultiTableWorkload();
            final SweepResult r = forEachAdaptiveCrashPoint(workload);

            LOG.info().$("[multi-table lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK(sum)=").$(Arrays.toString(r.recoveredByK()))
                    .$(", t1RecoveredByK=").$(workload.t1RecoveredByK)
                    .$(", t2RecoveredByK=").$(workload.t2RecoveredByK).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);
            Assert.assertEquals(
                    "per-table t1 series must have one entry per swept crash point",
                    r.sweptPoints, workload.t1RecoveredByK.size()
            );
            Assert.assertEquals(
                    "per-table t2 series must have one entry per swept crash point",
                    r.sweptPoints, workload.t2RecoveredByK.size()
            );

            // Oracle clause 4 (Bar-2 durable-survival FLOOR), on the driver-level SUM.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "summed recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }

            // KEY MULTI-TABLE ASSERTION: PER-TABLE monotonicity, independently. The SUM check above is
            // necessary but NOT sufficient — a bug where recovering t1 corrupts/skips t2 (or vice versa)
            // could still leave the SUM non-decreasing if one table's rise masks the other's dip in the
            // same crash-point transition. This is what actually proves "neither table is left behind by
            // the per-table recovery loop" independently of the other.
            for (int i = 1; i < workload.t1RecoveredByK.size(); i++) {
                Assert.assertTrue(
                        "t1 recovered count must be non-decreasing at sweep index " + (i + 1) + " ("
                                + workload.t1RecoveredByK.get(i - 1) + " -> " + workload.t1RecoveredByK.get(i) + ")",
                        workload.t1RecoveredByK.get(i) >= workload.t1RecoveredByK.get(i - 1)
                );
            }
            for (int i = 1; i < workload.t2RecoveredByK.size(); i++) {
                Assert.assertTrue(
                        "t2 recovered count must be non-decreasing at sweep index " + (i + 1) + " ("
                                + workload.t2RecoveredByK.get(i - 1) + " -> " + workload.t2RecoveredByK.get(i) + ")",
                        workload.t2RecoveredByK.get(i) >= workload.t2RecoveredByK.get(i - 1)
                );
            }

            // A genuine rise, not a degenerate all-full sweep (on the sum).
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full combined set)",
                    r.recoveredByK()[1] < ROWS * 2
            );
            // The durable epoch floor, PER TABLE, at the earliest crash point.
            Assert.assertTrue(
                    "t1 must recover at least the LAZY_K durable epoch rows at the earliest crash point (t1="
                            + workload.t1RecoveredByK.get(0) + ")",
                    workload.t1RecoveredByK.get(0) >= LAZY_K
            );
            Assert.assertTrue(
                    "t2 must recover at least the LAZY_K durable epoch rows at the earliest crash point (t2="
                            + workload.t2RecoveredByK.get(0) + ")",
                    workload.t2RecoveredByK.get(0) >= LAZY_K
            );

            // Oracle clause 3: at the LAST crash point k=N, BOTH tables independently restore ALL rows —
            // "full restore of BOTH", the headline multi-table claim.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows in t1",
                    ROWS, (int) workload.t1RecoveredByK.get(workload.t1RecoveredByK.size() - 1)
            );
            Assert.assertEquals(
                    "k=N must recover ALL committed rows in t2",
                    ROWS, (int) workload.t2RecoveredByK.get(workload.t2RecoveredByK.size() - 1)
            );
            Assert.assertEquals(
                    "k=N summed recovered count must equal both tables' full ROWS combined",
                    ROWS * 2, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * A representative rewinding crash point within the interleaved LAZY_M-per-table batch, chosen from
     * this class's own sweep staircase (see the logged {@code t1RecoveredByK}/{@code t2RecoveredByK} in
     * {@link #testMultiTableLazyGapSweepRecoversCleanlyAtEveryCrashPoint}: at N=110, k=50 lands BOTH
     * tables on a genuine, non-degenerate PARTIAL result — t1=6, t2=6, each strictly more than LAZY_K and
     * fewer than ROWS) — chosen the same way the sibling lazy-gap sweeps chose theirs ({@code
     * AdaptiveO3LazyGapCrashSweepTest.SCOREBOARD_REWIND_CRASH_K}, {@code
     * AdaptiveIndexedSymbolLazyGapCrashSweepTest.REPRESENTATIVE_REWIND_CRASH_K}).
     */
    private static final int REPRESENTATIVE_REWIND_CRASH_K = 50;

    /**
     * DATA BEFORE POINTER, for two tables crashing in the same lazy gap.
     *
     * <p>This was a negative control asserting that disabling {@code RecoveryCoordinator.recover()} could
     * not reproduce the correct result in BOTH tables. It could not, until {@code _txn}/{@code _cv}/the
     * index files stopped making their per-commit flush decision on the INSTANCE-GLOBAL {@code
     * cairo.commit.mode} ({@code != NOSYNC}) and started using each table's EFFECTIVE mode. Under ADAPTIVE
     * that eager msync published a commit pointer over column data that was still only lazily durable, and
     * recovery's epoch rewind was what repaired it. With the pointer as lazy as the data it describes the
     * torn-forward window is gone, and both arms land on the same correct result — at every crash point.
     * See {@link AdaptiveIndexedSymbolLazyGapCrashSweepTest} for the measured single-variable A/B.
     *
     * <p>What is asserted instead is the invariant that replaced it, and that fails loudly if the gate ever
     * regresses, now per table so a partial regression cannot hide behind its sibling: <b>after the crash
     * and before any recovery, neither table's {@code _txn} may claim a seqTxn beyond its durable epoch
     * cut.</b> Roll-forward's own value is proved by {@link AdaptiveRecoveryRollForwardCrashTest}.
     */
    @Test
    public void testCommitPointerNeverPublishedAheadOfDataInEitherTable() throws Exception {
        final TwoTableRows withRecovery = runLazyGapCrashScenario(REPRESENTATIVE_REWIND_CRASH_K, true);
        final TwoTableRows withoutRecovery = runLazyGapCrashScenario(REPRESENTATIVE_REWIND_CRASH_K, false);

        LOG.info().$("[multi-table lazy-gap negative control] k=").$(REPRESENTATIVE_REWIND_CRASH_K)
                .$(" withRecovery: ").$(withRecovery.toString())
                .$(" withoutRecovery: ").$(withoutRecovery.toString()).$();

        // WITH recovery (the supported/default configuration): both tables must be a fully valid, correct
        // identity prefix — a failure here would be a bug in the ENABLED-recovery path itself, not merely
        // a negative-control finding.
        assertValidIdentityPrefix("t1 withRecovery", withRecovery.t1);
        assertValidIdentityPrefix("t2 withRecovery", withRecovery.t2);
        Assert.assertTrue(
                "k=" + REPRESENTATIVE_REWIND_CRASH_K + " (recovery enabled) should land on a non-degenerate "
                        + "PARTIAL result in at least one table for this comparison to be meaningful (t1="
                        + withRecovery.t1.size() + ", t2=" + withRecovery.t2.size() + ")",
                (withRecovery.t1.size() > LAZY_K && withRecovery.t1.size() < ROWS)
                        || (withRecovery.t2.size() > LAZY_K && withRecovery.t2.size() < ROWS)
        );

        // WITHOUT recovery: disabled-recovery must not show MORE surviving rows in either table than
        // recovery-enabled at the identical crash point (a sanity bound; more rows without recovery would
        // be a different, stranger anomaly, not ordinary data loss).
        Assert.assertTrue(
                "t1: recovery-disabled must not show MORE surviving rows than recovery-enabled (withoutRecovery.t1="
                        + withoutRecovery.t1 + ", withRecovery.t1=" + withRecovery.t1 + ")",
                withoutRecovery.t1.size() <= withRecovery.t1.size()
        );
        Assert.assertTrue(
                "t2: recovery-disabled must not show MORE surviving rows than recovery-enabled (withoutRecovery.t2="
                        + withoutRecovery.t2 + ", withRecovery.t2=" + withRecovery.t2 + ")",
                withoutRecovery.t2.size() <= withRecovery.t2.size()
        );

        // THE INVARIANT (data before pointer), asserted per table and BEFORE the row checks below, so a
        // regression reports its CAUSE rather than its symptom, and in WHICH table.
        for (TwoTableRows arm : new TwoTableRows[]{withRecovery, withoutRecovery}) {
            Assert.assertTrue(
                    "t1: the crash must not leave _txn claiming past the durable epoch cut: the commit "
                            + "pointer is lazily durable like the data it exposes (preRecoverySeqTxn="
                            + arm.preRecoverySeqTxnT1 + ", durable cut=" + LAZY_K + ")",
                    arm.preRecoverySeqTxnT1 >= 0 && arm.preRecoverySeqTxnT1 <= LAZY_K
            );
            Assert.assertTrue(
                    "t2: the crash must not leave _txn claiming past the durable epoch cut: the commit "
                            + "pointer is lazily durable like the data it exposes (preRecoverySeqTxn="
                            + arm.preRecoverySeqTxnT2 + ", durable cut=" + LAZY_K + ")",
                    arm.preRecoverySeqTxnT2 >= 0 && arm.preRecoverySeqTxnT2 <= LAZY_K
            );
        }

        // And the consequence: the recovery-DISABLED arm is not merely "tolerable" here, it is correct in
        // BOTH tables. Held to the same bar.
        assertValidIdentityPrefix("t1 withoutRecovery", withoutRecovery.t1);
        assertValidIdentityPrefix("t2 withoutRecovery", withoutRecovery.t2);
    }

    private void assertValidIdentityPrefix(String label, List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            Assert.assertNotNull(label + " row " + i + " read back NULL (corruption)", rows.get(i));
            Assert.assertEquals(
                    label + " row " + i + " silently WRONG (not an identity prefix)",
                    (long) i, (long) rows.get(i)
            );
        }
    }

    /**
     * Paired t1/t2 recovered-row snapshots for the negative-control comparison.
     */
    private static final class TwoTableRows {
        final List<Long> t1;
        final List<Long> t2;
        /**
         * Each table's on-disk {@code _txn} seqTxn as the crash left it, sampled before any recovery ran.
         */
        long preRecoverySeqTxnT1 = -1;
        long preRecoverySeqTxnT2 = -1;

        TwoTableRows(List<Long> t1, List<Long> t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TwoTableRows)) {
                return false;
            }
            final TwoTableRows other = (TwoTableRows) o;
            return t1.equals(other.t1) && t2.equals(other.t2);
        }

        @Override
        public int hashCode() {
            return 31 * t1.hashCode() + t2.hashCode();
        }

        @Override
        public String toString() {
            return "t1=" + t1 + ", t2=" + t2;
        }
    }

    /**
     * Runs the LAZY_K/LAZY_M multi-table lazy-gap scenario once, standalone (not through the sweep
     * driver): each table gets its own LAZY_K-row durable-epoch prefix, both epochs are then disabled,
     * LAZY_M more rows are applied to EACH table round-robin-interleaved, a crash is armed at durability
     * op {@code k}, and the recovery triple runs with {@code RecoveryCoordinator.recover()} included or
     * skipped per {@code recoveryOn}. Returns the recovered {@code v} lists for both tables, ordered by
     * {@code v} (tolerant of a loud torn read). Leak-safe: drops both tables + reclaims per-cycle fds
     * before returning, mirroring the sweep driver.
     */
    private TwoTableRows runLazyGapCrashScenario(int k, boolean recoveryOn) throws Exception {
        final TwoTableRows[] resultBox = new TwoTableRows[1];
        final long[] preRecoverySeqTxn = {-1, -1};
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, recoveryOn ? "true" : "false");
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String t1 = "mt_lazygap_nc_1";
                final String t2 = "mt_lazygap_nc_2";
                execute("drop table if exists " + t1);
                execute("drop table if exists " + t2);
                drainWalQueue();
                execute("create table " + t1 + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                        + "with commit_mode='adaptive'");
                execute("create table " + t2 + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                        + "with commit_mode='adaptive'");
                final TableToken tt1 = engine.verifyTableName(t1);
                final TableToken tt2 = engine.verifyTableName(t2);

                for (int i = 0; i < LAZY_K; i++) {
                    insertRow(t1, i);
                }
                drainWalQueue();
                for (int i = 0; i < LAZY_K; i++) {
                    insertRow(t2, i);
                }
                drainWalQueue();

                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                crashFf.armCrashAt(base + k);
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        insertRow(t1, i);
                        drainWalQueue();
                        if (anyTableSuspended(tt1, tt2)) {
                            break;
                        }
                        insertRow(t2, i);
                        drainWalQueue();
                        if (anyTableSuspended(tt1, tt2)) {
                            break;
                        }
                    }
                } catch (CrashSimulationError propagated) {
                    // expected: the armed crash fired during the WAL-commit fsync path
                }

                // Driver-faithful recovery EXCEPT the recovery-enabled flag is the toggled variable.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());
                for (TableToken tt : new TableToken[]{tt1, tt2}) {
                    if (engine.getTableSequencerAPI().isSuspended(tt)) {
                        engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
                    }
                    engine.getTxnScoreboardPool().remove(tt); // fresh-restart model (see the driver's javadoc)
                }
                // The commit pointers as the crash left them, BEFORE anything can repair them.
                preRecoverySeqTxn[0] = readOnDiskTxnSeqTxn(tt1, PartitionBy.DAY);
                preRecoverySeqTxn[1] = readOnDiskTxnSeqTxn(tt2, PartitionBy.DAY);
                if (recoveryOn) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt1);
                engine.notifyWalTxnRepublisher(tt2);
                drainWalQueue();

                resultBox[0] = new TwoTableRows(readVOrderedByVAllowTorn(t1), readVOrderedByVAllowTorn(t2));
                resultBox[0].preRecoverySeqTxnT1 = preRecoverySeqTxn[0];
                resultBox[0].preRecoverySeqTxnT2 = preRecoverySeqTxn[1];

                // Leak-safe cleanup.
                for (TableToken tt : new TableToken[]{tt1, tt2}) {
                    if (engine.getTableSequencerAPI().isSuspended(tt)) {
                        engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
                    }
                    engine.getTxnScoreboardPool().remove(tt);
                }
                execute("drop table if exists " + t1);
                execute("drop table if exists " + t2);
                drainWalQueue();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                engine.releaseInactiveTableSequencers();
                for (long fd : crashFf.noCacheOpenFdsSnapshot()) {
                    if (!fdBaseline.contains(fd)) {
                        crashFf.forceClose(fd);
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, "true");
        }
        return resultBox[0];
    }

    /**
     * The per-table oracle body shared by both {@code t1} and {@code t2}: no silent corruption (identity
     * prefix by {@code v}), the durable-epoch floor, full restore at {@code k==n}, and a follow-up
     * write+read. Returns the recovered committed-row count for {@code table}. Parametrized by {@code label}
     * so a failure pinpoints exactly which table (t1 or t2) and which crash point k broke — the "neither
     * table left behind" diagnostic this task exists to provide.
     */
    private int oracleForTable(String label, int k, int n, String table) throws Exception {
        final List<Long> rows = readVOrderedByVAllowTorn(table);
        for (int i = 0; i < rows.size(); i++) {
            Assert.assertNotNull(label + " k=" + k + " row " + i + " read back NULL (corruption)", rows.get(i));
            Assert.assertEquals(
                    label + " k=" + k + " row " + i + " silently WRONG (not an identity prefix ordered by v) "
                            + "— a zero-fill/gap here is the suspected multi-table recovery bug",
                    (long) i, (long) rows.get(i)
            );
        }

        // Recovered committed-row count from the metadata (reliable even if a column read tore).
        final int recovered = (int) rowCount(table);
        Assert.assertTrue(
                label + " k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                rows.size() <= recovered
        );
        // The durable epoch floor: this table's own LAZY_K epoch'd rows are always durable, so recovery
        // never drops below them regardless of where in either table's lazy M-batch the crash landed.
        Assert.assertTrue(
                label + " k=" + k + ": recovery must never drop below the LAZY_K durable epoch rows (recovered="
                        + recovered + ")",
                recovered >= LAZY_K
        );

        if (k == n) {
            Assert.assertEquals(label + " k=N: recovery must restore ALL committed rows", ROWS, recovered);
            Assert.assertEquals(label + " k=N: the full identity set must read back clean", ROWS, rows.size());
        }

        // Clean reopen: a follow-up write + read must succeed on the recovered table. Use a LATER day (a
        // fresh partition, plain append) so the follow-up check is independent of the swept lazy-gap path.
        execute("insert into " + table + " values ('2024-10-09T00:00:00.000000Z', 999)");
        drainWalQueue();
        Assert.assertEquals(
                label + " k=" + k + ": follow-up insert must land on the recovered table",
                recovered + 1, rowCount(table)
        );
        return recovered;
    }

    private void withAdaptiveLazyGap(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);        // setup re-affirms/flips per phase
        // Recovery roll-forward left at its default (enabled) — we WANT it to run and prove it recovers.
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            body.run();
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private interface RunnableEx {
        void run() throws Exception;
    }

    private static void insertRow(String table, int v) throws Exception {
        execute("insert into " + table + " values ('" + String.format("2024-10-01T%02d:00:00.000000Z", v)
                + "', " + v + ")");
    }

    /**
     * count(*) — the committed row count from table metadata (reliable even if a column read would tear).
     */
    private long rowCount(String table) {
        try (RecordCursorFactory f = select("select count() from " + table)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : 0L;
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read {@code select v ... order by v}, returning the identity values gathered so far. A loud torn
     * read (CairoException/CairoError/SIGBUS-InternalError on a truncated column) is an acceptable crash
     * outcome — the (possibly partial) result read before the tear is returned rather than rethrown.
     */
    private List<Long> readVOrderedByVAllowTorn(String table) {
        final List<Long> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v from " + table + " order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // acceptable: corruption detected loudly; return the prefix read before the tear
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * W3 — the multi-table lazy-gap workload. {@code setup} builds EACH table's own durable-epoch prefix
     * (LAZY_K rows with the epoch enabled, then the epoch DISABLED for both), so the driver's swept {@code
     * commit} phase is the round-robin-interleaved LAZY_M-per-table lazily-applied rows — a sustained lazy
     * gap on BOTH tables simultaneously. Uses two reused, fixed table names (drop+recreate at the head of
     * each setup); the driver supplies the per-cycle isolation and — critically for this task — already
     * treats {@code setup}'s two returned tokens as a first-class multi-table recovery unit.
     */
    private final class LazyGapMultiTableWorkload implements AdaptiveCrashWorkload {
        private TableToken tt1;
        private TableToken tt2;
        final List<Integer> t1RecoveredByK = new ArrayList<>();
        final List<Integer> t2RecoveredByK = new ArrayList<>();

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            // Epoch ENABLED for the LAZY_K-row prefix so a durable cut is taken at seqTxn=LAZY_K for BOTH.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop table if exists " + T1);
            execute("drop table if exists " + T2);
            drainWalQueue();
            execute("create table " + T1 + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            execute("create table " + T2 + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            tt1 = engine.verifyTableName(T1);
            tt2 = engine.verifyTableName(T2);

            // LAZY_K rows on t1 -> apply -> durable epoch at seqTxn=LAZY_K. Plain ascending timestamps (pure
            // tail append, single partition) -- see the class javadoc for why this workload does NOT force O3.
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(T1, i);
            }
            drainWalQueue();
            // Same for t2, independently (its own v=0..LAZY_K-1 namespace, its own durable epoch cut).
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(T2, i);
            }
            drainWalQueue();

            // DISABLE further epochs: the driver's swept commit() phase (LAZY_M rows per table) is applied
            // LAZILY, building the sustained gap between each table's durable cut and its frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{tt1, tt2};
        }

        @Override
        public void commit() throws Exception {
            // Round-robin interleave: insert t1, drain, insert t2, drain, ... so a crash can land mid-flight
            // on EITHER table while the other's own already-applied-but-lazy rows are equally exposed.
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                insertRow(T1, i);
                drainWalQueue();
                if (anyTableSuspended(tt1, tt2)) {
                    return; // stop as a real power loss would, so we don't durably mask the injection point
                }
                insertRow(T2, i);
                drainWalQueue();
                if (anyTableSuspended(tt1, tt2)) {
                    return;
                }
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // Cheap, always-safe observation of BOTH suspend flags BEFORE any assertion — so even if t1's
            // assertion below throws, the log line has already captured whether t2 was ALSO left suspended
            // (the "did the per-table loop stop after the first table" diagnostic).
            final boolean s1 = engine.getTableSequencerAPI().isSuspended(tt1);
            final boolean s2 = engine.getTableSequencerAPI().isSuspended(tt2);
            LOG.info().$("[multi-table lazy-gap sweep] k=").$(k).$(" t1.suspended=").$(s1)
                    .$(" t2.suspended=").$(s2).$();

            Assert.assertFalse(
                    "k=" + k + ": t1 must NOT be suspended after recovery (per-table recovery loop must not "
                            + "leave it behind) — t2.suspended=" + s2,
                    s1
            );
            Assert.assertFalse(
                    "k=" + k + ": t2 must NOT be suspended after recovery (per-table recovery loop must not "
                            + "leave it behind) — t1.suspended=" + s1,
                    s2
            );

            final int r1 = oracleForTable("t1", k, n, T1);
            final int r2 = oracleForTable("t2", k, n, T2);
            t1RecoveredByK.add(r1);
            t2RecoveredByK.add(r2);
            return r1 + r2;
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("drop table if exists " + T1);
                execute("drop table if exists " + T2);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[multi-table lazy-gap sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }
    }
}
