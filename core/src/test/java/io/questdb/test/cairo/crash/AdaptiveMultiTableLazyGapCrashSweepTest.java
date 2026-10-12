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
 * Adaptive lazy-gap crash sweep over two tables that crash together and recover in one
 * {@link io.questdb.cairo.RecoveryCoordinator#recover()} pass. It exercises the per-table loop inside
 * {@code RecoveryCoordinator.recoverTable()}, which a single-table crash test cannot reach.
 * <p>
 * Both tables use the plain schema {@code (ts timestamp, v long)} with ascending hourly timestamps in one
 * {@code day} partition, so a failure here points at multi-table recovery rather than at O3 or symbol
 * handling. Each table takes a durable epoch at {@code seqTxn=LAZY_K}; epochs are then disabled and the
 * swept commit phase applies {@code LAZY_M} more rows to each table, interleaved t1, t2, t1, t2, ..., with
 * no further durable cut. A crash lands on one table's insert or apply step while the other table's lazy
 * rows are exposed as well.
 * <p>
 * The driver recovers both tokens that {@code setup()} returns with one {@code recover()} call. The oracle
 * checks each table on its own at every crash point and tracks each table's count separately
 * ({@code t1RecoveredByK}, {@code t2RecoveredByK}), so a recovery loop that skips a table, stops after the
 * first one, or lets one table's epoch rewind clobber the other fails on a named table at a named crash
 * point instead of hiding in a summed count.
 */
public class AdaptiveMultiTableLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    private static final String T1 = "mt_lazygap_1";
    private static final String T2 = "mt_lazygap_2";

    /**
     * Pre-epoch rows per table: a durable epoch is taken after these (interval=0 on the first applied batch).
     */
    private static final int LAZY_K = 4;
    /**
     * Post-epoch rows per table, applied lazily (epoch disabled): the lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed rows per table once the whole gap is applied (each table's own v: 0..ROWS-1).
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * The full sweep (N=100 crash points) needs more than the 20-minute test timeout, so it runs in three parts of
     * about 35 points each; see {@link #assertSweepPartRecoversCleanly}. If N grows well past 100, the last part
     * needs a further split.
     */
    @Test
    public void testMultiTableLazyGapSweepRecoversCleanlyAtCrashPoints1To35() throws Exception {
        assertSweepPartRecoversCleanly(1, 35);
    }

    @Test
    public void testMultiTableLazyGapSweepRecoversCleanlyAtCrashPoints36To70() throws Exception {
        assertSweepPartRecoversCleanly(36, 70);
    }

    @Test
    public void testMultiTableLazyGapSweepRecoversCleanlyAtCrashPoints71ToLast() throws Exception {
        assertSweepPartRecoversCleanly(71, Integer.MAX_VALUE);
    }

    /**
     * The rewinding crash point, pinned by phase rather than by an op count: this many lazy rows are applied
     * to each table, interleaved t1 then t2, and the crash fires on the first durability op of t1's next lazy
     * row commit, before that row's WAL is durable. Recovery rewinds both tables to their LAZY_K
     * durable-epoch cut and replays the applied rows, so both land on a partial result ({@code LAZY_K} plus
     * this many rows each) whatever the number of ops per commit.
     */
    private static final int REWIND_APPLIED_LAZY_ROWS = 2;

    /**
     * Data before pointer, for two tables crashing in the same lazy gap. The {@code _txn}, {@code _cv} and
     * index files take their per-commit flush decision from each table's effective commit mode, so under
     * ADAPTIVE the commit pointer is as lazily durable as the column data it describes.
     *
     * <p>The test asserts, per table so that a partial regression cannot hide behind its sibling: <b>after
     * the crash and before any recovery, neither table's {@code _txn} may claim a seqTxn beyond its durable
     * epoch cut.</b> As a consequence, the run without {@code RecoveryCoordinator.recover()} lands on the same
     * correct result as the run with it. Roll-forward itself is covered by
     * {@link AdaptiveRecoveryRollForwardCrashTest}.
     */
    @Test
    public void testCommitPointerNeverPublishedAheadOfDataInEitherTable() throws Exception {
        final TwoTableRows withRecovery = runLazyGapCrashScenario(REWIND_APPLIED_LAZY_ROWS, true);
        final TwoTableRows withoutRecovery = runLazyGapCrashScenario(REWIND_APPLIED_LAZY_ROWS, false);

        LOG.info().$("[multi-table lazy-gap negative control] appliedLazyRows=").$(REWIND_APPLIED_LAZY_ROWS)
                .$(" withRecovery: ").$(withRecovery.toString())
                .$(" withoutRecovery: ").$(withoutRecovery.toString()).$();

        // With recovery (the default configuration): both tables must be a valid identity prefix; a failure
        // here is a bug in the recovery path itself.
        assertValidIdentityPrefix("t1 withRecovery", withRecovery.t1);
        assertValidIdentityPrefix("t2 withRecovery", withRecovery.t2);
        Assert.assertTrue(
                "the crash (recovery enabled) should land on a non-degenerate "
                        + "PARTIAL result in at least one table for this comparison to be meaningful (t1="
                        + withRecovery.t1.size() + ", t2=" + withRecovery.t2.size() + ")",
                (withRecovery.t1.size() > LAZY_K && withRecovery.t1.size() < ROWS)
                        || (withRecovery.t2.size() > LAZY_K && withRecovery.t2.size() < ROWS)
        );
        Assert.assertEquals(
                "t1: the crash must land after " + REWIND_APPLIED_LAZY_ROWS + " lazy applies and before t1's "
                        + "next lazy row's WAL is durable (withRecovery.t1=" + withRecovery.t1 + ")",
                LAZY_K + REWIND_APPLIED_LAZY_ROWS, withRecovery.t1.size()
        );
        Assert.assertEquals(
                "t2: the crash must land after " + REWIND_APPLIED_LAZY_ROWS + " lazy applies to t2 "
                        + "(withRecovery.t2=" + withRecovery.t2 + ")",
                LAZY_K + REWIND_APPLIED_LAZY_ROWS, withRecovery.t2.size()
        );

        // Without recovery: neither table may show more surviving rows than with recovery at the same crash
        // point (a sanity bound; more rows without recovery would be a different anomaly, not ordinary data
        // loss).
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

        // The invariant (data before pointer), asserted per table and before the row checks below, so a
        // regression reports its cause rather than its symptom, and names the table.
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

        // The consequence: the run without recovery is correct in both tables too, held to the same bar.
        assertValidIdentityPrefix("t1 withoutRecovery", withoutRecovery.t1);
        assertValidIdentityPrefix("t2 withoutRecovery", withoutRecovery.t2);
    }

    /**
     * Each table builds its own durable epoch at seqTxn=LAZY_K, both epochs are then disabled, and the
     * driver sweeps a crash across durability ops {@code firstK..min(lastK, N)} of the interleaved
     * LAZY_M-per-table commit phase, running the full recovery triple (one {@code recover()} call for both
     * tables) and the per-table oracle at each. The three parts together sweep every point 1..N in order.
     * Asserts, at every crash point k of the part:
     * <ol>
     *   <li>neither table is suspended after recovery;</li>
     *   <li>no silent corruption in either table: surviving rows ordered by {@code v} form an identity
     *       prefix {@code {0..m-1}} for {@code t1} and {@code t2} independently (a loud torn read is
     *       tolerated; a wrong or absent value or a zero-fill fails);</li>
     *   <li>a follow-up write and read succeed on both recovered tables;</li>
     *   <li>each table's recovered count is non-decreasing in k within the part on its own, not only in the
     *       sum, and is at least LAZY_K at the part's first point; at k=1 the sum is short of the full set,
     *       and at k=N both tables recover all {@code ROWS}.</li>
     * </ol>
     */
    private void assertSweepPartRecoversCleanly(int firstK, int lastK) throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final LazyGapMultiTableWorkload workload = new LazyGapMultiTableWorkload();
            final SweepResult r = forEachAdaptiveCrashPoint(workload, DEFAULT_ADAPTIVE_CRASH_POINT_CAP, firstK, lastK);
            final int first = r.firstSweptK;
            final int last = r.lastSweptK;

            LOG.info().$("[multi-table lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", part=").$(first).$("..").$(last)
                    .$(", recoveredByK(sum)=").$(Arrays.toString(r.recoveredByK()))
                    .$(", t1RecoveredByK=").$(workload.t1RecoveredByK)
                    .$(", t2RecoveredByK=").$(workload.t2RecoveredByK).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);
            Assert.assertEquals(
                    "per-table t1 series must have one entry per swept crash point",
                    last - first + 1, workload.t1RecoveredByK.size()
            );
            Assert.assertEquals(
                    "per-table t2 series must have one entry per swept crash point",
                    last - first + 1, workload.t2RecoveredByK.size()
            );

            // Oracle clause 4 (durable survival floor), on the driver-level sum.
            for (int k = first + 1; k <= last; k++) {
                Assert.assertTrue(
                        "summed recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }

            // Per-table monotonicity. The sum check above is necessary but not sufficient: if recovering t1
            // corrupts or skips t2 (or the reverse), one table's rise can mask the other's dip in the same
            // crash-point transition. This check proves the per-table recovery loop leaves neither table
            // behind.
            for (int i = 1; i < workload.t1RecoveredByK.size(); i++) {
                Assert.assertTrue(
                        "t1 recovered count must be non-decreasing at k=" + (first + i) + " ("
                                + workload.t1RecoveredByK.get(i - 1) + " -> " + workload.t1RecoveredByK.get(i) + ")",
                        workload.t1RecoveredByK.get(i) >= workload.t1RecoveredByK.get(i - 1)
                );
            }
            for (int i = 1; i < workload.t2RecoveredByK.size(); i++) {
                Assert.assertTrue(
                        "t2 recovered count must be non-decreasing at k=" + (first + i) + " ("
                                + workload.t2RecoveredByK.get(i - 1) + " -> " + workload.t2RecoveredByK.get(i) + ")",
                        workload.t2RecoveredByK.get(i) >= workload.t2RecoveredByK.get(i - 1)
                );
            }

            // The durable epoch floor, per table, at the part's first crash point.
            Assert.assertTrue(
                    "t1 must recover at least the LAZY_K durable epoch rows at k=" + first + " (t1="
                            + workload.t1RecoveredByK.get(0) + ")",
                    workload.t1RecoveredByK.get(0) >= LAZY_K
            );
            Assert.assertTrue(
                    "t2 must recover at least the LAZY_K durable epoch rows at k=" + first + " (t2="
                            + workload.t2RecoveredByK.get(0) + ")",
                    workload.t2RecoveredByK.get(0) >= LAZY_K
            );

            if (first == 1) {
                // A genuine rise, not a degenerate all-full sweep (on the sum).
                Assert.assertTrue(
                        "sweep must show a genuine rise (earliest crash point < full combined set)",
                        r.recoveredByK()[1] < ROWS * 2
                );
            }

            if (last == r.sweptPoints) {
                // Oracle clause 3: at the last crash point k=N, both tables restore all rows.
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
            }
        }));
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
     * Paired t1/t2 recovered-row snapshots for the with/without recovery comparison.
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
            return o instanceof TwoTableRows other && t1.equals(other.t1) && t2.equals(other.t2);
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
     * LAZY_M more rows are applied to each table round-robin-interleaved, a crash fires on the first
     * durability op of t1's commit of lazy row {@code appliedLazyRows} (counted from 0), after that many
     * lazy applies to each table, and the recovery triple runs with {@code RecoveryCoordinator.recover()} included or
     * skipped per {@code recoveryOn}. Returns the recovered {@code v} lists for both tables, ordered by
     * {@code v} (tolerant of a loud torn read). Leak-safe: drops both tables + reclaims per-cycle fds
     * before returning, mirroring the sweep driver.
     */
    private TwoTableRows runLazyGapCrashScenario(int appliedLazyRows, boolean recoveryOn) throws Exception {
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
                execute("create table " + t1 + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("create table " + t2 + " (ts timestamp, v long) timestamp(ts) partition by day wal");
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
                int crashOp = -1;
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        if (i == LAZY_K + appliedLazyRows) {
                            // the first durability op of t1's commit of this row
                            crashOp = crashFf.durabilityOpCount() + 1;
                            crashFf.armCrashAt(crashOp);
                        }
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
                Assert.assertTrue(
                        "the lazy batch must reach row " + (LAZY_K + appliedLazyRows) + " to arm the crash",
                        crashOp > 0
                );
                Assert.assertFalse("the armed crash must fire", crashFf.isCrashArmed());
                final String crashedOp = crashFf.durabilityOpLog().get(crashOp - 1);
                LOG.info().$("[multi-table lazy-gap negative control] recovery=").$(recoveryOn)
                        .$(" crash at k=").$(crashOp - base).$(" of the lazy batch: ").$(crashedOp).$();

                // The driver's recovery steps, except that the recovery-enabled flag is the toggled variable.
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
                // The commit pointers as the crash left them, before anything can repair them.
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
     * write+read. Returns the recovered committed-row count for {@code table}. {@code label} names the
     * table in failure messages, so a failure pinpoints the table and the crash point k.
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

        // Clean reopen: a follow-up write + read must succeed on the recovered table. Use a later day (a
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
        // Recovery roll-forward stays at its default (enabled), so the sweep proves that it recovers.
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
     * count(*): the committed row count from table metadata (reliable even if a column read would tear).
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
     * outcome: the (possibly partial) result read before the tear is returned rather than rethrown.
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
     * The multi-table lazy-gap workload. {@code setup} builds each table's durable-epoch prefix (LAZY_K rows
     * with the epoch enabled, then the epoch disabled for both), so the driver's swept {@code commit} phase
     * applies the interleaved LAZY_M rows per table lazily: a sustained lazy gap on both tables at once. It
     * reuses two fixed table names (drop and recreate at the head of each setup); the driver supplies the
     * per-cycle isolation and recovers the two tokens {@code setup} returns as one unit.
     */
    private final class LazyGapMultiTableWorkload implements AdaptiveCrashWorkload {
        private TableToken tt1;
        private TableToken tt2;
        final List<Integer> t1RecoveredByK = new ArrayList<>();
        final List<Integer> t2RecoveredByK = new ArrayList<>();

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            // Epoch enabled for the LAZY_K-row prefix, so a durable cut is taken at seqTxn=LAZY_K for both.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop table if exists " + T1);
            execute("drop table if exists " + T2);
            drainWalQueue();
            execute("create table " + T1 + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("create table " + T2 + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            tt1 = engine.verifyTableName(T1);
            tt2 = engine.verifyTableName(T2);

            // LAZY_K rows on t1 -> apply -> durable epoch at seqTxn=LAZY_K. Plain ascending timestamps (pure
            // tail append, single partition); the class javadoc says why this workload does not force O3.
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(T1, i);
            }
            drainWalQueue();
            // Same for t2, independently (its own v=0..LAZY_K-1 namespace, its own durable epoch cut).
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(T2, i);
            }
            drainWalQueue();

            // Disable further epochs: the driver's swept commit() phase (LAZY_M rows per table) is applied
            // lazily, building the sustained gap between each table's durable cut and its frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{tt1, tt2};
        }

        @Override
        public void commit() throws Exception {
            // Round-robin interleave: insert t1, drain, insert t2, drain, ... so a crash can land mid-flight
            // on either table while the other's already-applied lazy rows are equally exposed.
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                insertRow(T1, i);
                drainWalQueue();
                if (anyTableSuspended(tt1, tt2)) {
                    return; // stop as a real power loss would, so the injection point is not durably masked
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
            // Read both suspend flags before any assertion, so that if t1's assertion below throws, the log
            // line still shows whether t2 is also left suspended (the per-table loop stopping after the
            // first table).
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
