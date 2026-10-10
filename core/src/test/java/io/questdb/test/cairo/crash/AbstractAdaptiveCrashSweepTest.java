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

import io.questdb.cairo.TableToken;
import io.questdb.std.Files;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;

/**
 * Base for the adaptive exhaustive crash-point sweep. {@link #forEachAdaptiveCrashPoint} runs a caller-supplied
 * deterministic adaptive workload, crashes at every durability op of its commit phase, recovers, and hands each
 * crash point to the workload's oracle. Workloads plug in through {@link AdaptiveCrashWorkload}.
 *
 * <h3>How a crash shows up</h3>
 * {@link CrashSimulationError} is an {@code Error} that unwinds to whichever layer runs the durability op:
 * <ul>
 *   <li><b>WAL-commit fsync path</b> (an {@code insert} under {@code W=0}, or an explicit epoch drive): the
 *       Error propagates out of the commit call, and the driver catches it.</li>
 *   <li><b>WAL-apply path</b> ({@code drainWalQueue()}: lazy apply and durable epoch): the top-level
 *       {@code catch (Throwable)} in {@code ApplyWal2TableJob} turns the Error into {@code suspendTable(...)}
 *       and returns normally. The crash shows up as
 *       {@link io.questdb.cairo.wal.seq.TableSequencerAPI#isSuspended}.</li>
 * </ul>
 * The driver treats either as "the crash fired". A workload's commit phase stops as soon as it observes a
 * suspend (see {@link #anyTableSuspended}), so post-crash commits do not durably extend the WAL and mask the
 * injection point.
 *
 * <h3>Modelling a fresh-process restart</h3>
 * A real power loss kills the JVM at the fsync and never runs the apply job's {@code catch -> suspendTable}. On
 * a live test engine that catch runs and leaves a transient suspend on the cached {@code SeqTxnTracker}, which
 * would exclude the table from apply. The driver clears it before recovery, as a freshly booted engine starts
 * with un-suspended trackers.
 *
 * <h3>Count-stable per-k isolation</h3>
 * Each iteration uses a fresh table named after the iteration id and calls
 * {@link AbstractCrashConsistencyTest#markDurableBaseline()} after setup, so earlier iterations' tables are
 * durable and inert to {@code crash()}/{@code recover()}. The crash point is armed relative to the
 * {@code durabilityOpCount()} read after that baseline ({@code armCrashAt(base + k)}), so op {@code k} is the
 * k-th durability op of this commit phase. The tests drive WAL apply synchronously via {@code drainWalQueue()},
 * so the per-phase op count is the same in the count pass and every sweep pass.
 */
public abstract class AbstractAdaptiveCrashSweepTest extends AbstractAdaptiveCrashTest {

    /**
     * Crash-point sweeps run for minutes each, so the whole family runs only with
     * {@code -Dquestdb.fuzz.nightly=true}: PR CI skips them through an assumption, and the adaptive soak pipeline
     * sets the flag.
     */
    @Before
    public void assumeNightlySweep() {
        Assume.assumeTrue(
                "adaptive crash sweeps are nightly-only; run with -Dquestdb.fuzz.nightly=true",
                Boolean.getBoolean("questdb.fuzz.nightly")
        );
    }

    /**
     * Default crash-point cap. Truncation past it is logged.
     */
    protected static final int DEFAULT_ADAPTIVE_CRASH_POINT_CAP = 200;

    private int phaseFirstOp = -1; // 1-based index of the count pass's first commit-phase durability op
    private int phaseOpCount;      // how many ops that phase performed (N)

    /**
     * A deterministic, replayable adaptive crash workload. The same iteration must reproduce the same durability-op
     * sequence across the count pass and every sweep pass, which makes {@code armCrashAt(k)} reproducible.
     */
    protected interface AdaptiveCrashWorkload {

        /**
         * Creates fresh table(s) for this iteration plus any durable baseline data, and returns the tokens recovery
         * re-publishes ({@code notifyWalTxnRepublisher}) and the oracle inspects. Called once per iteration with a
         * distinct {@code iteration} id (0 for the count pass, then 1..min(N,cap)); the table name embeds the id so
         * iterations do not collide.
         */
        TableToken[] setup(int iteration) throws Exception;

        /**
         * Applies the deterministic commit sequence the sweep crashes within. Stops promptly once the crash has fired,
         * either as a propagated {@link CrashSimulationError} (no handling needed) or as a suspended table (check
         * {@link #anyTableSuspended} after each apply step and return); otherwise post-crash commits durably extend the
         * WAL and mask the injection point.
         */
        void commit() throws Exception;

        /**
         * Asserts the crash-aware oracle for crash point {@code k} of {@code n} after recovery, and returns the
         * recovered committed-row count so the driver can check that the count is non-decreasing in k and complete at
         * k=n.
         */
        int oracle(int k, int n) throws Exception;

        /**
         * Whether a table left suspended by the count pass, which runs with no fault armed, rejects the drawn workload
         * as unable to exercise crash recovery.
         * <p>
         * Randomized workloads want this: a generator can emit a combination the product does not support (such as a
         * replace-range insert over a Parquet partition), and sweeping it would report that suspend as a recovery
         * failure at every crash point. Some curated workloads end their clean pass suspended by design, so they opt
         * out.
         */
        default boolean rejectWhenCountPassSuspends() {
            return false;
        }

        /**
         * Declare how many of the clean pass's {@code countedOps} belong to the atomic durability phase.
         * The default sweeps the full clean count. An override is valid only when the workload's semantics prove that
         * op {@code returnValue + 1} is the first post-commit, best-effort durability operation; it never derives the
         * boundary by observing whether an injected fault was swallowed.
         */
        default int atomicCommitDurabilityOpCount(int countedOps) {
            return countedOps;
        }

        /**
         * Release/drop this iteration's state so the next iteration starts clean. Best-effort.
         */
        default void teardown() throws Exception {
        }
    }

    /**
     * The outcome of a sweep: N (count pass), the cap, the points actually swept, and per-k recovery.
     */
    protected static final class SweepResult {
        public final int atomicCommitDurabilityOpCount;
        public final int cap;
        public final int firstSweptK;
        public final int lastSweptK;
        public final int n;
        public final boolean stoppedAtDeclaredBoundary;
        public final boolean truncated;
        final int[] recoveredByK; // index firstSweptK..lastSweptK -> recovered committed-row count at that crash point
        public final int sweptPoints; // the size of the whole crash-point set, min(N, cap); a part sweeps a range of it

        SweepResult(
                int n,
                int atomicCommitDurabilityOpCount,
                int cap,
                int sweptPoints,
                int firstSweptK,
                int lastSweptK,
                boolean truncated,
                int[] recoveredByK
        ) {
            this.n = n;
            this.atomicCommitDurabilityOpCount = atomicCommitDurabilityOpCount;
            this.cap = cap;
            this.sweptPoints = sweptPoints;
            this.firstSweptK = firstSweptK;
            this.lastSweptK = lastSweptK;
            this.stoppedAtDeclaredBoundary = atomicCommitDurabilityOpCount < n && !truncated;
            this.truncated = truncated;
            this.recoveredByK = recoveredByK;
        }

        /**
         * Per-k recovered committed-row count; index {@code k} in {@code firstSweptK..lastSweptK}, every other entry 0.
         */
        public int[] recoveredByK() {
            return recoveredByK;
        }
    }

    /**
     * {@link #forEachAdaptiveCrashPoint(AdaptiveCrashWorkload, int)} with the default cap (200).
     */
    protected SweepResult forEachAdaptiveCrashPoint(AdaptiveCrashWorkload workload) throws Exception {
        return forEachAdaptiveCrashPoint(workload, DEFAULT_ADAPTIVE_CRASH_POINT_CAP);
    }

    /**
     * The single durability op armed at crash point {@code k}, as {@code <n> <kind> <path>}, so a failure names the
     * barrier it crashed at. Valid only after the count pass has run.
     */
    protected String phaseDurabilityOp(int k) {
        if (phaseFirstOp < 1) {
            return "<count pass has not run>";
        }
        final java.util.List<String> log = crashFf.durabilityOpLog();
        final int idx = phaseFirstOp - 1 + k - 1;
        return idx >= 0 && idx < log.size() ? log.get(idx) : "<op index out of range>";
    }

    /**
     * The commit phase's durability ops from the count pass, one line each ({@code <n> <kind> <path>}). A pinned
     * op count fails with a bare "expected 27 but was 26"; attaching this to the assertion message shows whether a
     * barrier moved, appeared or disappeared, the last being a durability regression. Valid only after the count
     * pass has run.
     */
    protected String phaseDurabilityOps() {
        if (phaseFirstOp < 1) {
            return "<count pass has not run>";
        }
        final java.util.List<String> log = crashFf.durabilityOpLog();
        final int from = Math.min(phaseFirstOp - 1, log.size());
        final int to = Math.min(from + phaseOpCount, log.size());
        return String.join("\n", log.subList(from, to));
    }

    /**
     * Count pass only: runs {@code workload}'s commit phase with no fault armed and returns {@code N}, the number of
     * durability ops it performs.
     * <p>
     * A randomized workload's N is unknown until it has been generated and committed once, and the sweep's cap must
     * be chosen before the sweep. This lets a caller size the cap to the workload it drew, or reject a very large
     * one. It uses the same setup/commit/teardown/reclaim sequence as the sweep's own count pass, so calling it and
     * then {@link #forEachAdaptiveCrashPoint(AdaptiveCrashWorkload, int)} on the same workload instance is safe.
     */
    protected int probeCommitDurabilityOps(AdaptiveCrashWorkload workload) throws Exception {
        if (crashFf == null) {
            throw new IllegalStateException("call runWithCrashFacade(...) first");
        }
        final java.util.Set<Long> nonCacheFdBaseline = new java.util.HashSet<>(crashFf.noCacheOpenFdsSnapshot());
        workload.setup(0);
        final int opsBeforeCommit = crashFf.durabilityOpCount();
        workload.commit();
        final int n = crashFf.durabilityOpCount() - opsBeforeCommit;
        workload.teardown();
        releaseEngineHandles();
        reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        Assert.assertTrue("probe: workload commit phase must perform >= 1 durability op (N=" + n + ")", n > 0);
        return n;
    }

    /**
     * Sweep a process kill across every kill point of {@code workload}'s commit phase (see
     * {@link CrashFaultFilesFacade#armKillAt}). A kill keeps the page cache, so this sees what the
     * power-loss sweep cannot: bytes rewritten in place after their last flush.
     * <ol>
     *   <li><b>Count pass:</b> run the commit phase with no kill armed; {@code N} = kill points it passed.</li>
     *   <li><b>Sweep:</b> for {@code k = 1..N}: fresh identical setup, arm the kill just before the k-th
     *       kill point of the phase, run the whole phase (the engine is not interrupted), restore the kill
     *       image, run the recovery triple and the workload's oracle for {@code k}. Point {@code N + 1}
     *       kills after the phase has finished.</li>
     * </ol>
     * Every point is swept, without a cap: a workload that uses this keeps its commit phase small.
     */
    protected SweepResult forEachKillPoint(AdaptiveCrashWorkload workload) throws Exception {
        if (crashFf == null) {
            throw new IllegalStateException("call runWithCrashFacade(...) first");
        }
        final java.util.Set<Long> nonCacheFdBaseline = new java.util.HashSet<>(crashFf.noCacheOpenFdsSnapshot());

        workload.setup(0);
        final int pointsBeforeCommit = crashFf.killPointCount();
        workload.commit();
        final int n = crashFf.killPointCount() - pointsBeforeCommit;
        workload.teardown();
        releaseEngineHandles();
        reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        Assert.assertTrue("workload commit phase must pass >= 1 kill point (N=" + n + ")", n > 0);
        LOG.info().$("[forEachKillPoint] N=").$(n).$(" -> sweeping kill points 1..").$(n + 1).I$();

        final int[] recoveredByK = new int[n + 2];
        for (int k = 1; k <= n + 1; k++) {
            final TableToken[] tokens = workload.setup(k);
            final CharSequence dbRoot = engine.getConfiguration().getDbRoot();
            if (k <= n) {
                crashFf.armKillAt(crashFf.killPointCount() + k, dbRoot);
            }
            workload.commit();
            if (k > n) {
                crashFf.takeKillImage(dbRoot);
            }
            if (!crashFf.isKillImageTaken()) {
                crashFf.armKillAt(-1, null);
                workload.teardown();
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(nonCacheFdBaseline);
                Assert.fail("kill point k=" + k + " was never reached: the commit phase's kill-point count is not "
                        + "stable across passes (N=" + n + ")");
            }
            recoverAfterCrash(tokens);
            recoveredByK[k] = workload.oracle(k, n + 1);
            workload.teardown();
            releaseEngineHandles();
            reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        }
        Files.getMmapCache().asyncMunmap();
        return new SweepResult(n, n, n + 1, n + 1, 1, n + 1, false, recoveredByK);
    }

    /**
     * Reason the first suspended table gives, or null when none is suspended.
     */
    protected String firstSuspendReason(TableToken... tokens) {
        for (TableToken tt : tokens) {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                final io.questdb.cairo.wal.seq.SeqTxnTracker t = engine.getTableSequencerAPI().getTxnTracker(tt);
                return "[table=" + tt.getTableName() + ", errorTag=" + t.getErrorTag()
                        + ", errorMessage=" + t.getErrorMessage() + "]";
            }
        }
        return null;
    }

    /**
     * The drawn workload suspends the table with no fault injected, so it cannot test crash recovery. Thrown from
     * the count pass; a randomized caller redraws the seed, a pinned one fails.
     */
    public static final class WorkloadNotCrashTestableException extends RuntimeException {
        public WorkloadNotCrashTestableException(String reason) {
            super("workload suspends the table with NO fault injected, so it cannot exercise crash "
                    + "recovery: " + reason);
        }
    }

    /**
     * Sweeps a crash across every durability op of {@code workload}'s commit phase.
     * <ol>
     *   <li><b>Count pass:</b> run the commit phase with no fault; {@code N} = durability ops it performed.</li>
     *   <li><b>Sweep:</b> for {@code k = 1..min(N, cap)}: fresh identical baseline, {@code armCrashAt(k)},
     *       run the commit phase, expect the crash to fire, roll files back with {@code crash()}, run the
     *       recovery triple, and invoke the workload's oracle for {@code k}.</li>
     * </ol>
     * If {@code N > cap}, the truncation is logged and flagged on the result.
     */
    protected SweepResult forEachAdaptiveCrashPoint(AdaptiveCrashWorkload workload, int cap) throws Exception {
        return forEachAdaptiveCrashPoint(workload, cap, 1, Integer.MAX_VALUE);
    }

    /**
     * {@link #forEachAdaptiveCrashPoint(AdaptiveCrashWorkload, int)} restricted to crash points
     * {@code firstK..min(lastK, N, cap)}, so a long sweep can be split into test methods that each end inside the
     * test timeout. Every point starts from a fresh identical baseline, so a part sweeps exactly the points the whole
     * sweep would in that range. A range that starts past the last crash point fails, so a shrunken workload cannot
     * leave a part sweeping nothing.
     */
    protected SweepResult forEachAdaptiveCrashPoint(
            AdaptiveCrashWorkload workload,
            int cap,
            int firstK,
            int lastK
    ) throws Exception {
        if (crashFf == null) {
            throw new IllegalStateException("call runWithCrashFacade(...) first");
        }

        // Baseline non-cached engine fds before the count pass, so leaked one-shot directory fds from the count
        // workload do not enter the baseline every crash iteration uses.
        final java.util.Set<Long> nonCacheFdBaseline = new java.util.HashSet<>(crashFf.noCacheOpenFdsSnapshot());

        // 1. Count pass: no fault armed; N = durability ops performed by the commit phase.
        final TableToken[] countPassTokens = workload.setup(0);
        final int opsBeforeCommit = crashFf.durabilityOpCount();
        workload.commit();
        final int n = crashFf.durabilityOpCount() - opsBeforeCommit;
        // Record the count pass's window so a pinned-op-count assertion can name the ops it saw. Recorded before
        // teardown, whose own ops would otherwise count as the phase's.
        phaseFirstOp = opsBeforeCommit + 1; // the facade numbers ops from 1
        phaseOpCount = n;
        // The count pass ran with no fault armed. If it already suspended the table, every crash point would fail on a
        // condition unrelated to durability. A randomized generator can emit such a combination (a replace-range insert
        // over a partition an earlier txn converted to Parquet, which QuestDB does not support), so this rejects the
        // drawn seed once instead of reporting N crash-recovery failures.
        final String uncrashedSuspend = workload.rejectWhenCountPassSuspends()
                ? firstSuspendReason(countPassTokens)
                : null;
        workload.teardown();
        releaseEngineHandles();
        reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        if (uncrashedSuspend != null) {
            throw new WorkloadNotCrashTestableException(uncrashedSuspend);
        }
        Assert.assertTrue("workload commit phase must perform >= 1 durability op (N=" + n + ")", n > 0);

        final int atomicCommitOps = workload.atomicCommitDurabilityOpCount(n);
        Assert.assertTrue(
                "declared atomic durability-op count must be in [1, N] (declared=" + atomicCommitOps
                        + ", N=" + n + ")",
                atomicCommitOps >= 1 && atomicCommitOps <= n
        );

        final int sweptPoints = Math.min(atomicCommitOps, cap);
        final boolean truncated = atomicCommitOps > cap;
        if (truncated) {
            LOG.info().$("[forEachAdaptiveCrashPoint] TRUNCATING sweep at global cap: N=").$(n)
                    .$(", declared atomic ops=").$(atomicCommitOps).$(", cap=").$(cap)
                    .$(" -> sweeping crash points 1..").$(sweptPoints).I$();
        } else if (atomicCommitOps < n) {
            LOG.info().$("[forEachAdaptiveCrashPoint] workload-declared atomic boundary: N=").$(n)
                    .$(", atomic ops=").$(atomicCommitOps).$(", first excluded post-commit op=")
                    .$(atomicCommitOps + 1).$(" -> sweeping crash points 1..").$(sweptPoints).I$();
        } else {
            LOG.info().$("[forEachAdaptiveCrashPoint] N=").$(n)
                    .$(" -> sweeping ALL crash points 1..").$(sweptPoints).I$();
        }

        Assert.assertTrue(
                "crash points " + firstK + ".." + lastK + " must start inside the sweep's 1.." + sweptPoints,
                firstK >= 1 && firstK <= lastK && firstK <= sweptPoints
        );
        final int lastSweptK = Math.min(lastK, sweptPoints);
        if (firstK > 1 || lastSweptK < sweptPoints) {
            LOG.info().$("[forEachAdaptiveCrashPoint] split sweep: this part runs only crash points ").$(firstK)
                    .$("..").$(lastSweptK).$(" of 1..").$(sweptPoints).I$();
        }

        final int[] recoveredByK = new int[sweptPoints + 1];
        // 2. Sweep: crash at each declared atomic-commit durability op k, recover, run the oracle.
        for (int k = firstK; k <= lastSweptK; k++) {
            final TableToken[] tokens = workload.setup(k);
            // Fresh, count-stable baseline: everything on disk now (prior tables and this table's pre-commit state) is
            // durable and inert to crash()/recover(); ops are armed relative to this point.
            markDurableBaseline();
            final int base = crashFf.durabilityOpCount();
            crashFf.armCrashAt(base + k); // fire on the k-th durability op of THIS commit phase

            boolean fired = false;
            try {
                workload.commit();
            } catch (CrashSimulationError propagated) {
                fired = true; // WAL-commit fsync path: the Error propagated out of the commit
            }
            if (!fired) {
                // WAL-apply path: ApplyWal2TableJob swallowed the Error into a table suspend.
                fired = anyTableSuspended(tokens);
            }
            if (!fired && !crashFf.isCrashArmed()) {
                workload.teardown();
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(nonCacheFdBaseline);
                Assert.fail(
                        "undeclared swallowed durability fault at in-boundary crash point k=" + k
                                + " (declared atomic ops=" + atomicCommitOps + ", N=" + n + "): the fault "
                                + "was consumed but neither propagated nor suspended a workload table"
                );
            }
            if (!fired) {
                workload.teardown();
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(nonCacheFdBaseline);
                Assert.fail(
                        "crash point k=" + k + " never fired — the commit phase's durability-op count is not "
                                + "stable/deterministic across passes (expected op " + (base + k) + ")"
                );
            }

            recoverAfterCrash(tokens);

            recoveredByK[k] = workload.oracle(k, n);
            workload.teardown();
            releaseEngineHandles();
            reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        }

        // Direct epoch-metadata reads use read-only mmaps, and the test harness has no scheduled AsyncMunmapJob, so
        // drain its queued mappings before the enclosing exact fd check.
        Files.getMmapCache().asyncMunmap();
        return new SweepResult(n, atomicCommitOps, cap, sweptPoints, firstK, lastSweptK, truncated, recoveredByK);
    }
}
