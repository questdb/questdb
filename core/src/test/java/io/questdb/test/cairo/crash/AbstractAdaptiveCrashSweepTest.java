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
 * Base for the ADAPTIVE exhaustive crash-point SWEEP (SP-D increment D1.a). Provides
 * {@link #forEachAdaptiveCrashPoint} — the driver that runs a caller-supplied deterministic adaptive
 * workload and crashes at EVERY durability op of its commit phase, then recovers and hands each crash
 * point to the workload's oracle. The five curated workloads W0–W4 (D1.c) and this class's own
 * self-check ({@code AdaptiveCrashSweepSelfCheckTest}) plug into it via {@link AdaptiveCrashWorkload}.
 *
 * <p>It sits on a dedicated subclass of {@link AbstractCrashConsistencyTest} (rather than on that base
 * directly, as the spec sketch allowed) so the generic crash oracle stays uncluttered by the
 * adaptive-specific recovery/suspend machinery below; every adaptive sweep test extends THIS class and
 * so still shares the driver.
 *
 * <h3>Why the sweep needs more than "expect a CrashSimulationError"</h3>
 * A crash armed during the swept commit phase manifests in TWO ways, because {@link CrashSimulationError}
 * is an {@code Error} that unwinds to whichever layer is running the durability op:
 * <ul>
 *   <li><b>WAL-commit fsync path</b> (an {@code insert} under {@code W=0}, or an explicit epoch drive) —
 *       the Error propagates OUT of the commit call. The driver catches it.</li>
 *   <li><b>WAL-apply path</b> ({@code drainWalQueue()} — lazy apply + durable epoch) — {@code
 *       ApplyWal2TableJob}'s top-level {@code catch (Throwable)} SWALLOWS the Error into a table
 *       {@code suspendTable(...)} and returns normally. There is no exception to catch; the crash shows
 *       up as {@link io.questdb.cairo.wal.seq.TableSequencerAPI#isSuspended}.</li>
 * </ul>
 * The driver treats EITHER as "the crash fired". A workload's commit phase should stop as soon as it
 * observes a suspend (see {@link #anyTableSuspended}) so that post-crash commits do not durably extend
 * the WAL and mask the injection point — exactly as a real power loss would halt everything.
 *
 * <h3>Modelling a fresh-process restart</h3>
 * A REAL power loss kills the JVM AT the fsync; it never runs the apply job's {@code catch ->
 * suspendTable}. On a live test engine that catch DOES run, leaving a transient in-memory suspend on the
 * cached {@code SeqTxnTracker} (a suspended table is excluded from apply, so recovery could never
 * re-apply it). The driver clears that artifact before recovery, so the recovery triple re-applies
 * exactly as a freshly booted engine — whose trackers start un-suspended — would.
 *
 * <h3>Count-stable per-k isolation</h3>
 * Each sweep iteration uses a FRESH, uniquely-named table (the workload names it after the iteration id)
 * and calls {@link AbstractCrashConsistencyTest#markDurableBaseline()} after setup, so every prior
 * iteration's tables are already fully durable and inert to {@code crash()}/{@code recover()}. The crash
 * point is armed RELATIVE to the live {@code durabilityOpCount()} read right after that baseline
 * ({@code armCrashAt(base + k)}), so op {@code k} always means "the k-th durability op of THIS commit
 * phase" regardless of how many ops earlier iterations performed. Because these tests drive WAL apply
 * synchronously via {@code drainWalQueue()} (no background apply/purge threads), that per-phase op count
 * is deterministic and reproducible between the count pass and every sweep pass.
 */
public abstract class AbstractAdaptiveCrashSweepTest extends AbstractAdaptiveCrashTest {

    /**
     * Adaptive crash-point SWEEPS are long-running (each test iterates EVERY durability op of its commit
     * phase as a separate crash+recover point, ~minutes each) and are fuzz/soak workloads, not unit tests.
     * The regular pipeline now also selects {@code io.questdb.test.cairo.crash}, so gate the whole sweep
     * family behind {@code -Dquestdb.fuzz.nightly=true}: PR CI skips them via a fast assumption, while the
     * adaptive soak pipeline sets the flag and runs them.
     */
    @Before
    public void assumeNightlySweep() {
        Assume.assumeTrue(
                "adaptive crash sweeps are nightly-only; run with -Dquestdb.fuzz.nightly=true",
                Boolean.getBoolean("questdb.fuzz.nightly")
        );
    }

    /**
     * Original crash-consistency sweep cap (2026-06-22 spec); truncation past N is logged, never silent.
     */
    protected static final int DEFAULT_ADAPTIVE_CRASH_POINT_CAP = 200;

    private int phaseFirstOp = -1; // 1-based index of the count pass's first commit-phase durability op
    private int phaseOpCount;      // how many ops that phase performed (N)

    /**
     * A deterministic, replayable adaptive crash workload. The SAME iteration must reproduce the SAME
     * durability-op sequence across the count pass and every sweep pass — that determinism is what makes
     * {@code armCrashAt(k)} reproducible.
     */
    protected interface AdaptiveCrashWorkload {

        /**
         * Create fresh, uniquely-named table(s) for this iteration plus any durable baseline data, and
         * return the table token(s) recovery must re-publish ({@code notifyWalTxnRepublisher}) and the
         * oracle inspects. Called once per count/sweep iteration with a DISTINCT {@code iteration} id
         * (0 = the count pass, then 1..min(N,cap)); the name MUST embed the id so iterations do not
         * collide.
         */
        TableToken[] setup(int iteration) throws Exception;

        /**
         * Apply the deterministic commit sequence that the sweep arms a crash within (the swept phase).
         * MUST stop promptly once the crash has fired — it manifests either as a propagated
         * {@link CrashSimulationError} (which needs no handling here) OR as a table becoming suspended
         * (check {@link #anyTableSuspended} after each apply step and return) — otherwise post-crash
         * commits would durably extend the WAL and mask the injection point.
         */
        void commit() throws Exception;

        /**
         * Assert the crash-aware oracle (D1.b) for crash point {@code k} of {@code n} AFTER recovery, and
         * return the recovered committed-row count so the driver can prove the injection points are
         * distinct (non-decreasing in k, reaching the full set by k=n).
         */
        int oracle(int k, int n) throws Exception;

        /**
         * Declare how many of the clean pass's {@code countedOps} belong to the atomic durability phase.
         * The default sweeps the full clean count. An override is valid only when workload semantics prove
         * that op {@code returnValue + 1} is the first post-commit, best-effort durability operation; it must
         * never derive the boundary by observing whether an injected fault was swallowed.
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
        public final int n;
        public final boolean stoppedAtDeclaredBoundary;
        public final boolean truncated;
        final int[] recoveredByK; // index 1..sweptPoints -> recovered committed-row count at that crash point
        public final int sweptPoints;

        SweepResult(
                int n,
                int atomicCommitDurabilityOpCount,
                int cap,
                int sweptPoints,
                boolean truncated,
                int[] recoveredByK
        ) {
            this.n = n;
            this.atomicCommitDurabilityOpCount = atomicCommitDurabilityOpCount;
            this.cap = cap;
            this.sweptPoints = sweptPoints;
            this.stoppedAtDeclaredBoundary = atomicCommitDurabilityOpCount < n && !truncated;
            this.truncated = truncated;
            this.recoveredByK = recoveredByK;
        }

        /**
         * Per-k recovered committed-row count; index {@code k} in {@code 1..sweptPoints} ({@code [0]} unused).
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
     * Sweep a crash across every durability op of {@code workload}'s commit phase.
     * <ol>
     *   <li><b>Count pass:</b> run the commit phase with NO fault; {@code N} = durability ops it performed.</li>
     *   <li><b>Sweep:</b> for {@code k = 1..min(N, cap)}: fresh identical baseline, {@code armCrashAt(k)},
     *       run the commit phase, expect the crash to fire, roll files back with {@code crash()}, run the
     *       recovery triple, and invoke the workload's oracle for {@code k}.</li>
     * </ol>
     * If {@code N > cap} the truncation is LOGGED (never silent) and flagged on the result.
     */
    /**
     * The commit phase's durability ops, one line each ({@code <n> <kind> <path>}), from the count pass.
     * <p>
     * A pinned op count is a drift tripwire, and a bare "expected 27 but was 26" says nothing about whether
     * a barrier moved, was added, or was REMOVED — the last of which is a durability regression. Attach this
     * to the assertion message so the diff is readable at the point of failure. Valid only after the count
     * pass has run.
     */
    /**
     * The single durability op armed at crash point {@code k}, as {@code <n> <kind> <path>}. A failure at a
     * crash point is far easier to act on when it names the barrier it crashed at rather than just an index.
     * Valid only after the count pass has run.
     */
    protected String phaseDurabilityOp(int k) {
        if (phaseFirstOp < 1) {
            return "<count pass has not run>";
        }
        final java.util.List<String> log = crashFf.durabilityOpLog();
        final int idx = phaseFirstOp - 1 + k - 1;
        return idx >= 0 && idx < log.size() ? log.get(idx) : "<op index out of range>";
    }

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
     * COUNT PASS ONLY: run {@code workload}'s commit phase with no fault armed and return {@code N}, the
     * number of durability ops it performs — without paying for the sweep.
     * <p>
     * A randomized workload's N is not known until it has been generated and committed once, but the sweep's
     * cap has to be chosen BEFORE the sweep: pick it too low and the run dies on the truncation bar rather
     * than on a durability defect. This lets a caller size the cap to the workload it actually drew (or
     * reject a pathologically large one) instead of pinning a constant that only suits one fixed seed.
     * <p>
     * Uses the same setup/commit/teardown/reclaim sequence as the sweep's own count pass, so calling this
     * first and then {@link #forEachAdaptiveCrashPoint(AdaptiveCrashWorkload, int)} on the SAME workload
     * instance is safe — the sweep re-runs its own count pass from an equivalent baseline.
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

    protected SweepResult forEachAdaptiveCrashPoint(AdaptiveCrashWorkload workload, int cap) throws Exception {
        if (crashFf == null) {
            throw new IllegalStateException("call runWithCrashFacade(...) first");
        }

        // Baseline NON-cached engine fds before the count pass. The count workload must not normalize
        // leaked one-shot directory fds into the baseline used by every crash iteration.
        final java.util.Set<Long> nonCacheFdBaseline = new java.util.HashSet<>(crashFf.noCacheOpenFdsSnapshot());

        // ---- 1. COUNT PASS: no fault armed; N = durability ops performed by the commit phase. ----
        workload.setup(0);
        final int opsBeforeCommit = crashFf.durabilityOpCount();
        workload.commit();
        final int n = crashFf.durabilityOpCount() - opsBeforeCommit;
        // Remember the count pass's window so a workload's pinned-op-count assertion can name the ops it
        // actually saw. Recorded BEFORE teardown, whose own ops would otherwise be mistaken for the phase's.
        phaseFirstOp = opsBeforeCommit + 1; // the facade numbers ops from 1
        phaseOpCount = n;
        workload.teardown();
        releaseEngineHandles();
        reclaimLingeringNonCacheFds(nonCacheFdBaseline);
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

        final int[] recoveredByK = new int[sweptPoints + 1];
        // ---- 2. SWEEP: crash at each DECLARED atomic-commit durability op k, recover, run the oracle. ----
        for (int k = 1; k <= sweptPoints; k++) {
            final TableToken[] tokens = workload.setup(k);
            // Fresh, count-stable baseline: everything on disk NOW (prior tables + this table's pre-commit
            // state) is durable and inert to crash()/recover(); ops are armed relative to this point.
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

        // Direct epoch-metadata probes use read-only mmaps. In the test harness there is no continuously
        // scheduled AsyncMunmapJob, so drain its final queued mappings before the enclosing exact FD check.
        Files.getMmapCache().asyncMunmap();
        return new SweepResult(n, atomicCommitOps, cap, sweptPoints, truncated, recoveredByK);
    }
}
