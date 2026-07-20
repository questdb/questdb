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

import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import org.junit.Assert;

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
public abstract class AbstractAdaptiveCrashSweepTest extends AbstractCrashConsistencyTest {

    /**
     * Original crash-consistency sweep cap (2026-06-22 spec); truncation past N is logged, never silent.
     */
    protected static final int DEFAULT_ADAPTIVE_CRASH_POINT_CAP = 200;

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
         * Release/drop this iteration's state so the next iteration starts clean. Best-effort.
         */
        default void teardown() throws Exception {
        }
    }

    /**
     * The outcome of a sweep: N (count pass), the cap, the points actually swept, and per-k recovery.
     */
    protected static final class SweepResult {
        public final int cap;
        public final int n;
        public final boolean truncated;
        final int[] recoveredByK; // index 1..sweptPoints -> recovered committed-row count at that crash point
        public final int sweptPoints;

        SweepResult(int n, int cap, int sweptPoints, boolean truncated, int[] recoveredByK) {
            this.n = n;
            this.cap = cap;
            this.sweptPoints = sweptPoints;
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
     * True if ANY of the given tables is currently suspended (a swallowed WAL-apply crash).
     */
    protected boolean anyTableSuspended(TableToken... tokens) {
        for (TableToken tt : tokens) {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                return true;
            }
        }
        return false;
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
    protected SweepResult forEachAdaptiveCrashPoint(AdaptiveCrashWorkload workload, int cap) throws Exception {
        if (crashFf == null) {
            throw new IllegalStateException("call runWithCrashFacade(...) first");
        }

        // ---- 1. COUNT PASS: no fault armed; N = durability ops performed by the commit phase. ----
        workload.setup(0);
        final int opsBeforeCommit = crashFf.durabilityOpCount();
        workload.commit();
        final int n = crashFf.durabilityOpCount() - opsBeforeCommit;
        workload.teardown();
        releaseEngineHandles();
        Assert.assertTrue("workload commit phase must perform >= 1 durability op (N=" + n + ")", n > 0);

        final int sweptPoints = Math.min(n, cap);
        final boolean truncated = n > cap;
        if (truncated) {
            LOG.info().$("[forEachAdaptiveCrashPoint] TRUNCATING sweep: N=").$(n)
                    .$(" > cap=").$(cap).$(" -> sweeping crash points 1..").$(sweptPoints).I$();
        } else {
            LOG.info().$("[forEachAdaptiveCrashPoint] N=").$(n)
                    .$(" -> sweeping ALL crash points 1..").$(sweptPoints).I$();
        }

        final int[] recoveredByK = new int[sweptPoints + 1];
        // Baseline of NON-cached engine fds (name registry etc.) after a clean release: any non-cached fd
        // open beyond this at a cycle's end was left dangling by an fsync-interrupted operation and is
        // reclaimed (a real power loss's process death would close it; the live JVM cannot).
        final java.util.Set<Long> nonCacheFdBaseline = new java.util.HashSet<>(crashFf.noCacheOpenFdsSnapshot());

        // ---- 2. SWEEP: crash at each atomic-commit durability op k, recover, run the oracle for k. ----
        // A workload's commit phase may append BEST-EFFORT durability ops that run AFTER the transaction is
        // already durable (e.g. convertPartitionNativeToParquet advances the durable epoch post-commit inside
        // a log-and-swallow try/catch: the convert stays durable via the WAL, the epoch/purge just falls back
        // to async). Those ops cannot lose committed data, and — for the convert — leave a committed parquet
        // partition whose raw-fd-written _pm/data.parquet the crash model does not track. The count pass counts
        // them in N, so the sweep detects the first such op (crash fired but neither propagated nor suspended)
        // as the atomic-commit boundary and stops there.
        int atomicCommitOps = sweptPoints;
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
                // The one-shot arm was CONSUMED (the op fired) yet the crash neither propagated out of the
                // commit NOR suspended the table: we have reached a BEST-EFFORT post-commit durability op
                // (see the loop's block comment). The atomic transaction is already durable, so every
                // remaining op is best-effort cleanup that cannot lose committed data; the sweepable range
                // ends at k-1. Stop here — running the oracle for k would read the committed parquet
                // partition whose raw-fd-written _pm the crash model cannot roll back (spurious "invalid size
                // header"). The natural commit boundary, logged (never a silent cap). A genuine cross-pass
                // op-count drift is DISTINCT: its arm is never consumed (isCrashArmed() stays true), so it
                // falls through to the assertion below and fails.
                LOG.info().$("[forEachAdaptiveCrashPoint] best-effort post-commit durability op at k=").$(k)
                        .$(" (atomic commit already durable); swept atomic-commit crash points 1..").$(k - 1)
                        .$(" of N=").$(n).$(" -> stopping").I$();
                atomicCommitOps = k - 1;
                workload.teardown();
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(nonCacheFdBaseline);
                break;
            }
            Assert.assertTrue(
                    "crash point k=" + k + " never fired — the commit phase's durability-op count is not "
                            + "stable/deterministic across passes (expected op " + (base + k) + ")",
                    fired
            );

            recoverAfterCrash(tokens);

            recoveredByK[k] = workload.oracle(k, n);
            workload.teardown();
            releaseEngineHandles();
            reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        }

        return new SweepResult(n, cap, atomicCommitOps, truncated, recoveredByK);
    }

    /**
     * Power-loss rollback + production-faithful recovery on the live engine, in {@code
     * CairoEngine.completeInit()} order: release handles, {@code crash()} (roll files back to their durable
     * content), clear the transient in-memory suspend that a live-engine apply-crash left (a real power
     * loss never ran that catch), then the recovery triple —
     * {@code RecoveryCoordinator.recover()} -> {@code notifyWalTxnRepublisher(tt)} -> {@code drainWalQueue()}.
     */
    protected void recoverAfterCrash(TableToken[] tokens) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        // Empty the WAL-writer pool BEFORE crash(), while the segment files are still their intact
        // (fallocate'd) size: a pooled WalWriter's close rolls back only its UNCOMMITTED tail (no fsync, so
        // the durable content the crash preserves is untouched) and drops its stale mmap. Doing this AFTER
        // crash() instead would roll back against the already-truncated file and DESTROY the durable WAL
        // (setAppendPosition truncating committed rows) that recovery must roll forward. A freshly booted
        // engine's WAL-writer pool is empty; this models that so recovery and the workload's follow-up
        // write both open fresh, correctly-mapped writers.
        //
        // Force-reclaim any WAL writer the swept crash left checked out FIRST: under adaptive group commit
        // (W>0) the crash can fire on the deferred close-time fdatasync inside WalWriter.cleanupBeforeClose
        // (flushPendingDurable), which distresses the writer and RETHROWS, so WalWriterTenant.close() unwinds
        // with the pool slot still OWNED. releaseAllWalWriters() below cannot reclaim an owned slot (its CAS
        // from UNALLOCATED fails -> "table is left behind on pool shutdown"); this reclaims it first, the WAL
        // analogue of releaseEngineHandles()'s releaseCrashOrphanedWriters for table writers. Run BEFORE
        // crash() for the same intact-file reason as the pool release below. A no-op under W=0.
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        crashFf.crash(engine.getConfiguration().getDbRoot());

        // Model a fresh-process restart: clear the transient suspend the apply job's catch(Throwable) set
        // when it swallowed the CrashSimulationError. A freshly booted engine starts with un-suspended
        // trackers; without this, the suspended table is excluded from apply and recovery cannot re-apply.
        for (TableToken tt : tokens) {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
            }
        }

        // Model a fresh-process restart, part 2: evict the pooled TxnScoreboard. The V2 scoreboard is
        // ANONYMOUS native memory held alive by the engine's scoreboard pool, NOT a file crash() can roll
        // back, so its monotonic `max` txn high-water mark survives the simulated crash on this live engine
        // where a real power loss's process death would have discarded it (the next boot re-creates it
        // empty). This matters ONLY when recovery REWINDS the on-disk _txn below that stale pre-crash max —
        // exactly what a sustained-lazy-gap epoch rewind does: the pre-crash O3 apply pushed `max` up, then
        // RecoveryCoordinator rewinds _txn/_cv to the (lower) durable epoch cut and re-applies. A reader then
        // opening at the rewound txn can never satisfy TxnScoreboardV2.acquireTxn (updateMax fails on
        // txn < max), spinning to a spurious "Transaction read timeout" that a fresh boot would never see.
        // (No-op for the epoch-every-batch sweeps, whose _txn is never rewound below max.) Readers/writers
        // are already released above, so the pooled scoreboards are idle (refCount 0) and removed cleanly;
        // the post-recovery apply + oracle read then open a FRESH scoreboard, exactly as a booted engine does.
        for (TableToken tt : tokens) {
            engine.getTxnScoreboardPool().remove(tt);
        }

        // Model a fresh-process restart, part 3: force-close the cached table sequencer AND drop its
        // SeqTxnTracker (resetForReboot), so both reload from the durable txnlog on disk — exactly as a
        // booted engine does. Under group commit (W>0) a crash on the DEFERRED close-time fdatasync
        // (flushPendingDurable inside WalWriter.cleanupBeforeClose) distresses the WAL WRITER but NOT the
        // sequencer (unlike W=0, whose inline sequencer sync distresses+closes the sequencer), so a
        // convert/alter that was assigned a seqTxn in memory (lastTxn=N) but whose txnlog record rolled back
        // (durable high-water N-1) leaves the STILL-OPEN sequencer advertising the stale N. forAllWalTables
        // then reads that in-memory N (its open-sequencer slow path) instead of the durable txnlog, re-seeds
        // the tracker's seqTxn to N, and recovery's apply spins forever: updateWriterTxns keeps returning
        // `writerTxn(N-1) < seqTxn(N)` == true, re-notifying ApplyWal2TableJob for a txn the rolled-back WAL
        // no longer has. Closing the sequencer discards that stale high-water and forces forAllWalTables onto
        // its durable-txnlog fast path, so the fresh tracker re-inits to N-1 and the apply converges. (A
        // monotonic tracker purge alone is insufficient: the open sequencer would just re-seed N.)
        for (TableToken tt : tokens) {
            engine.getTableSequencerAPI().resetForReboot(tt);
        }

        new RecoveryCoordinator(engine).recover();
        for (TableToken tt : tokens) {
            engine.notifyWalTxnRepublisher(tt);
        }
        drainWalQueue();
    }

    /**
     * Release readers, writers, WAL writers and inactive sequencers so the next iteration starts fresh.
     */
    protected void releaseEngineHandles() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        // Force-reclaim any writer this (single-threaded) sweep left checked out or locked when a simulated
        // crash unwound a TableWriter.close()/unlock mid-operation: releaseAllWriters() above skips OWNED
        // entries (only their owner can return them), so without this they linger busy with an open .lock fd
        // and trip the enclosing assertMemoryLeak's busy-writer / open-fd checks. A real power loss's process
        // death reclaims them; this models that on the live JVM, the writer-pool analogue of the non-cache fd
        // reclaim in reclaimLingeringNonCacheFds. A no-op on a healthy engine (production never leaks a writer).
        engine.releaseCrashOrphanedWriters();
        // WAL-writer analogue of the line above: a swept crash on the deferred close-time fdatasync under
        // group commit (W>0) leaves a distressed WalWriter owned in the pool (see recoverAfterCrash); reclaim
        // it before releaseAllWalWriters() so the full release does not trip "left behind on pool shutdown".
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        engine.releaseInactiveTableSequencers();
    }

    /**
     * Reclaim any NON-cached fd left open beyond {@code baseline} — a fault-injection artifact: a simulated
     * crash unwinds an fsync mid-operation on the LIVE JVM, so an {@code openRWNoCache}/{@code openRONoCache}
     * fd whose owning operation was interrupted before its close lingers, whereas a real power loss's process
     * death has the OS reclaim every fd. Closing the per-cycle delta (after all pooled handles are released)
     * models that, keeping the sweep leak-clean over many cycles without touching the engine's own fds.
     */
    protected void reclaimLingeringNonCacheFds(java.util.Set<Long> baseline) {
        for (long fd : crashFf.noCacheOpenFdsSnapshot()) {
            if (!baseline.contains(fd)) {
                crashFf.forceClose(fd); // robust to already-reclaimed fds (see CrashFaultFilesFacade#forceClose)
            }
        }
    }
}
