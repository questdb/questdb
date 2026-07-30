/*+*****************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.str.Path;

/**
 * Shared ADAPTIVE crash-test machinery: the production-faithful "model a fresh-process restart" sequence
 * ({@link #recoverAfterCrash}), the handle/fd reclamation that keeps a live-JVM crash simulation leak-clean,
 * and the two probes every adaptive crash oracle needs ({@link #anyTableSuspended},
 * {@link #readOnDiskTxnSeqTxn}).
 *
 * <p>This sits BETWEEN {@link AbstractCrashConsistencyTest} (the generic crash oracle) and
 * {@link AbstractAdaptiveCrashSweepTest} (the exhaustive per-durability-op sweep driver, which is
 * nightly-only). Adaptive crash tests that assert ONE deliberately-constructed crash state rather than
 * sweeping every op extend this class directly, so they run in PR CI while still recovering exactly the way
 * the sweeps do. Before the split these helpers lived on the sweep base, which forced such tests either into
 * the nightly gate or into private, drift-prone copies of the restart sequence.
 */
public abstract class AbstractAdaptiveCrashTest extends AbstractCrashConsistencyTest {

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
     * The on-disk {@code _txn}'s committed {@code seqTxn}, read straight from the file with a private
     * {@link TxReader}: no pool, no engine state, no recovery. Returns -1 if {@code _txn} will not load.
     * <p>
     * This exists to pin the load-bearing adaptive invariant that the lazy-gap sweeps' negative controls
     * used to reach indirectly: the commit POINTER is never published ahead of the data it exposes. After a
     * crash and BEFORE any recovery, an adaptive table's {@code _txn} must sit at or below its durable epoch
     * cut. When it does not, a reader sees rows whose column data was never flushed -- the zero-filled shape
     * ({@code [0, 0, 1, 2, 3]} instead of {@code [0, 1, 2, 3, 4]}) that only recovery could then repair.
     * <p>
     * Assumes a micro-timestamp designated column, which every sweep workload here uses.
     */
    protected long readOnDiskTxnSeqTxn(TableToken token, int partitionBy) {
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
            try (TxReader reader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                reader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, partitionBy);
                return reader.unsafeLoadAll() ? reader.getSeqTxn() : -1;
            }
        }
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

        // resetForReboot() reloads the durable sequencer state, including a suspension persisted by the
        // live apply job after it caught CrashSimulationError. A real power loss terminates the process at
        // the injected durability operation, so that catch-side effect never happens. Clear it after the
        // reload as well as before it, then let recovery decide from durable table/WAL state alone.
        for (TableToken tt : tokens) {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
            }
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
}
