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
 * Shared adaptive crash-test machinery: the fresh-process restart sequence ({@link #recoverAfterCrash}), the
 * handle and fd reclamation that keeps a live-JVM crash simulation leak-clean, and two helpers every adaptive
 * crash oracle needs ({@link #anyTableSuspended}, {@link #readOnDiskTxnSeqTxn}).
 * <p>
 * It sits between {@link AbstractCrashConsistencyTest}, the generic crash oracle, and
 * {@link AbstractAdaptiveCrashSweepTest}, the nightly per-durability-op sweep driver. A test that asserts one
 * constructed crash state extends this class directly, so it runs in PR CI and recovers the same way the sweeps do.
 */
public abstract class AbstractAdaptiveCrashTest extends AbstractCrashConsistencyTest {

    /**
     * True if any of the given tables is suspended (a swallowed WAL-apply crash).
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
     * After a crash and before any recovery, an adaptive table's {@code _txn} must sit at or below its durable epoch
     * cut: the commit pointer is never published ahead of the data it exposes. Otherwise a reader sees rows whose
     * column data was never flushed, the zero-filled shape ({@code [0, 0, 1, 2, 3]} instead of
     * {@code [0, 1, 2, 3, 4]}) that only recovery can repair.
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
     * Reclaims any non-cached fd left open beyond {@code baseline}. A simulated crash unwinds an fsync mid-operation
     * on the live JVM, so an {@code openRWNoCache}/{@code openRONoCache} fd whose operation was interrupted before
     * its close lingers; a real power loss has the OS reclaim every fd. Closing the per-cycle delta, after all pooled
     * handles are released, models that and keeps the sweep leak-clean without touching the engine's own fds.
     */
    protected void reclaimLingeringNonCacheFds(java.util.Set<Long> baseline) {
        for (long fd : crashFf.noCacheOpenFdsSnapshot()) {
            if (!baseline.contains(fd)) {
                crashFf.forceClose(fd); // robust to already-reclaimed fds (see CrashFaultFilesFacade#forceClose)
            }
        }
    }

    /**
     * Power-loss rollback and recovery on the live engine, in {@code CairoEngine.completeInit()} order: release
     * handles, {@code crash()} (roll files back to their durable content), clear the transient in-memory suspend that
     * a live-engine apply crash left (a real power loss never runs that catch), then
     * {@code RecoveryCoordinator.recover()} -> {@code notifyWalTxnRepublisher(tt)} -> {@code drainWalQueue()}.
     */
    protected void recoverAfterCrash(TableToken[] tokens) {
        simulateRestart(tokens);
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
        // Force-reclaim any writer that a simulated crash left checked out or locked by unwinding
        // TableWriter.close()/unlock. releaseAllWriters() skips owned entries, so without this they linger with an
        // open .lock fd and trip assertMemoryLeak's busy-writer and open-fd checks. A real power loss reclaims them.
        // No-op on a healthy engine.
        engine.releaseCrashOrphanedWriters();
        // The WAL-writer analogue: a crash on the deferred close-time fdatasync under group commit (W>0) leaves a
        // distressed WalWriter owned in the pool. Reclaim it so releaseAllWalWriters() does not trip "left behind on
        // pool shutdown".
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        engine.releaseAllViewWalWriters();
        engine.releaseInactiveTableSequencers();
        // Table writers are released last. The WAL-writer and sequencer releases above re-enter the engine (draining
        // WAL work opens a TableWriter), so a writer released earlier can exist again here. The caller's next step,
        // reclaimLingeringNonCacheFds, force-closes every non-baseline no-cache fd, and a live pooled writer's symbol
        // and index fds are no-cache too: reclaiming them makes the next iteration's close fail with "Invalid fd, not
        // found in cache" (TableWriter.doClose -> freeSymbolMapWriters).
        //
        // No unit test covers this ordering: it needs the sweep's multi-iteration baseline. The fuzz seeds
        // 1858466998207609950,7098192113821025591 and -5599363307717330847,-4230914800796809183 fail without it.
        engine.releaseAllWriters();
        engine.releaseCrashOrphanedWriters();
    }

    /**
     * The first half of {@link #recoverAfterCrash}: everything a power loss and a fresh process do before
     * {@code RecoveryCoordinator.recover()} runs.
     */
    protected void simulateRestart(TableToken[] tokens) {
        // The power loss is now, or earlier when an armed crash fired: the releases below flush (a writer's close-time
        // save point, a WalWriter's pending batch), and a real power loss runs none of that.
        crashFf.powerLoss();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        // Empty the WAL-writer pool before crash(), while the segment files still have their fallocated size: a pooled
        // WalWriter's close rolls back only its uncommitted tail and drops its mmap. After crash() the same close would
        // roll back against the truncated file and destroy durable WAL rows that recovery must roll forward. A freshly
        // booted engine's WAL-writer pool is empty.
        //
        // Reclaim crash-orphaned WAL writers first: under group commit (W>0) the crash can fire on the deferred
        // close-time fdatasync in WalWriter.cleanupBeforeClose, which distresses the writer and rethrows, leaving its
        // pool slot owned. releaseAllWalWriters() cannot reclaim an owned slot. No-op under W=0.
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        // Views own a separate WAL-writer pool. Its mappings must also close before durable files are restored.
        engine.releaseAllViewWalWriters();
        crashFf.crash(engine.getConfiguration().getDbRoot());

        // A fresh process starts with un-suspended trackers: clear the suspend the apply job's catch set when it
        // swallowed the CrashSimulationError, or recovery excludes the table from apply.
        for (TableToken tt : tokens) {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
            }
        }

        // Evict the pooled TxnScoreboard. The V2 scoreboard is anonymous native memory, not a file crash() rolls back,
        // so its monotonic max txn survives the simulated crash where a fresh process would start empty. That matters
        // when recovery rewinds _txn below the stale max (a sustained-lazy-gap epoch rewind):
        // TxnScoreboardV2.acquireTxn then fails for txn < max and a reader spins to a spurious "Transaction read
        // timeout". Readers and writers are released above, so the pooled scoreboards are idle and removed cleanly.
        for (TableToken tt : tokens) {
            engine.getTxnScoreboardPool().remove(tt);
        }

        // Close the cached sequencer and reset its SeqTxnTracker (resetForReboot) so both reload from the durable
        // txnlog, as on boot. Under group commit (W>0) a crash on the deferred close-time fdatasync distresses the WAL
        // writer but not the sequencer, so a convert or alter that got seqTxn N in memory while its txnlog record
        // rolled back to N-1 leaves the open sequencer advertising N. forAllWalTables would re-seed the tracker to N
        // and recovery's apply would wait forever for a txn the WAL no longer has. Closing the sequencer makes
        // forAllWalTables read the durable txnlog, so the tracker restarts at N-1.
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
    }
}
