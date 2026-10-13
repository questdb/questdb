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

package io.questdb.cairo.wal.seq;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

public class SeqTxnTracker {
    public static final long UNINITIALIZED_TXN = -1;
    private static final long SEQ_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "seqTxn");
    private static final long SUSPENDED_STATE_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "suspendedState");
    private static final long WAITER_REGISTRATION_COUNT_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "waiterRegistrationCount");
    private static final long WRITER_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "writerTxn");
    private final FiberWalWaitQueue fiberWaiters = new FiberWalWaitQueue();
    private final Metrics metrics;
    private final TableWriterPressureControlImpl pressureControl;
    // The table's committed _txn txn, published by the table writer each time it is released; -1 until then.
    // A commit that moves the txn without a WAL transaction (a partition removed or converted by the writer
    // itself) leaves it above the newest save point's pin, which WalPurgeJob reads for the idle save point.
    private volatile long committedTxn = -1;
    // The seqTxn of the table's newest durable save point. 0 means none yet, which keeps all WAL of a fresh
    // adaptive table; it advances as save points publish. WalPurgeJob reads it as the purge floor under
    // adaptive, and in any mode while the table holds a save point pin.
    private volatile long durableEpochSeqTxn = 0;
    // Counts the times RecoveryCoordinator restored this table from its save point since the process started.
    // wal_tables() exposes it, so an operator can see that this table needed recovery.
    private volatile long recoveryIncarnation = 0;
    // Required logical completion survives ordinary publication and writer eviction. Only coverage plus
    // finalization clears it; recovery reconstructs it before the first writer opens.
    private volatile boolean durableEpochCompletionRequired;
    // Set when the inline save point after a change that no WAL replay can redo fails or is skipped: the next
    // apply batch or clean close takes one regardless of the cadence. Cleared when a save point publishes.
    // Written only by the thread that holds the table writer.
    private volatile boolean durableEpochOwed;
    // Set by WalPurgeJob when the table's older save point is behind its current state and no save point has
    // been taken for one interval; the next apply run that applies nothing takes one more save point.
    private volatile boolean idleEpochRequested;
    // Wall-clock ms of WalPurgeJob's last idle save point request that no save point has followed yet, 0 if none.
    // It asks again only one interval later, so a request that keeps failing, or finds the writer held, is tried
    // once per interval, not at every purge pass. A published save point clears it.
    private volatile long idleEpochRequestTs;
    // Wall-clock ms of the table's last save point, for the adaptive cadence gate; 0 means none yet, so the
    // first apply batch under adaptive is eligible. Only the apply worker that holds the table writer writes it,
    // so it needs no CAS; WalPurgeJob reads it for the idle save point request.
    private volatile long lastEpochTs = 0;
    // Rows applied to this table since its last save point, for the adaptive backlog gate.
    // ApplyWal2TableJob.advance() resets it when a save point publishes. Same single-thread access as lastEpochTs.
    private long rowsSinceEpoch = 0;
    // The pin of the other save point generation, held in the slot pinnedEpochTxn does not use, or -1 if none.
    // Recovery falls back to that generation when the current one is damaged, so its partition versions stay
    // until the next save point overwrites it.
    private volatile long otherPinnedEpochTxn = -1;
    // The generation (0 or 1) that holds the current save point, the one the table stands on: the generation
    // recovery selected, or the one this table's writer last published; -1 while no save point is pinned. The
    // next save point is written into the other generation. A txn cannot name it: the idle save point leaves
    // both generations at one txn, and recovery may select either of them.
    private volatile int pinnedEpochGeneration = -1;
    // The partition-version txn of the current save point, pinned in the scoreboard, or -1 if none. advance()
    // pins a new save point in the free ping-pong slot, and this one becomes the other pin. Like the scoreboard,
    // both live in memory only; recovery re-establishes the pins after a restart.
    private volatile long pinnedEpochTxn = -1;
    // Which ping-pong slot holds pinnedEpochTxn: true => EPOCH_ID_A, false => EPOCH_ID_B. The next save point
    // pins into the other slot. The initial value is arbitrary, since no pin is held yet.
    private volatile boolean pinnedEpochSlotIsA = false;
    // The highest seqTxn whose WAL commit is device-durable (data, then events, then the sequencer log) and
    // lies in the contiguous durable prefix across all concurrent writers of this table (see the pending lists
    // below). Adaptive and sync advance it; under nosync and async it stays at -1, no local fsync guarantee.
    // Written on the WAL commit or group-commit flush thread, read lock-free by the QWP durable-ack threads.
    private volatile long localDurableSeqTxn = -1L;
    // Adaptive group commit (W>0) keeps the durable prefix contiguous. Several WalWriters of one table share
    // this tracker and flush their deferred batches independently (WalGroupCommitFlushQueue.sweep or the
    // writer's own commit-driven trigger), with no barrier between writers. So a writer must not advance the
    // shared frontier to its own seqTxn: writer B (seqTxn 11) flushing before writer A (seqTxn 10) would claim
    // A's page-cache-only txn 10 as durable, and the QWP durable ack would be false. Instead each writer records
    // the oldest unflushed seqTxn of its batch here, and localDurableSeqTxn advances only to
    // min(maxCoveredSeqTxn, oldest pending - 1), or to maxCoveredSeqTxn when nothing is pending. The parallel
    // lists hold one entry per writer, pendingWalIds[i] -> pendingLoSeqTxns[i], and change once per batch.
    // durableFrontierLock guards them, not `this`, which the waiter machinery uses, so a frontier update never
    // contends with fireWaiters/updateWriterTxns.
    private final Object durableFrontierLock = new Object();
    private final IntList pendingWalIds = new IntList();
    private final LongList pendingLoSeqTxns = new LongList();
    // Parallel 0/1 flags: 1 marks a pin whose writer is gone (torn down after distress, a crash or a close)
    // without flushing its batch, so no markWriterDurable(itsWalId) will come. orphanWriterPending explains
    // why any peer's device flush can release such a pin.
    private final IntList pendingOrphaned = new IntList();
    // Parallel list: orphanSeq at the moment each pin became an orphan, or Long.MAX_VALUE while the pin is live
    // (a live pin is never reaped). A flusher may reap an orphan only if this stamp precedes the mark it took
    // before its fdatasync; snapshotOrphanSweepMark() explains why the stamp is taken when the pin is orphaned.
    private final LongList pendingOrphanSeqs = new LongList();
    // Monotonic counter, bumped under durableFrontierLock by orphanWriterPending.
    private long orphanSeq;
    // Highest seqTxn any completed sequencer fdatasync has covered: seqTxn as it stood under the sequencer
    // write lock when that fdatasync ran (TableSequencerImpl.fdatasyncTxnLog). Everything at or below it is
    // device-durable, so under W>0 it is the ceiling of localDurableSeqTxn; the pins can only hold the
    // frontier lower. A running max rather than the latest value: a txn covered by a peer's earlier fdatasync
    // stays claimable after the pin that held it back is released. Guarded by durableFrontierLock.
    private long maxCoveredSeqTxn = UNINITIALIZED_TXN;
    // Set by fenceDurableFrontier when a device flush of this table's sequencer txn log fails, and never
    // cleared. Linux reports a writeback error on a file only once, so a later fdatasync of the same log, by a
    // peer writer or on a reopened fd, can return 0 although the pages it should have carried were dropped.
    // From then on no barrier can prove the log durable, so the frontier must not move. Lives here rather
    // than on the sequencer because a distressed sequencer is reopened, and the tracker survives that.
    // Guarded by durableFrontierLock, together with the errno and operation of the failure that set it.
    private boolean frontierFenced;
    private int frontierFenceErrno;
    private String frontierFenceOperation;
    // Live-view dedup-base signal. The apply
    // worker is the single writer per table, so plain volatile suffices (no CAS). A
    // coupled dedup-base live view reads these to decide whether an applied seqTxn range
    // matches its raw WAL stream (so it can raw-WAL route instead of the applied-reader
    // path). Ordering discipline: recordApplied writes divergence and trackedFrom BEFORE
    // covered, and the consumer reads covered first, so observing covered >= to also
    // observes the paired divergence/trackedFrom.
    //
    // Highest applied seqTxn recorded this process; the range the signal vouches for is
    // [trackedFrom, covered]. Benignly jumps over structural / non-DATA seqTxns.
    private volatile long dedupSignalCoveredSeqTxn = Numbers.LONG_NULL;
    // Highest seqTxn <= covered whose applied state diverges from the raw WAL stream
    // (dedup removed rows, a skipped DATA commit, or a data-shaped non-DATA op). LONG_NULL
    // if none. Monotone, so a consumer reading it later than covered only over-reports.
    private volatile long dedupSignalDivergenceSeqTxn = Numbers.LONG_NULL;
    // The from seqTxn of the first batch recorded this process; set once, never decreases.
    // A range whose lower bound sits below this is not vouched for (cold signal).
    private volatile long dedupSignalTrackedFromSeqTxn = Numbers.LONG_NULL;
    private volatile long dirtyWriterTxn;
    // Volatile because fireWaiters() and registerWaiter() can race. See comments there
    private volatile boolean dropped;
    private volatile String errorMessage = "";
    // Hard-suspend flag: when set, the table is excluded from WAL apply and (when
    // cairo.wal.apply.suspended.write.denied is enabled) denied WAL writes. Set by
    // ALTER TABLE ... SUSPEND WAL, cleared by ALTER TABLE ... RESUME WAL. The reloadable
    // cairo.wal.apply.suspended.tables config list is an additional source checked by the engine.
    private volatile boolean hardSuspended;
    private volatile ErrorTag errorTag = ErrorTag.NONE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long seqTxn = UNINITIALIZED_TXN;
    // -1 suspended
    // 0 unknown
    // 1 not suspended
    private volatile int suspendedState = 0;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long waiterRegistrationCount;
    private volatile long writerTxn = UNINITIALIZED_TXN;

    public SeqTxnTracker(CairoConfiguration configuration) {
        this.pressureControl = new TableWriterPressureControlImpl(configuration);
        this.metrics = configuration.getMetrics();
    }

    /**
     * Returns whether WalPurgeJob asked for an idle save point since the last call, and clears the request. Apply
     * worker only.
     */
    public boolean consumeIdleEpochRequest() {
        if (idleEpochRequested) {
            idleEpochRequested = false;
            return true;
        }
        return false;
    }

    public long getCommittedTxn() {
        return committedTxn;
    }

    public long getDedupSignalCoveredSeqTxn() {
        return dedupSignalCoveredSeqTxn;
    }

    public long getDedupSignalDivergenceSeqTxn() {
        return dedupSignalDivergenceSeqTxn;
    }

    public long getDedupSignalTrackedFromSeqTxn() {
        return dedupSignalTrackedFromSeqTxn;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorTag getErrorTag() {
        return errorTag;
    }

    public long getLagTxnCount() {
        return Math.max(0, this.dirtyWriterTxn - this.writerTxn);
    }

    public TableWriterPressureControl getMemPressureControl() {
        return pressureControl;
    }

    public long getDurableEpochSeqTxn() {
        return durableEpochSeqTxn;
    }

    /**
     * True until every candidate covers the required logical change and pending finalization succeeds.
     */
    public boolean isDurableEpochCompletionRequired() {
        return durableEpochCompletionRequired;
    }

    /**
     * True while a save point is owed after a change no WAL replay can redo; see {@link #setDurableEpochOwed}.
     */
    public boolean isDurableEpochOwed() {
        return durableEpochOwed;
    }

    /**
     * Returns the highest seqTxn in the contiguous device-durable prefix of the table's WAL commits, or -1
     * when none is known durable. Only adaptive and sync advance it; under nosync and async it stays -1.
     */
    public long getLocalDurableSeqTxn() {
        return localDurableSeqTxn;
    }

    /**
     * Returns the number of times recovery restored this table from its save point in this process.
     */
    public long getRecoveryIncarnation() {
        return recoveryIncarnation;
    }

    /**
     * Counts one restore from the save point; RecoveryCoordinator calls it.
     */
    public void bumpRecoveryIncarnation() {
        recoveryIncarnation++;
    }

    /**
     * Wall-clock ms at which the WAL purge job last asked for an idle save point; 0 once a save point publishes.
     */
    public long getIdleEpochRequestTs() {
        return idleEpochRequestTs;
    }

    /**
     * Wall-clock ms of the last save point (adaptive cadence gate); 0 if none yet.
     */
    public long getLastEpochTs() {
        return lastEpochTs;
    }

    /**
     * Rows applied since the last save point (adaptive backlog gate). Apply worker only.
     */
    public long getRowsSinceEpoch() {
        return rowsSinceEpoch;
    }

    /**
     * Adds to the count of rows applied since the last save point (adaptive backlog gate). Apply worker only.
     */
    public void addRowsSinceEpoch(long rows) {
        rowsSinceEpoch += rows;
    }

    /**
     * Resets the count of rows applied since the last save point; called when one publishes. Apply worker only.
     */
    public void resetRowsSinceEpoch() {
        rowsSinceEpoch = 0;
    }

    /**
     * The txn pinned for the other save point generation, or -1 if none.
     */
    public long getOtherPinnedEpochTxn() {
        return otherPinnedEpochTxn;
    }

    /**
     * The generation that holds the current save point, the one the table stands on, or -1 while no save point
     * is pinned.
     */
    public int getPinnedEpochGeneration() {
        return pinnedEpochGeneration;
    }

    public long getPinnedEpochTxn() {
        return pinnedEpochTxn;
    }

    /**
     * Which ping-pong slot holds the current save point's pin: true => EPOCH_ID_A, false => EPOCH_ID_B.
     */
    public boolean isPinnedEpochSlotA() {
        return pinnedEpochSlotIsA;
    }

    public long getSeqTxn() {
        return seqTxn;
    }

    @TestOnly
    public long getWaiterRegistrationCount() {
        return waiterRegistrationCount;
    }

    public long getWriterTxn() {
        return writerTxn;
    }

    public boolean initTxns(long newWriterTxn, long newSeqTxn, boolean isSuspended) {
        if (Unsafe.cas(this, SUSPENDED_STATE_OFFSET, 0, isSuspended ? -1 : 1) && isSuspended) {
            metrics.tableWriterMetrics().incSuspendedTables();
        }
        // seqTxn has to be initialized before writerTxn since isInitialised() method checks writerTxn
        long stxn = seqTxn;
        while (stxn < newSeqTxn && !Unsafe.cas(this, SEQ_TXN_OFFSET, stxn, newSeqTxn)) {
            stxn = seqTxn;
        }
        metrics.walMetrics().addSeqTxn(newSeqTxn - Math.max(0, stxn));
        long wtxn = writerTxn;
        while (newWriterTxn > wtxn && !Unsafe.cas(this, WRITER_TXN_OFFSET, wtxn, newWriterTxn)) {
            wtxn = writerTxn;
        }
        metrics.walMetrics().addWriterTxn(newWriterTxn - Math.max(0, wtxn));
        return seqTxn > 0 && seqTxn > writerTxn;
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isHardSuspended() {
        return hardSuspended;
    }

    public boolean isIdleEpochRequested() {
        return idleEpochRequested;
    }

    public boolean isInitialised() {
        return writerTxn != UNINITIALIZED_TXN;
    }

    public boolean isSuspended() {
        return suspendedState < 0;
    }

    public boolean notifyOnCheck(long newSeqTxn) {
        // Updates seqTxn and returns true if CheckWalTransactionsJob should post notification
        // to run ApplyWal2TableJob for the table
        long stxn = seqTxn;
        while (newSeqTxn > stxn && !Unsafe.cas(this, SEQ_TXN_OFFSET, stxn, newSeqTxn)) {
            stxn = seqTxn;
        }
        return writerTxn < seqTxn && suspendedState > 0 && pressureControl.isReadyToProcess();
    }

    public boolean notifyOnCommit(long newSeqTxn) {
        // Updates seqTxn and returns true if the commit should post notification
        // to run ApplyWal2TableJob for the table
        long stxn = seqTxn;
        while (newSeqTxn > stxn) {
            if (Unsafe.cas(this, SEQ_TXN_OFFSET, stxn, newSeqTxn)) {
                metrics.walMetrics().addSeqTxn(newSeqTxn - stxn);
                break;
            }
            stxn = seqTxn;
        }
        // Return that Apply job notification is needed
        // when there is some new work for ApplyWal2Table job
        // Notify on transactions that are first move seqTxn from -1 or 0
        // or when writerTxn is behind seqTxn by 1 and not suspended
        return (stxn < 1 || writerTxn == (newSeqTxn - 1)) && suspendedState >= 0;
    }

    public void notifyOnDrop() {
        synchronized (this) {
            if (dropped) {
                return;
            }
            dropped = true;
        }
        metrics.walMetrics().addSeqTxn(-seqTxn);
        metrics.walMetrics().addWriterTxn(-writerTxn);
        // Release this table's share of the engine-wide durable-ack gauge (wal_apply_local_durable_seq_txn), so a
        // dropped table does not leave its last value there. resetDurableFrontier() makes the one decrement, and
        // the dropped guard above makes this run at most once per table.
        resetDurableFrontier();
        fireWaiters();
    }

    /**
     * Records an applied WAL seqTxn range for the live-view dedup-base signal.
     * Called once per applied batch/op by the
     * apply worker, which is the single writer per table -- plain volatile writes, no CAS.
     * <p>
     * Ordering discipline: divergence and trackedFrom are written BEFORE covered, so a
     * consumer that reads covered first (and observes {@code covered >= to}) is guaranteed
     * to also observe the paired divergence/trackedFrom.
     *
     * @param fromSeqTxn    the first seqTxn of the applied batch/op
     * @param coveredSeqTxn the highest seqTxn now applied by this batch/op
     * @param diverged      true if the applied state differs from the raw WAL stream for
     *                      this range: dedup removed rows, a DATA commit was skipped, or a
     *                      data-shaped non-DATA op (TRUNCATE / DROP PARTITION / TTL /
     *                      REPLACE_RANGE) removed or replaced rows
     */
    public void recordApplied(long fromSeqTxn, long coveredSeqTxn, boolean diverged) {
        if (dedupSignalTrackedFromSeqTxn == Numbers.LONG_NULL) {
            dedupSignalTrackedFromSeqTxn = fromSeqTxn;
        }
        if (diverged && coveredSeqTxn > dedupSignalDivergenceSeqTxn) {
            dedupSignalDivergenceSeqTxn = coveredSeqTxn;
        }
        // covered LAST: release-store pairing divergence/trackedFrom with the covered read.
        if (coveredSeqTxn > dedupSignalCoveredSeqTxn) {
            dedupSignalCoveredSeqTxn = coveredSeqTxn;
        }
    }

    public SourceRegistrationResult registerWaiter(FiberWalWaitRegistration waiter) {
        SourceRegistrationResult result = waiter.register(fiberWaiters);
        if (result == SourceRegistrationResult.ACCEPTED) {
            Unsafe.getAndAddLong(this, WAITER_REGISTRATION_COUNT_OFFSET, 1);
            if (writerTxn >= waiter.getTargetWriterTxn() || isSuspended() || dropped) {
                fireWaiters();
            }
        }
        return result;
    }

    /**
     * Records the table's committed _txn txn. Table writer only, when it is released.
     */
    public void setCommittedTxn(long committedTxn) {
        this.committedTxn = committedTxn;
    }

    public void setHardSuspended(boolean hardSuspended) {
        this.hardSuspended = hardSuspended;
    }

    public void setSuspended(ErrorTag errorTag, String errorMessage) {
        this.errorTag = errorTag;
        this.errorMessage = errorMessage;

        // should be the last one to be set
        // to make sure error details are available for read when the table is suspended
        this.suspendedState = -1;

        metrics.tableWriterMetrics().incSuspendedTables();
        fireWaiters();
    }

    public void setDurableEpochSeqTxn(long durableEpochSeqTxn) {
        this.durableEpochSeqTxn = durableEpochSeqTxn;
    }

    /**
     * Advances the device-durable frontier to {@code seqTxn}. Monotonic: a value below the current frontier
     * is ignored, since durable data never becomes non-durable, which also makes an out-of-order prefix
     * recompute in {@link #markWriterDurable(int, long, long)} safe.
     *
     * <p>Called directly once a commit is device-durable: under sync, and under adaptive with {@code W=0}, a
     * writer's commit is durable before its seqTxn is assigned, so concurrent writers cannot over-claim. A save
     * point also moves the frontier to its seqTxn once it has flushed the sequencer log.
     * {@link #markWriterDurable(int, long, long)} calls it under {@code durableFrontierLock} for the
     * {@code W>0} contiguous prefix.
     */
    public void setLocalDurableSeqTxn(long seqTxn) {
        synchronized (durableFrontierLock) {
            checkFrontierFencedLocked();
            setLocalDurableSeqTxnLocked(seqTxn);
        }
    }

    /**
     * Adaptive group commit (W&gt;0): records the oldest unflushed {@code loSeqTxn} of {@code walId}'s current
     * batch, so the shared durable-ack frontier stays at the contiguous durable prefix across concurrent
     * writers. putIfAbsent: a writer's later commits in the same batch keep its recorded floor. Only
     * {@link #markWriterDurable(int, long, long)} (after that writer's fdatasync) and
     * {@link #resetDurableFrontier()} (recovery, restart) drop the pin. A teardown without a flush leaves the
     * pin, so the frontier stays behind the writer's non-durable data.
     */
    public void registerWriterPending(int walId, long loSeqTxn) {
        synchronized (durableFrontierLock) {
            final int idx = indexOfPendingWalId(walId);
            if (idx < 0) {
                pendingWalIds.add(walId);
                pendingLoSeqTxns.add(loSeqTxn);
                pendingOrphaned.add(0);
                pendingOrphanSeqs.add(Long.MAX_VALUE);
            } else {
                // Unreachable while WalIdGenerator never reuses a walId within a process. Should one be reused,
                // the entry is un-orphaned (a live writer owns it again and will call markWriterDurable) and keeps
                // its older, lower floor, so a later commit in the batch never raises the pin.
                pendingOrphaned.setQuick(idx, 0);
                pendingOrphanSeqs.setQuick(idx, Long.MAX_VALUE);
            }
        }
    }

    /**
     * Adaptive group commit (W&gt;0): returns the orphan-sweep mark for a later
     * {@link #markWriterDurable(int, long, long)}. The caller takes it before its
     * {@code sequencer.fdatasyncTxnLog()} and passes it unchanged to the {@code markWriterDurable} that follows.
     *
     * <p>That order is the safety argument. Orphaning finalises a pin's txn range: the writer is dead from that
     * instant and appends no further records. So an orphan stamped at or below the mark appended all its txns
     * before the mark, which precedes the fdatasync, and reaping the orphan cannot over-claim.
     *
     * <p>The stamp is taken when the pin is orphaned, not when it is registered, because registration is
     * putIfAbsent: later commits of the same batch reuse the pin, so a registration stamp would date only the
     * start of a range that keeps growing. A peer could then commit after the flusher's fdatasync, die, and be
     * reaped on its old stamp, over a txn the fdatasync never covered. An orphan stamped after the mark stays
     * pinned until the next flush, the conservative direction.
     */
    public long snapshotOrphanSweepMark() {
        synchronized (durableFrontierLock) {
            return orphanSeq;
        }
    }

    /**
     * Adaptive group commit (W&gt;0): marks {@code walId}'s pin as orphaned, because its writer is gone (torn
     * down after distress, a crash or a close) without a device flush, so no
     * {@link #markWriterDurable(int, long, long)} will arrive for it. The pin stays: the writer's shared
     * sequencer records are still only in the page cache, and dropping the pin would let the durable-ack
     * frontier advance over them.
     *
     * <p>Any peer's flush can release an orphan. Under adaptive with W&gt;0 every writer makes its private files
     * device-durable before it sequences ({@code WalWriter.syncIfRequired0} fdatasyncs each segment column and
     * the events file, then {@code getSequencerTxn()} assigns the seqTxn); a batch defers only the shared
     * sequencer flush. So the orphan's txns are durable once any writer of this table completes
     * {@code sequencer.fdatasyncTxnLog()}, and {@link #markWriterDurable(int, long, long)}, which runs right
     * after that fdatasync, reaps orphans. Nothing else clears a dead writer's pin
     * ({@link #resetDurableFrontier()} runs only on table drop or tracker purge), so without that reap one
     * distressed writer would hold the table's durable acks back until the next save point, or forever on a
     * table that receives no further applies.
     *
     * <p>Idempotent; an unknown or already removed walId is a no-op.
     */
    public void orphanWriterPending(int walId) {
        synchronized (durableFrontierLock) {
            final int idx = indexOfPendingWalId(walId);
            if (idx > -1) {
                pendingOrphaned.setQuick(idx, 1);
                // Stamp when the pin is orphaned, and again on a repeat call: the writer is dead from this
                // point, so its txn range is final. A later stamp is only more conservative.
                pendingOrphanSeqs.setQuick(idx, ++orphanSeq);
            }
        }
    }

    /**
     * Adaptive group commit (W&gt;0): called after {@code walId}'s batched device flush (data, then events,
     * then the sequencer log) completes. Drops the writer's pin, then advances {@link #localDurableSeqTxn}
     * monotonically to the contiguous durable prefix across the remaining pending writers:
     * {@code min(oldest unflushed) - 1}, or the covered ceiling when nothing is pending, and never above that
     * ceiling. It never advances to the flushing writer's own seqTxn, which could over-claim a peer's unflushed
     * txn. An unknown or already removed walId is a no-op that still recomputes the prefix.
     *
     * <p>{@code coveredSeqTxn} is what the caller's fdatasync covered: {@code seqTxn} as read under the
     * sequencer write lock, where no append is in flight. The lock is released before this method runs, so the
     * writer's next commit or a peer may already have sequenced further txns that reuse (putIfAbsent) the pin
     * dropped here, and a structure txn carries no pin at all. So the covered value bounds the frontier, kept as
     * a running max in {@link #maxCoveredSeqTxn}, not the live {@code seqTxn}.
     *
     * <p>Also reaps the orphaned pins (see {@link #orphanWriterPending(int)}) stamped at or below
     * {@code orphanSweepMark}: the caller's flush of the whole shared sequencer log makes their txns durable,
     * because private WAL files are flushed before sequencing. Orphans are cleared only here and by
     * {@link #resetDurableFrontier()}, never on the teardown path, where the batch is still volatile.
     *
     * <p>Throws a data-sync failure, leaving every pin in place, once {@link #fenceDurableFrontier(Throwable)}
     * has run: the caller's fdatasync may have returned 0 only because the kernel had already reported the
     * error to the writer whose flush failed.
     */
    public void markWriterDurable(int walId, long orphanSweepMark, long coveredSeqTxn) {
        synchronized (durableFrontierLock) {
            // Before touching the pins: a rejected flush must leave its pin for the caller to orphan.
            checkFrontierFencedLocked();
            final int idx = indexOfPendingWalId(walId);
            if (idx > -1) {
                removePendingIndex(idx);
            }
            // Reap orphans back to front, so a removal cannot shift an index not yet visited, and only those
            // stamped at or before the caller's mark (see snapshotOrphanSweepMark). A later orphan may hold a
            // sequencer record the caller's fdatasync did not cover; it stays pinned until the next flush.
            for (int i = pendingOrphaned.size() - 1; i >= 0; i--) {
                if (pendingOrphaned.getQuick(i) == 1 && pendingOrphanSeqs.getQuick(i) <= orphanSweepMark) {
                    removePendingIndex(i);
                }
            }
            if (coveredSeqTxn > maxCoveredSeqTxn) {
                maxCoveredSeqTxn = coveredSeqTxn;
            }
            final long target = pendingWalIds.size() == 0
                    ? maxCoveredSeqTxn
                    : Math.min(maxCoveredSeqTxn, minPendingLoSeqTxn() - 1);
            setLocalDurableSeqTxnLocked(target);
        }
    }

    /**
     * Freezes the durable-ack frontier after a failed device flush of this table's sequencer txn log. Must be
     * called while the sequencer write lock is still held, so that no peer can run its own fdatasync of the
     * log, and see a 0 that hides the error, before the fence is up. Every later
     * {@link #markWriterDurable(int, long, long)} and {@link #setLocalDurableSeqTxn(long)} then throws. A no-op
     * unless {@code failure} is, or is caused by, a data-sync failure.
     */
    public void fenceDurableFrontier(Throwable failure) {
        for (Throwable cursor = failure; cursor != null; cursor = cursor.getCause()) {
            if (cursor instanceof CairoException e && e.isDataSyncFailure()) {
                synchronized (durableFrontierLock) {
                    if (!frontierFenced) {
                        frontierFenced = true;
                        frontierFenceErrno = e.getErrno();
                        frontierFenceOperation = e.getDataSyncOperation();
                    }
                }
                return;
            }
        }
    }

    public boolean isDurableFrontierFenced() {
        synchronized (durableFrontierLock) {
            return frontierFenced;
        }
    }

    /**
     * Recovery and restart reset: clears every pending-writer pin and drops the durable frontier back to -1,
     * adjusting the engine-wide frontier gauge so that a re-advance from -1 does not double-count. A fresh
     * writer then recomputes from an empty map instead of the pins a torn-down writer left. Keeps the fence
     * set by {@link #fenceDurableFrontier(Throwable)}: a failed barrier is final for the process.
     */
    public void resetDurableFrontier() {
        clearPinnedEpoch();
        synchronized (durableFrontierLock) {
            pendingWalIds.clear();
            pendingLoSeqTxns.clear();
            pendingOrphaned.clear();
            pendingOrphanSeqs.clear();
            maxCoveredSeqTxn = UNINITIALIZED_TXN;
            final long current = localDurableSeqTxn;
            if (current > 0) {
                metrics.walMetrics().addLocalDurableSeqTxn(-current);
            }
            localDurableSeqTxn = -1L;
        }
    }

    private void checkFrontierFencedLocked() {
        if (frontierFenced) {
            throw CairoException.dataSyncFailureCarry(frontierFenceErrno, frontierFenceOperation)
                    .put("durable frontier is fenced by a failed sequencer barrier");
        }
    }

    private void setLocalDurableSeqTxnLocked(long seqTxn) {
        if (seqTxn > localDurableSeqTxn) {
            final long previous = localDurableSeqTxn;
            metrics.walMetrics().addLocalDurableSeqTxn(seqTxn - Math.max(0, previous));
            localDurableSeqTxn = seqTxn;
        }
    }

    // Removes entry `idx` from all four parallel pending lists. Callers hold durableFrontierLock.
    private void removePendingIndex(int idx) {
        pendingWalIds.removeIndex(idx);
        pendingLoSeqTxns.removeIndex(idx);
        pendingOrphaned.removeIndex(idx);
        pendingOrphanSeqs.removeIndex(idx);
    }

    /**
     * The number of contiguous-prefix pins held, live and orphaned, so that a reaped pin can be told apart
     * from one a later save point merely masks by moving {@link #localDurableSeqTxn} past it.
     */
    @TestOnly
    public int getPendingWriterPinCount() {
        synchronized (durableFrontierLock) {
            return pendingWalIds.size();
        }
    }

    /**
     * The number of orphaned pins (writer torn down without a device flush).
     */
    @TestOnly
    public int getOrphanedWriterPinCount() {
        synchronized (durableFrontierLock) {
            int n = 0;
            for (int i = 0, k = pendingOrphaned.size(); i < k; i++) {
                if (pendingOrphaned.getQuick(i) == 1) {
                    n++;
                }
            }
            return n;
        }
    }

    private int indexOfPendingWalId(int walId) {
        for (int i = 0, n = pendingWalIds.size(); i < n; i++) {
            if (pendingWalIds.getQuick(i) == walId) {
                return i;
            }
        }
        return -1;
    }

    private long minPendingLoSeqTxn() {
        long min = Long.MAX_VALUE;
        for (int i = 0, n = pendingLoSeqTxns.size(); i < n; i++) {
            final long v = pendingLoSeqTxns.getQuick(i);
            if (v < min) {
                min = v;
            }
        }
        return min;
    }

    /**
     * Asks the apply job for one more save point at the table's current state; see {@link #consumeIdleEpochRequest}.
     * WalPurgeJob only, with the wall-clock ms of the request.
     */
    public void requestIdleEpoch(long nowMs) {
        idleEpochRequestTs = nowMs;
        idleEpochRequested = true;
    }

    /**
     * Record the wall-clock ms of the last durable epoch (adaptive cadence gate). A save point ends the idle
     * request back-off.
     */
    public void setLastEpochTs(long lastEpochTs) {
        this.lastEpochTs = lastEpochTs;
        this.idleEpochRequestTs = 0;
    }

    /**
     * Arms at required-operation boundaries or recovery; only the writer's successful completion clears it.
     */
    public void setDurableEpochCompletionRequired(boolean required) {
        durableEpochCompletionRequired = required;
    }

    /**
     * Marks cadence-independent publication debt. A single successful save point clears it.
     */
    public void setDurableEpochOwed(boolean durableEpochOwed) {
        this.durableEpochOwed = durableEpochOwed;
    }

    /**
     * Records the save point txn pinned in the scoreboard (or -1 when none), the ping-pong slot that holds it
     * (true => EPOCH_ID_A), the other generation's pin in the other slot (or -1), and the generation that holds
     * the current save point. Called after advance() completes the pin handover.
     */
    public synchronized void setPinnedEpoch(long pinnedEpochTxn, boolean slotIsA, long otherPinnedEpochTxn, int pinnedEpochGeneration) {
        this.pinnedEpochTxn = pinnedEpochTxn;
        this.pinnedEpochSlotIsA = slotIsA;
        this.otherPinnedEpochTxn = otherPinnedEpochTxn;
        this.pinnedEpochGeneration = pinnedEpochGeneration;
    }

    public synchronized void clearPinnedEpoch() {
        pinnedEpochTxn = -1;
        pinnedEpochSlotIsA = false;
        otherPinnedEpochTxn = -1;
        pinnedEpochGeneration = -1;
        durableEpochSeqTxn = 0;
    }

    public boolean isRangeAvailableToEpoch(long fromTxn, long toTxn) {
        final long epochTxn = pinnedEpochTxn;
        final long otherEpochTxn = otherPinnedEpochTxn;
        return (epochTxn < fromTxn || epochTxn >= toTxn) && (otherEpochTxn < fromTxn || otherEpochTxn >= toTxn);
    }

    /**
     * Re-establishes the save point pins on a scoreboard the pool has just created for this table, in the
     * slots {@link #setPinnedEpoch} recorded, so the next save point's handover releases exactly these pins.
     * <p>
     * This tracker, not the scoreboard, is the pins' source of truth: the pool frees a scoreboard as soon as
     * nothing references it, and its replacement starts blank. Everything that moves a pin -- the writer's
     * save point handover, recovery's pin, the release on leaving adaptive -- holds a reference to the pooled
     * scoreboard until it has recorded the result here, so a replacement can only be created once the record
     * is final. Clearing the record on table drop or tracker purge takes no reference, and needs none: a
     * board that keeps the stale pin only over-protects a table that is going away.
     * <p>
     * The monitor keeps the txn and its slot a consistent pair. It is a leaf lock: nothing called under it
     * takes another lock, so the pool may call this under its own lock.
     */
    public synchronized void restoreEpochPin(TxnScoreboard scoreboard) {
        final long epochTxn = pinnedEpochTxn;
        if (epochTxn > -1) {
            final int slotId = pinnedEpochSlotIsA ? TxnScoreboard.EPOCH_ID_A : TxnScoreboard.EPOCH_ID_B;
            final boolean pinned = scoreboard.incrementTxn(slotId, epochTxn);
            assert pinned : "epoch pin slot busy on a new scoreboard";
        }
        final long otherEpochTxn = otherPinnedEpochTxn;
        if (otherEpochTxn > -1) {
            final int slotId = pinnedEpochSlotIsA ? TxnScoreboard.EPOCH_ID_B : TxnScoreboard.EPOCH_ID_A;
            final boolean pinned = scoreboard.incrementTxn(slotId, otherEpochTxn);
            assert pinned : "other epoch pin slot busy on a new scoreboard";
        }
    }

    public void setUnsuspended() {
        // should be the first one to be set
        // no error details should be read when table is not suspended
        this.suspendedState = 1;

        this.errorTag = ErrorTag.NONE;
        this.errorMessage = "";

        metrics.tableWriterMetrics().decSuspendedTables();
    }

    /**
     * Updates writerTxn and dirtyWriterTxn and returns true if the ApplyWal2Tables job should be notified.
     *
     * @param writerTxn      txn that is available for reading
     * @param dirtyWriterTxn txn that is in flight that is not yet fully written
     * @return true if ApplyWal2Tables job should be notified
     */
    public boolean updateWriterTxns(long writerTxn, long dirtyWriterTxn) {
        boolean progressMade = false;
        synchronized (this) {
            if (dropped) {
                return false;
            }
            long prevWriterTxn = this.writerTxn;
            long prevDirtyWriterTxn = this.dirtyWriterTxn;
            this.writerTxn = writerTxn;
            this.dirtyWriterTxn = dirtyWriterTxn;
            // Progress made means table is not suspended
            if (writerTxn > prevWriterTxn) {
                suspendedState = 1;
                metrics.walMetrics().addWriterTxn(writerTxn - prevWriterTxn);
                progressMade = true;
            } else if (dirtyWriterTxn > prevDirtyWriterTxn) {
                suspendedState = 1;
            }
        }
        if (progressMade) {
            fireWaiters();
        }
        return writerTxn < seqTxn;
    }

    private void fireWaiters() {
        fiberWaiters.fire(writerTxn, isSuspended() || dropped);
    }
}
