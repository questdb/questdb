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
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.SourceRegistrationResult;
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
