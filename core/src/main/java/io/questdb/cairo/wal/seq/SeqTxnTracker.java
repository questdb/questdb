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
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.mp.ValueHolder;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

public class SeqTxnTracker {
    public static final long UNINITIALIZED_TXN = -1;
    private static final long SEQ_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "seqTxn");
    private static final long SUSPENDED_STATE_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "suspendedState");
    private static final long WRITER_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "writerTxn");
    private static final ThreadLocal<WaiterHolder> HOLDER = ThreadLocal.withInitial(WaiterHolder::new);
    private final Metrics metrics;
    private final TableWriterPressureControlImpl pressureControl;
    private final Queue<WaiterHolder> waiters = ConcurrentQueue.createConcurrentQueue(WaiterHolder::new);
    private volatile long dirtyWriterTxn;
    // Volatile because fireWaiters() and registerWaiter() read this outside the
    // tracker monitor; without volatile they could observe a stale false after
    // notifyOnDrop has flipped it to true.
    private volatile boolean dropped;
    private volatile String errorMessage = "";
    private volatile ErrorTag errorTag = ErrorTag.NONE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long seqTxn = UNINITIALIZED_TXN;
    // -1 suspended
    // 0 unknown
    // 1 not suspended
    private volatile int suspendedState = 0;
    private volatile long writerTxn = UNINITIALIZED_TXN;

    public SeqTxnTracker(CairoConfiguration configuration) {
        this.pressureControl = new TableWriterPressureControlImpl(configuration);
        this.metrics = configuration.getMetrics();
    }

    /**
     * Drains the waiter queue and cancels any waiter whose deadline has passed.
     * Cancelled waiters are enqueued on their resume job so the parked body can observe
     * the cancellation and throw. Non-expired waiters are re-enqueued. Called periodically
     * by {@link io.questdb.cairo.wal.seq.WaiterTimeoutJob}.
     *
     * @param nowMillis current time in milliseconds, same clock domain as waiter deadlines
     */
    public void cancelExpiredWaiters(long nowMillis) {
        WaiterHolder holder = HOLDER.get();
        TxnWaiter sentinel = null;
        while (waiters.tryDequeue(holder)) {
            TxnWaiter w = holder.waiter;
            holder.waiter = null;
            if (w == null) {
                continue;
            }
            if (w == sentinel) {
                // Completed one full pass with no further expirations; put w back and stop.
                enqueueHolder(holder, w);
                return;
            }
            if (w.isExpired(nowMillis)) {
                if (w.tryCancel()) {
                    w.resumeJob.enqueue(w.cont);
                }
            } else {
                if (sentinel == null) {
                    sentinel = w;
                }
                enqueueHolder(holder, w);
            }
        }
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

    @TestOnly
    public long getSeqTxn() {
        return seqTxn;
    }

    @TestOnly
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

    public synchronized void notifyOnDrop() {
        if (dropped) {
            return;
        }
        dropped = true;
        metrics.walMetrics().addSeqTxn(-seqTxn);
        metrics.walMetrics().addWriterTxn(-writerTxn);
        fireWaiters();
    }

    /**
     * Registers a parked SQL continuation that will be resumed when the tracker's
     * {@code writerTxn} advances to at least {@code waiter.targetWriterTxn}, or when
     * the table becomes suspended or dropped. A waiter whose target is already met
     * at registration time is fired before this method returns, covering the obvious
     * race where {@code updateWriterTxns} ran between the caller's pre-check and the
     * register call.
     */
    public void registerWaiter(TxnWaiter waiter) {
        enqueueHolder(HOLDER.get(), waiter);
        if (writerTxn >= waiter.targetWriterTxn || isSuspended() || dropped) {
            fireWaiters();
        }
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
    public synchronized boolean updateWriterTxns(long writerTxn, long dirtyWriterTxn) {
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
            fireWaiters();
        } else if (dirtyWriterTxn > prevDirtyWriterTxn) {
            suspendedState = 1;
        }
        return writerTxn < seqTxn;
    }

    private void enqueueHolder(WaiterHolder holder, TxnWaiter waiter) {
        holder.waiter = waiter;
        waiters.enqueue(holder);
        holder.waiter = null;
    }

    /**
     * Drains the waiter queue, firing any waiter whose target writerTxn has been met or
     * whose table has become suspended/dropped. Non-ready waiters are re-enqueued. Fired
     * waiters are CAS'd PENDING -> FIRED and the winning thread enqueues the continuation
     * on the waiter's resume job.
     *
     * <p>Streaming algorithm: dequeue one waiter at a time, fire-or-reenqueue, and remember
     * the first waiter we put back. When we encounter that sentinel a second time, we have
     * made one full pass without firing anything more in the tail — put the sentinel back
     * and stop. Zero allocation; tolerates concurrent drains because each thread carries
     * its own sentinel and the {@code TxnWaiter.state} CAS makes double-firing impossible.
     */
    private void fireWaiters() {
        WaiterHolder holder = HOLDER.get();
        long wtxn = this.writerTxn;
        boolean terminal = isSuspended() || dropped;
        TxnWaiter sentinel = null;
        while (waiters.tryDequeue(holder)) {
            TxnWaiter w = holder.waiter;
            holder.waiter = null;
            if (w == null) {
                continue;
            }
            if (w == sentinel) {
                // Completed one full pass with no further fireable waiters; put w back and stop.
                enqueueHolder(holder, w);
                return;
            }
            if (terminal || wtxn >= w.targetWriterTxn) {
                if (w.tryFire()) {
                    w.resumeJob.enqueue(w.cont);
                }
                // cancelled waiters were already enqueued by the canceller; drop on the floor
            } else {
                if (sentinel == null) {
                    sentinel = w;
                }
                enqueueHolder(holder, w);
            }
        }
    }

    private static final class WaiterHolder implements ValueHolder<WaiterHolder> {
        TxnWaiter waiter;

        @Override
        public void clear() {
            waiter = null;
        }

        @Override
        public void copyTo(WaiterHolder dest) {
            dest.waiter = waiter;
        }
    }
}
