/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.wal.O3JobParallelismRegulator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.TestOnly;

public class SeqTxnTracker implements O3JobParallelismRegulator {
    public static final long UNINITIALIZED_TXN = -1;
    private static final Log LOG = LogFactory.getLog(SeqTxnTracker.class);
    private static final long SEQ_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "seqTxn");
    private static final long SUSPENDED_STATE_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "suspendedState");
    private static final long WRITER_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "writerTxn");
    private volatile long dirtyWriterTxn;
    private volatile String errorMessage = "";
    private volatile ErrorTag errorTag = ErrorTag.NONE;
    private int maxRecordedInflightPartitions = 1;
    // positive int: holds max parallelism
    // negative int: holds backoff counter
    private int memoryPressureRegulationValue = Integer.MAX_VALUE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long seqTxn = UNINITIALIZED_TXN;
    // -1 suspended
    // 0 unknown
    // 1 not suspended
    private volatile int suspendedState = 0;
    private long walBackoffUntil = -1;
    private volatile long writerTxn = UNINITIALIZED_TXN;

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorTag getErrorTag() {
        return errorTag;
    }

    public long getLagTxnCount() {
        return Math.max(0, this.dirtyWriterTxn - this.writerTxn);
    }

    public int getMaxO3MergeParallelism() {
        return Math.max(1, memoryPressureRegulationValue);
    }

    public int getMemoryPressureLevel() {
        if (memoryPressureRegulationValue == Integer.MAX_VALUE) {
            return 0;
        }
        if (memoryPressureRegulationValue < 0) {
            return 2;
        }
        return 1;
    }

    @TestOnly
    public long getSeqTxn() {
        return seqTxn;
    }

    @TestOnly
    public long getWriterTxn() {
        return writerTxn;
    }

    public void hadEnoughMemory(CharSequence tableName, Rnd rnd) {
        maxRecordedInflightPartitions = 1;
        walBackoffUntil = -1;
        if (memoryPressureRegulationValue == Integer.MAX_VALUE) {
            // already at max parallelism, can't go more optimistic
            return;
        }
        if (memoryPressureRegulationValue < 0) {
            // was in backoff, go back to parallelism = 1
            memoryPressureRegulationValue = 1;
            LOG.info().$("Memory pressure easing off, removing backoff [table=").$(tableName).I$();
            return;
        }
        if (rnd.nextInt(4) == 0) { // 25% chance to double parallelism
            int beforeDoubling = memoryPressureRegulationValue;
            memoryPressureRegulationValue *= 2;
            if (memoryPressureRegulationValue < beforeDoubling) {
                // overflow
                memoryPressureRegulationValue = Integer.MAX_VALUE;
            }
        }
        LOG.info().$("Memory pressure easing off [table=").$(tableName).$(", maxParallelism=").$(memoryPressureRegulationValue).I$();
    }

    public boolean initTxns(long newWriterTxn, long newSeqTxn, boolean isSuspended) {
        Unsafe.cas(this, SUSPENDED_STATE_OFFSET, 0, isSuspended ? -1 : 1);
        long wtxn = writerTxn;
        while (newWriterTxn > wtxn && !Unsafe.cas(this, WRITER_TXN_OFFSET, wtxn, newWriterTxn)) {
            wtxn = writerTxn;
        }
        long stxn = seqTxn;
        while (stxn < newSeqTxn && !Unsafe.cas(this, SEQ_TXN_OFFSET, stxn, newSeqTxn)) {
            stxn = seqTxn;
        }
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
        return writerTxn < seqTxn && suspendedState > 0 && MicrosecondClockImpl.INSTANCE.getTicks() > walBackoffUntil;
    }

    public boolean notifyOnCommit(long newSeqTxn) {
        // Updates seqTxn and returns true if the commit should post notification
        // to run ApplyWal2TableJob for the table
        long stxn = seqTxn;
        while (newSeqTxn > stxn) {
            if (Unsafe.cas(this, SEQ_TXN_OFFSET, stxn, newSeqTxn)) {
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

    /**
     * Applies anti-OOM measures if possible, either by reducing job parallelism, or applying backoff.<br>
     * If it was possible to apply more measures, returns true → the operation can retry.<br>
     * If all measures were exhausted, returns false → the operation should now fail.
     */
    public boolean onOutOfMemory(long nowMicros, CharSequence tableName, Rnd rnd) {
        if (maxRecordedInflightPartitions == 1) {
            // There was no parallelism
            if (memoryPressureRegulationValue <= -5) {
                // Maximum backoff already tried => fail
                walBackoffUntil = -1;
                return false;
            }
            if (memoryPressureRegulationValue > 0) {
                // Switch from reducing parallelism to backoff
                memoryPressureRegulationValue = -1;
            } else {
                // Increase backoff counter
                memoryPressureRegulationValue--;
            }
            int delayMicros = rnd.nextInt(4_000_000);
            LOG.info().$("Memory pressure is high [table=").$(tableName).$(", backoffCounter=").$(-memoryPressureRegulationValue).$(", delay=").$(delayMicros).$(" us]").$();
            walBackoffUntil = nowMicros + delayMicros;
            return true;
        }
        // There was some parallelism, halve max parallelism
        walBackoffUntil = -1;
        memoryPressureRegulationValue = maxRecordedInflightPartitions / 2;
        maxRecordedInflightPartitions = 1;
        LOG.info().$("Memory pressure is high [table=").$(tableName).$(", maxParallelism=").$(memoryPressureRegulationValue).I$();

        return true;
    }

    public void setSuspended(ErrorTag errorTag, String errorMessage) {
        this.errorTag = errorTag;
        this.errorMessage = errorMessage;

        // should be the last one to be set
        // to make sure error details are available for read when the table is suspended
        this.suspendedState = -1;
    }

    public void setUnsuspended() {
        // should be the first one to be set
        // no error details should be read when table is not suspended
        this.suspendedState = 1;

        this.errorTag = ErrorTag.NONE;
        this.errorMessage = "";
    }

    public boolean shouldBackOffDueToMemoryPressure(long nowMicros) {
        return nowMicros < walBackoffUntil;
    }

    @Override
    public void updateInflightPartitions(int count) {
        maxRecordedInflightPartitions = Math.max(maxRecordedInflightPartitions, count);
    }

    /**
     * Updates writerTxn and dirtyWriterTxn and returns true if the Apply2Wal job should be notified.
     * This method is not thread-safe and should be called under TableWriter lock.
     *
     * @param writerTxn      txn that is available for reading
     * @param dirtyWriterTxn txn that is in flight that is not yet fully written
     * @return true if Apply2Wal job should be notified
     */
    public synchronized boolean updateWriterTxns(long writerTxn, long dirtyWriterTxn) {
        // This is only called under TableWriter lock inside Apply2Wal job
        // with no threads race
        // TODO: remove other calls and make the call non-synchronized. The calls to reset txn
        // when queue is full seems like redundant after all the changes in CheckWalTransactionsJob
        long prevWriterTxn = this.writerTxn;
        long prevDirtyWriterTxn = this.dirtyWriterTxn;
        this.writerTxn = writerTxn;
        this.dirtyWriterTxn = dirtyWriterTxn;

        // Progress made means table is not suspended
        if (writerTxn > prevWriterTxn || dirtyWriterTxn > prevDirtyWriterTxn) {
            suspendedState = 1;
        }
        return writerTxn < seqTxn;
    }
}
