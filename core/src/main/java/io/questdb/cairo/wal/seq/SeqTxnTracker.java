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
import io.questdb.cairo.wal.O3InflightPartitionRegulator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.TestOnly;

public class SeqTxnTracker implements O3InflightPartitionRegulator {
    private static final Log LOG = LogFactory.getLog(SeqTxnTracker.class);
    private static final long SEQ_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "seqTxn");
    private static final long SUSPENDED_STATE_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "suspendedState");
    private static final long WRITER_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "writerTxn");
    private final Rnd rnd;
    private int backoffCounter = 0;
    private volatile String errorMessage = "";
    private volatile ErrorTag errorTag = ErrorTag.NONE;
    private int maxParallelism = Integer.MAX_VALUE;
    private int maxRecordedInflightPartitions = 1;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long seqTxn = -1;
    // -1 suspended
    // 0 unknown
    // 1 not suspended
    private volatile int suspendedState = 0;
    private long walBackoffUntil = -1;
    private volatile long writerTxn = -1;

    public SeqTxnTracker(Rnd rnd) {
        this.rnd = rnd;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorTag getErrorTag() {
        return errorTag;
    }

    public int getMaxO3MergeParallelism() {
        return maxParallelism;
    }

    public int getMemoryPressureLevel() {
        // todo
        return 42;
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
        return writerTxn != -1;
    }

    @TestOnly
    public boolean isSuspended() {
        return suspendedState < 0;
    }

    public boolean notifyCommitReadable(long newWriterTxn) {
        // This is only called under TableWriter lock
        // with no threads race
        writerTxn = newWriterTxn;
        if (newWriterTxn > -1) {
            suspendedState = 1;
        }
        return newWriterTxn < seqTxn;
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

    public boolean onPressureIncreased(long nowMicros) {
        if (maxRecordedInflightPartitions == 1) {
            if (backoffCounter >= 5) {
                walBackoffUntil = -1;
                return false;
            }
            backoffCounter++;
            LOG.info().$("Memory pressure is high, backoffCounter=").$(backoffCounter).$();
            walBackoffUntil = nowMicros + rnd.nextInt(4_000_000);
            return true;
        }
        backoffCounter = 0;
        walBackoffUntil = -1;
        maxParallelism = maxRecordedInflightPartitions / 2;
        maxRecordedInflightPartitions = 1;
        LOG.info().$("Memory pressure building up, new max parallelism=").$(maxParallelism).$();

        return true;
    }

    public void onPressureReduced(long nowMicros) {
        maxRecordedInflightPartitions = 1;
        backoffCounter = 0;
        walBackoffUntil = -1;
        if (maxParallelism == Integer.MAX_VALUE) {
            // fast path
            return;
        }

        int i = rnd.nextInt();
        if (Integer.numberOfTrailingZeros(i) >= 4) { // 1 in 16
            int origMaxParallelism = maxParallelism;
            maxParallelism *= 2;
            if (maxParallelism < origMaxParallelism) {
                // overflow
                maxParallelism = Integer.MAX_VALUE;
            }
        }
        LOG.info().$("Memory pressure easing off, new max parallelism=").$(maxParallelism).$();
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

    public boolean shouldBackOff(long nowMicros) {
        return nowMicros < walBackoffUntil;
    }

    @Override
    public void updateInflightPartitions(int count) {
        maxRecordedInflightPartitions = Math.max(maxRecordedInflightPartitions, count);
    }
}
