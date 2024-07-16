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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

public class SeqTxnTracker {
    private static final Log LOG = LogFactory.getLog(SeqTxnTracker.class);
    private static final int MAX_MEM_PRESSURE_LEVEL = 10;
    private static final int MEM_PRESSURE_THRESHOLD_TO_START_BACKOFF = 5; // when exceeded we start introducing back off
    private static final int MIN_MEM_PRESSURE_LEVEL = 0;
    private static final long SEQ_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "seqTxn");
    private static final long SUSPENDED_STATE_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "suspendedState");
    private static final long WRITER_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "writerTxn");
    private final Rnd rnd;
    private volatile String errorMessage = "";
    private volatile ErrorTag errorTag = ErrorTag.NONE;
    private int memPressureLevel;
    private volatile int memoryPressureLevel;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long seqTxn = -1;
    // -1 suspended
    // 0 unknown
    // 1 not suspended
    private volatile int suspendedState = 0;
    private volatile long walBackoffUntil = -1;
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
        if (memPressureLevel == 0) {
            // fast path
            return Integer.MAX_VALUE;
        }

        int adjustedLevel = Math.min(memPressureLevel, MEM_PRESSURE_THRESHOLD_TO_START_BACKOFF) * 10;
        int partitionParallelism = 51 - adjustedLevel;
        assert partitionParallelism > 0;
        // level 1 = parallelism 41
        // level 2 = parallelism 31
        // level 3 = parallelism 21
        // level 4 = parallelism 11
        // level 5 = parallelism 1
        // level 6 = parallelism 1 + backoff
        // level 7 = parallelism 1 + backoff
        // level 8 = parallelism 1 + backoff
        // level 9 = parallelism 1 + backoff
        // level 10 = parallelism 1 + backoff
        return partitionParallelism;
    }

    @TestOnly
    public int getMemPressureLevel() {
        return memPressureLevel;
    }

    public int getMemoryPressureLevel() {
        return memoryPressureLevel;
    }

    @TestOnly
    public long getSeqTxn() {
        return seqTxn;
    }

    public long getWalBackoffUntil() {
        return walBackoffUntil;
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
        return writerTxn < seqTxn && suspendedState > 0;
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
        // we might consider more aggressive increase in the future to quickly reduce parallelism
        // when memory pressure is high. For now, we are conservative. It might take multiple cycles
        // to find the right level.

        if (memPressureLevel == MAX_MEM_PRESSURE_LEVEL) {
            // we are already at max level, we can't increase it further
            // return false to indicate that we can't retry the operation
            // this will likely suspend the table
            adjustWalBackoff(nowMicros);
            return false;
        }
        memPressureLevel++;
        setMemoryPressureLevel(memPressureLevel);
        adjustWalBackoff(nowMicros);
        LOG.infoW().$("Memory pressure building up, new level=").$(memPressureLevel).$();
        return true;
    }

    public void onPressureReduced(long nowMicros) {
        if (memPressureLevel == MIN_MEM_PRESSURE_LEVEL) {
            // fast path
            return;
        }

        if (memPressureLevel > MEM_PRESSURE_THRESHOLD_TO_START_BACKOFF) {
            memPressureLevel--;
            adjustWalBackoff(nowMicros);
        } else {
            // we increase parallelism probabilistically - the more pressure the lesser chance of increasing it
            int minTrailingZeros = (memPressureLevel + 2);
            // if memoryPressure == 1  we require 3 trailing zeros -> that's 1 in 8 chance
            // if memoryPressure == 2  we require 4 trailing zeros -> that's 1 in 16 chance
            // ...
            // if memoryPressure == 5 we require 7 trailing zeros -> that's 1 in 128 chance
            // yes, we are assuming that random number generator is reasonably good
            int i = rnd.nextInt();
            if (Integer.numberOfTrailingZeros(i) >= minTrailingZeros) {
                memPressureLevel--;
            }
        }
        LOG.infoW().$("Memory pressure easing off for table, new level=").$(memPressureLevel).$();
    }

    public void setMemoryPressureLevel(int memoryPressureLevel) {
        this.memoryPressureLevel = memoryPressureLevel;
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

    public void setWalBackoffUntil(long walBackoffUntil) {
        this.walBackoffUntil = walBackoffUntil;
    }

    public boolean shouldBackOff(long nowMicros) {
        return nowMicros < getWalBackoffUntil();
    }

    private void adjustWalBackoff(long nowMicros) {
        if (memPressureLevel > 5) {
            int backoffMillis = (1 << (memPressureLevel + 3));
            // level 6 -> 512 ms
            // level 7 -> 1024 ms
            // level 8 -> 2048 ms
            // level 9 -> 4096 ms
            // level 10 -> 8192 ms
            long backoffMicros = backoffMillis * 1_000L;
            setWalBackoffUntil(nowMicros + backoffMicros);
        } else {
            // we reduce parallelism, that's already aggressive enough
            // so no back off
            setWalBackoffUntil(-1);
        }
    }

}
