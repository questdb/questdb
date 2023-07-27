/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.Unsafe;

public class SeqTxnTracker {
    private static final long SEQ_TXN_OFFSET;
    private static final long SUSPENDED_STATE_OFFSET;
    private static final long WRITER_TXN_OFFSET;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long seqTxn = -1;
    // -1 suspended
    // 0 unknown
    // 1 not suspended
    private volatile int suspendedState = 0;
    private volatile long writerTxn = -1;

    public boolean initTxns(long newWriterTxn, long newSeqTxn, boolean isSuspended) {
        Unsafe.cas(this, SUSPENDED_STATE_OFFSET, 0, isSuspended ? -1 : 1);
        long seqTxn = this.seqTxn;
        while (seqTxn < newSeqTxn && !Unsafe.cas(this, SEQ_TXN_OFFSET, seqTxn, newSeqTxn)) {
            seqTxn = this.seqTxn;
        }
        long writerTxn = this.writerTxn;
        while (writerTxn < this.writerTxn && !Unsafe.cas(this, WRITER_TXN_OFFSET, writerTxn, newWriterTxn)) {
            writerTxn = this.writerTxn;
        }
        return this.suspendedState > 0 && this.seqTxn > this.writerTxn;
    }

    public boolean isInitialised() {
        return writerTxn != -1;
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
                // Return that Apply job notification is needed
                // when there is some new work for ApplyWal2Table job
                return (stxn == -1 || writerTxn == (newSeqTxn - 1)) && suspendedState >= 0;
            }
            stxn = seqTxn;
        }
        return false;
    }

    public void setSuspended() {
        this.suspendedState = -1;
    }

    public void setUnsuspended() {
        this.suspendedState = 1;
    }

    static {
        SEQ_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "seqTxn");
        WRITER_TXN_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "writerTxn");
        SUSPENDED_STATE_OFFSET = Unsafe.getFieldOffset(SeqTxnTracker.class, "suspendedState");
    }
}
