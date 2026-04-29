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

import io.questdb.mp.WorkerContinuation;
import io.questdb.std.Unsafe;

/**
 * Represents a single SQL evaluation parked inside a {@link SeqTxnTracker}, waiting for
 * the tracker's {@code writerTxn} to reach {@link #targetWriterTxn}.
 *
 * <p>Instances are pooled: typically one per {@link io.questdb.griffin.SqlExecutionContext}.
 * {@link #reset} clears the state back to PENDING and publishes fresh target / continuation
 * / deadline fields. Since PGWire serializes queries per connection and a single
 * {@code wait_wal_table} invocation is sequential within a query, only one wait is in
 * flight per context at a time -- the pooled instance is safe to reuse across waits.
 *
 * <p>The {@link #state} field is a CAS'd 3-way marker. Only one concurrent thread wins
 * the CAS from PENDING to FIRED or CANCELLED, so {@code cont.scheduleResume()} is invoked
 * at most once per wait.
 *
 * <p>The continuation knows its origin pool's resume sink at construction; firing/cancelling
 * therefore does not need to thread a resume job through the waiter.
 */
public final class TxnWaiter {
    public static final long NO_DEADLINE = Long.MAX_VALUE;
    public static final int STATE_CANCELLED = 2;
    public static final int STATE_FIRED = 1;
    public static final int STATE_PENDING = 0;
    static final long STATE_OFFSET = Unsafe.getFieldOffset(TxnWaiter.class, "state");
    long deadlineMillis = NO_DEADLINE;
    volatile int state = STATE_PENDING;
    long targetWriterTxn;
    private WorkerContinuation cont;

    public TxnWaiter() {
    }

    public TxnWaiter(long targetWriterTxn, WorkerContinuation cont) {
        this(targetWriterTxn, cont, NO_DEADLINE);
    }

    public TxnWaiter(long targetWriterTxn, WorkerContinuation cont, long deadlineMillis) {
        this.targetWriterTxn = targetWriterTxn;
        this.cont = cont;
        this.deadlineMillis = deadlineMillis;
    }

    public WorkerContinuation getContinuation() {
        return cont;
    }

    public boolean isCancelled() {
        return state == STATE_CANCELLED;
    }

    public boolean isExpired(long nowMillis) {
        return deadlineMillis != NO_DEADLINE && nowMillis >= deadlineMillis;
    }

    public boolean isFired() {
        return state == STATE_FIRED;
    }

    /**
     * Resets this pooled waiter for reuse. Publishes the new fields and clears
     * {@link #state} back to PENDING; the volatile write on {@code state} synchronizes
     * the reset so any observer that reads state == PENDING sees the refreshed fields.
     */
    public void reset(long targetWriterTxn, WorkerContinuation cont, long deadlineMillis) {
        this.targetWriterTxn = targetWriterTxn;
        this.cont = cont;
        this.deadlineMillis = deadlineMillis;
        this.state = STATE_PENDING;
    }

    /**
     * Attempts to transition this waiter from PENDING to CANCELLED. Returns {@code true}
     * if the CAS won, in which case the caller is responsible for calling
     * {@code cont.scheduleResume()} so the parked body can observe the cancellation.
     */
    public boolean tryCancel() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED);
    }

    public boolean tryFire() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED);
    }
}
