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

import io.questdb.mp.ContinuationResumeJob;
import io.questdb.mp.SqlContinuation;
import io.questdb.std.Unsafe;

/**
 * Represents a single SQL evaluation parked inside a {@link SeqTxnTracker}, waiting for
 * the tracker's {@code writerTxn} to reach {@link #targetWriterTxn}.
 *
 * <p>Instances are pooled: typically one per {@link io.questdb.griffin.SqlExecutionContext}.
 * {@link #reset} clears the state back to PENDING and publishes fresh target / continuation
 * / resume-job / deadline fields. Since PGWire serializes queries per connection and a
 * single {@code wait_wal_table} invocation is sequential within a query, only one wait is
 * in flight per context at a time — the pooled instance is safe to reuse across waits.
 *
 * <p>The {@link #state} field is a CAS'd 3-way marker:
 * <ul>
 *     <li>{@link #STATE_PENDING} — waiter is registered and has not been handed off</li>
 *     <li>{@link #STATE_FIRED} — a fireWaiters pass transitioned this waiter to ready
 *     and has enqueued the continuation for resume</li>
 *     <li>{@link #STATE_CANCELLED} — the waiter was cancelled (timeout, client
 *     disconnect); on resume the body reads this and throws</li>
 * </ul>
 * Only one concurrent thread wins the CAS from PENDING to FIRED or CANCELLED, so the
 * continuation is enqueued at most once.
 */
public final class TxnWaiter {
    public static final long NO_DEADLINE = Long.MAX_VALUE;
    public static final int STATE_CANCELLED = 2;
    public static final int STATE_FIRED = 1;
    public static final int STATE_PENDING = 0;
    static final long STATE_OFFSET = Unsafe.getFieldOffset(TxnWaiter.class, "state");
    SqlContinuation cont;
    long deadlineMillis = NO_DEADLINE;
    ContinuationResumeJob resumeJob;
    volatile int state = STATE_PENDING;
    long targetWriterTxn;

    public TxnWaiter() {
    }

    public TxnWaiter(long targetWriterTxn, SqlContinuation cont, ContinuationResumeJob resumeJob) {
        this(targetWriterTxn, cont, resumeJob, NO_DEADLINE);
    }

    public TxnWaiter(long targetWriterTxn, SqlContinuation cont, ContinuationResumeJob resumeJob, long deadlineMillis) {
        this.targetWriterTxn = targetWriterTxn;
        this.cont = cont;
        this.resumeJob = resumeJob;
        this.deadlineMillis = deadlineMillis;
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
    public void reset(long targetWriterTxn, SqlContinuation cont, ContinuationResumeJob resumeJob, long deadlineMillis) {
        this.targetWriterTxn = targetWriterTxn;
        this.cont = cont;
        this.resumeJob = resumeJob;
        this.deadlineMillis = deadlineMillis;
        this.state = STATE_PENDING;
    }

    /**
     * Attempts to transition this waiter from PENDING to CANCELLED. Returns {@code true}
     * if the CAS won, in which case the caller is responsible for enqueueing this
     * waiter's continuation on the resume job so the parked body can observe the
     * cancellation and throw.
     */
    public boolean tryCancel() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED);
    }

    public boolean tryFire() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED);
    }
}
