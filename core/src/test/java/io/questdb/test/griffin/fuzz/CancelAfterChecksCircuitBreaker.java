/*******************************************************************************
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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Cancels the query that consults it at one of its checks, as a client's cancellation that
 * arrives while the query runs does, and stays cancelled from then on. The breaker counts the
 * checks of every thread that consults it, the query's owner and the workers that run its tasks,
 * which share it, and cancels at the check that follows {@code checksBeforeCancel} others: none
 * cancels at the first check, and a count that the query never reaches leaves it to complete.
 * <p>
 * With tasks on several threads, the check that cancels is the one that the threads' interleaving
 * puts there, so a count names a point in the execution only when the owner runs every task.
 */
public final class CancelAfterChecksCircuitBreaker extends AtomicBooleanCircuitBreaker {
    private final AtomicLong checks = new AtomicLong();
    private final long checksBeforeCancel;

    /**
     * @param checksBeforeCancel the checks that pass before the one that cancels, at least 0
     */
    public CancelAfterChecksCircuitBreaker(CairoEngine engine, long checksBeforeCancel) {
        super(engine, 0);
        assert checksBeforeCancel >= 0;
        this.checksBeforeCancel = checksBeforeCancel;
    }

    @Override
    public boolean checkIfTripped() {
        onCheck();
        return super.checkIfTripped();
    }

    /**
     * The checks that the breaker counted, on any thread.
     */
    public long getChecks() {
        return checks.get();
    }

    @Override
    public int getState() {
        onCheck();
        return super.getState();
    }

    /**
     * True once a check cancelled the query.
     */
    public boolean isTripped() {
        return checks.get() > checksBeforeCancel;
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        onCheck();
        super.statefulThrowExceptionIfTrippedNoThrottle();
    }

    private void onCheck() {
        if (checks.getAndIncrement() == checksBeforeCancel) {
            cancel();
        }
    }
}
