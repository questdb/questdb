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

package io.questdb.test.tools;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A circuit breaker that counts the checks made from inside one method, and at a given one of
 * them runs a hook and, optionally, cancels the query: a test can stop an execution at a chosen
 * point of a chosen step, such as the first check of a merge, without timing.
 */
public class MethodCheckCircuitBreaker extends AtomicBooleanCircuitBreaker {
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private final AtomicInteger checks = new AtomicInteger();
    private final String className;
    private final boolean isCancelling;
    private final String methodName;
    private final int reachedCheck;
    // Runs at the given check, before the breaker cancels the query there.
    private volatile Runnable onReached;

    public MethodCheckCircuitBreaker(CairoEngine engine, String className, String methodName, int reachedCheck, boolean isCancelling) {
        super(engine);
        this.className = className;
        this.methodName = methodName;
        this.reachedCheck = reachedCheck;
        this.isCancelling = isCancelling;
    }

    @Override
    public boolean checkIfTripped() {
        onCheck();
        return super.checkIfTripped();
    }

    public int getChecks() {
        return checks.get();
    }

    @Override
    public int getState() {
        onCheck();
        return super.getState();
    }

    public void setOnReached(Runnable onReached) {
        this.onReached = onReached;
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        onCheck();
        super.statefulThrowExceptionIfTrippedNoThrottle();
    }

    private boolean isInside() {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className)
                && frame.getMethodName().equals(methodName)));
    }

    private void onCheck() {
        if (isInside() && checks.incrementAndGet() == reachedCheck) {
            final Runnable hook = onReached;
            if (hook != null) {
                hook.run();
            }
            if (isCancelling) {
                cancel();
            }
        }
    }
}
