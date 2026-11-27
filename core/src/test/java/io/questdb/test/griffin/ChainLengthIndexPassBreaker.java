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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Records the breaker checks of the pass that indexes the chain lengths of a parallel hash join's
 * builds for a count, by the thread that makes each, and on request cancels the query, or runs an
 * action, at the first one. A network connection's breaker is not thread safe, so the workers check
 * copies of it, and so is the owner when it runs a task itself; this breaker is not thread safe
 * either, and reports no timeout, as a network breaker without a query timeout does, so the checks
 * that it records reached it rather than a copy.
 */
class ChainLengthIndexPassBreaker extends AtomicBooleanCircuitBreaker {
    private static final String HEAP = "io.questdb.griffin.engine.join.HashJoinRowHeap";
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private final boolean isCancelling;
    // Runs at the first check of the pass, before the breaker cancels, if it cancels; null for none.
    @Nullable
    private final Runnable onFirstPassCheck;
    private final AtomicInteger passCheckCount = new AtomicInteger();
    private final Set<Thread> passThreads = ConcurrentHashMap.newKeySet();

    ChainLengthIndexPassBreaker(CairoEngine engine, boolean isCancelling, @Nullable Runnable onFirstPassCheck) {
        super(engine);
        this.isCancelling = isCancelling;
        this.onFirstPassCheck = onFirstPassCheck;
    }

    /**
     * The checks that the pass made since construction, on any thread.
     */
    int getPassCheckCount() {
        return passCheckCount.get();
    }

    @Override
    public long getTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * True when the pass made its checks on this thread alone, or made none.
     */
    boolean isPassOnlyOn(Thread thread) {
        return passThreads.isEmpty() || passThreads.size() == 1 && passThreads.contains(thread);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    // The pass checks the breaker through statefulThrowExceptionIfTrippedTimeThrottledOrYield(),
    // which checks it here first.
    @Override
    public void statefulThrowExceptionIfTrippedTimeThrottled() {
        if (isInsidePass()) {
            passThreads.add(Thread.currentThread());
            if (passCheckCount.incrementAndGet() == 1) {
                if (onFirstPassCheck != null) {
                    onFirstPassCheck.run();
                }
                if (isCancelling) {
                    cancel();
                }
            }
        }
        super.statefulThrowExceptionIfTrippedTimeThrottled();
    }

    // True when the check runs inside the pass.
    static boolean isInsidePass() {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(HEAP)
                && frame.getMethodName().equals("indexChainLengths")));
    }
}
