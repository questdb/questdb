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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

/**
 * Owner-side scheduling decisions of a parallel publish-and-drain phase. Call
 * {@link #observeProgress()} at the top of every wait iteration, before the done check.
 */
public final class QueryParallelOwnerLoop {
    private SqlExecutionCircuitBreaker circuitBreaker;
    private @Nullable QueryParallelFiberDispatcher dispatcher;
    private boolean hasPublication;
    private boolean isFiberOwner;
    private boolean isOwnerParkable;
    private long lastOwnerYieldNanos;
    private long observedGlobalProgress;
    private long observedProgress;
    private AsyncQueryProgressState progressState;

    // returns false when the owner cannot park and must spin or steal instead
    public boolean awaitProgress() {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        if (!isOwnerParkable) {
            return false;
        }
        assert dispatcher != null;
        if (!dispatcher.awaitProgress(progressState, observedProgress, observedGlobalProgress, circuitBreaker)) {
            Os.pause();
        }
        return true;
    }

    // returns false when the owner cannot park and must spin or steal instead
    public boolean awaitProgressWhileDraining(boolean isOwnerTripped) {
        if (!isOwnerParkable) {
            return false;
        }
        assert dispatcher != null;
        if (!dispatcher.awaitProgressWhileDraining(
                progressState,
                observedProgress,
                observedGlobalProgress,
                isOwnerTripped ? null : circuitBreaker
        )) {
            Os.pause();
        }
        return true;
    }

    // without a publication permit the owner cooperates whenever it is a Fiber; with one, only
    // while the dispatcher would also let it park
    public void checkBeforeHelping() {
        checkBeforeHelping(hasPublication ? isOwnerParkable : isFiberOwner, false);
    }

    public void checkBeforeHelpingNoThrottle() {
        checkBeforeHelping(isFiberOwner, true);
    }

    public boolean hasPublication() {
        return hasPublication;
    }

    public boolean isOwnerParkable() {
        return isOwnerParkable;
    }

    public void observeProgress() {
        observedProgress = progressState.getVersion();
        observedGlobalProgress = dispatcher != null ? dispatcher.getProgressVersion() : 0;
        isOwnerParkable = dispatcher != null && dispatcher.isOwnerParkable();
    }

    public void of(
            @Nullable QueryParallelFiberDispatcher dispatcher,
            SqlExecutionCircuitBreaker circuitBreaker,
            AsyncQueryProgressState progressState
    ) {
        this.dispatcher = dispatcher;
        this.circuitBreaker = circuitBreaker;
        this.progressState = progressState;
        this.hasPublication = false;
        this.isFiberOwner = dispatcher != null && QueryParallelFiberDispatcher.isFiberOwner();
        this.isOwnerParkable = false;
        this.lastOwnerYieldNanos = QueryParallelFiberDispatcher.OWNER_YIELD_UNSET;
    }

    public void releasePublication() {
        if (dispatcher != null && hasPublication) {
            hasPublication = false;
            dispatcher.releasePublication();
        }
    }

    public void tryAcquirePublication() {
        hasPublication = dispatcher == null || dispatcher.tryAcquirePublication();
    }

    private void checkBeforeHelping(boolean isCooperative, boolean isUnthrottled) {
        if (isCooperative) {
            assert dispatcher != null;
            if (isUnthrottled) {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            } else {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            lastOwnerYieldNanos = dispatcher.cooperateFiberOwner(lastOwnerYieldNanos);
        } else if (isUnthrottled) {
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
        } else {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        }
    }
}
