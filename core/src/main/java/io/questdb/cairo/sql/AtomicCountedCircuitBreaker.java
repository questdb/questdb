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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Circuit breaker that doesn't check network connection status or timeout
 * and only allows cancelling statement via CANCEL QUERY command.
 */
public class AtomicCountedCircuitBreaker implements SqlExecutionCircuitBreaker {
    protected volatile @NotNull AtomicBoolean cancelledFlag = new AtomicBoolean(false);
    protected volatile @NotNull AtomicInteger inProgress = new AtomicInteger(0);
    private long fd = -1;
    private int testCount = 0;
    private int throttle;

    public AtomicCountedCircuitBreaker() {
        inProgress = new AtomicInteger(0);
        cancelledFlag = new AtomicBoolean(false);
    }

    public AtomicCountedCircuitBreaker(int throttle) {
        this.throttle = throttle;
        inProgress = new AtomicInteger(0);
        cancelledFlag = new AtomicBoolean(false);
    }

    public void cancel() {
        // This call can be concurrent with the call to setCancelledFlag
        inProgress.set(-1);
        cancelledFlag.set(true);
    }

    @Override
    public boolean checkIfTripped(long millis, long fd) {
        return isCancelled();
    }

    @Override
    public boolean checkIfTripped() {
        return isCancelled();
    }

    public void clear() {
        fd = -1;
        testCount = 0;
    }

    // returns remaining processes to await
    @Override
    public int finish() {
        return inProgress.decrementAndGet();
    }


    @Override
    public @NotNull AtomicBoolean getCancelledFlag() {
        return cancelledFlag;
    }

    @Override
    public @Nullable SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return null;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public int getState() {
        if (isCancelled()) {
            return STATE_CANCELLED;
        }

        int ip = inProgress.get();

        if (ip == 0) {
            return STATE_FINISHED;
        }

        if (ip < 0) {
            return STATE_CANCELLED;
        }

        return STATE_OK;
    }

    @Override
    public int getState(long millis, long fd) {
        return getState();
    }

    @Override
    public long getTimeout() {
        throw new UnsupportedOperationException("AtomicCountedCircuitBreaker does not support timeout");
    }

    public int inc() {
        return inProgress.incrementAndGet();
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public boolean isTimerSet() {
        return true;
    }

    public void reset() {
        cancelledFlag.set(false);
        inProgress.set(0);
    }

    @Override
    public void resetTimer() {
        // ignore
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelledFlag) {
        if (cancelledFlag != null) {
            this.cancelledFlag = cancelledFlag;
        }
    }

    @Override
    public void setFd(long fd) {
        this.fd = fd;
    }

    public void statefulThrowExceptionIfTripped() {
        if (testCount < throttle) {
            testCount++;
        } else {
            statefulThrowExceptionIfTrippedNoThrottle();
        }
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        testCount = 0;
        if (isCancelled()) {
            throw CairoException.queryCancelled(fd);
        }
    }

    @Override
    public void unsetTimer() {
        // ignore
    }

    private boolean isCancelled() {
        return cancelledFlag.get();
    }


}
