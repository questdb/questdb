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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

// Circuit breaker that doesn't check network connection status or timeout and only allows cancelling statement via CANCEL QUERY command .
public class AtomicBooleanCircuitBreaker implements SqlExecutionCircuitBreaker {
    private final int throttle;
    private AtomicBoolean cancelledFlag;
    private int fd = -1;
    private int testCount = 0;

    public AtomicBooleanCircuitBreaker() {
        this(0);
        cancelledFlag = new AtomicBoolean(false);
    }

    public AtomicBooleanCircuitBreaker(int throttle) {
        this.throttle = throttle;
    }

    public void cancel() {
        cancelledFlag.set(true);
    }

    @Override
    public boolean checkIfTripped(long millis, int fd) {
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

    @Override
    public @Nullable SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return null;
    }

    @Override
    public int getFd() {
        return fd;
    }

    @Override
    public int getState() {
        return isCancelled() ? STATE_CANCELLED : STATE_OK;
    }

    @Override
    public int getState(long millis, int fd) {
        return getState();
    }

    @Override
    public boolean isTimerSet() {
        return true;
    }

    public void reset() {
        cancelledFlag.set(false);
    }

    @Override
    public void resetTimer() {
        // ignore
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelledFlag) {
        this.cancelledFlag = cancelledFlag;
    }

    @Override
    public void setFd(int fd) {
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
