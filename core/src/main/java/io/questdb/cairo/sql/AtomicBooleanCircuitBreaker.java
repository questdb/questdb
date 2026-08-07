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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.mp.continuation.CancellationBinding;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Circuit breaker that doesn't check network connection status or timeout
 * and only allows cancelling statement via CANCEL QUERY command.
 */
public class AtomicBooleanCircuitBreaker implements SqlExecutionCircuitBreaker {
    @Deprecated
    protected volatile AtomicBoolean cancelledFlag = new AtomicBoolean(false);
    private final CancellationBinding cancellationBinding;
    private final CairoEngine engine;
    private final int throttle;
    private long fd = -1;
    private int testCount = 0;

    public AtomicBooleanCircuitBreaker(CairoEngine engine) {
        this(engine, 0);
    }

    public AtomicBooleanCircuitBreaker(CairoEngine engine, int throttle) {
        this.cancellationBinding = new CancellationBinding(cancelledFlag);
        this.engine = engine;
        this.throttle = throttle;
    }

    public synchronized void cancel() {
        cancellationBinding.cancel();
    }

    @Override
    public synchronized void clearCancelledFlag(AtomicBoolean expected) {
        cancellationBinding.clear(expected);
        cancelledFlag = cancellationBinding.getFlag();
    }

    @Override
    public synchronized void clearCancelledFlag(AtomicBoolean expected, long expectedGeneration) {
        cancellationBinding.clear(expected, expectedGeneration);
        cancelledFlag = cancellationBinding.getFlag();
    }

    @Override
    public void copyCancelledFlagTo(CancellationBinding target) {
        cancellationBinding.copyTo(target);
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

    @Override
    public AtomicBoolean getCancelledFlag() {
        return cancellationBinding.getFlag();
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
        return isCancelled() ? STATE_CANCELLED : STATE_OK;
    }

    @Override
    public int getState(long millis, long fd) {
        return getState();
    }

    @Override
    public long getTimeout() {
        throw new UnsupportedOperationException("AtomicBooleanCircuitBreaker does not support timeout");
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public boolean isTimerSet() {
        return true;
    }

    public synchronized void reset() {
        cancellationBinding.reset();
    }

    @Override
    public void resetTimer() {
        // No timer to reset, but start a fresh throttle window for the new query so the next breaker
        // consultation performs a real cancellation check.
        testCount = 0;
    }

    @Override
    public synchronized void setCancelledFlag(AtomicBoolean cancelledFlag) {
        cancellationBinding.set(cancelledFlag);
        this.cancelledFlag = cancelledFlag;
    }

    @Override
    public synchronized void setCancelledFlag(CancellationBinding source) {
        source.copyTo(cancellationBinding);
        cancelledFlag = cancellationBinding.getFlag();
    }

    @Override
    public synchronized void setCancelledFlag(AtomicBoolean cancelledFlag, long generation) {
        cancellationBinding.set(cancelledFlag, generation);
        this.cancelledFlag = cancelledFlag;
    }

    @Override
    public void setFd(long fd) {
        this.fd = fd;
    }

    public void statefulThrowExceptionIfTripped() {
        // Always perform a real check on the first call after a reset (testCount == 0), so empty/instant
        // queries that consult the breaker only a handful of times still observe cancellation. Otherwise
        // test once per throttle window to keep hot per-row/per-frame loops cheap.
        if (testCount == 0 || testCount >= throttle) {
            statefulThrowExceptionIfTrippedNoThrottle(); // performs the real test and resets testCount to 0
        }
        testCount++;
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
        return cancellationBinding.isCancelledOrUnbound() || engine.isClosing();
    }
}
