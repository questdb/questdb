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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A delegating {@link SqlExecutionCircuitBreaker} that counts how many times execution code
 * consults the breaker, without changing its behavior. A consultation is a call to any of
 * {@link #checkIfTripped()}, {@link #checkIfTripped(long, long)},
 * {@link #checkIfTrippedNoThrottle()}, {@link #getState()}, {@link #getState(long, long)},
 * {@link #statefulThrowExceptionIfTripped()},
 * {@link #statefulThrowExceptionIfTrippedNoThrottle()} or
 * {@link #statefulThrowExceptionIfTrippedTimeThrottled()}; all other methods delegate without
 * counting. The counter is thread-safe: parallel execution shares a thread-safe execution-context
 * circuit breaker across worker threads, so the wrapper may be consulted concurrently.
 * <p>
 * {@code QueryAssertion.expectCircuitBreakerChecks()} installs this wrapper around the execution
 * context's circuit breaker to assert that the cursor under test honors the breaker, i.e. that the
 * query is cancellable.
 */
public class CountingSqlExecutionCircuitBreaker implements SqlExecutionCircuitBreaker {
    private final AtomicLong checkCount = new AtomicLong();
    private final SqlExecutionCircuitBreaker delegate;

    public CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker delegate) {
        this.delegate = delegate;
    }

    @Override
    public void cancel() {
        delegate.cancel();
    }

    @Override
    public boolean checkIfTripped() {
        checkCount.incrementAndGet();
        return delegate.checkIfTripped();
    }

    @Override
    public boolean checkIfTripped(long millis, long fd) {
        checkCount.incrementAndGet();
        return delegate.checkIfTripped(millis, fd);
    }

    @Override
    public boolean checkIfTrippedNoThrottle() {
        checkCount.incrementAndGet();
        return delegate.checkIfTrippedNoThrottle();
    }

    @Override
    public AtomicBoolean getCancelledFlag() {
        return delegate.getCancelledFlag();
    }

    /**
     * Number of times any of the counted check methods has been called since construction.
     */
    public long getCheckCount() {
        return checkCount.get();
    }

    @Override
    public @Nullable SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return delegate.getConfiguration();
    }

    public SqlExecutionCircuitBreaker getDelegate() {
        return delegate;
    }

    @Override
    public long getFd() {
        return delegate.getFd();
    }

    @Override
    public int getState() {
        checkCount.incrementAndGet();
        return delegate.getState();
    }

    @Override
    public int getState(long millis, long fd) {
        checkCount.incrementAndGet();
        return delegate.getState(millis, fd);
    }

    @Override
    public long getTimeout() {
        return delegate.getTimeout();
    }

    @Override
    public boolean isThreadSafe() {
        return delegate.isThreadSafe();
    }

    @Override
    public boolean isTimerSet() {
        return delegate.isTimerSet();
    }

    @Override
    public void resetTimer() {
        delegate.resetTimer();
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelledFlag) {
        delegate.setCancelledFlag(cancelledFlag);
    }

    @Override
    public void setFd(long fd) {
        delegate.setFd(fd);
    }

    @Override
    public void statefulThrowExceptionIfTripped() {
        checkCount.incrementAndGet();
        delegate.statefulThrowExceptionIfTripped();
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        checkCount.incrementAndGet();
        delegate.statefulThrowExceptionIfTrippedNoThrottle();
    }

    @Override
    public void statefulThrowExceptionIfTrippedTimeThrottled() {
        checkCount.incrementAndGet();
        delegate.statefulThrowExceptionIfTrippedTimeThrottled();
    }

    @Override
    public void unsetTimer() {
        delegate.unsetTimer();
    }
}
