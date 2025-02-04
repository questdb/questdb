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

import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

// This wrapper itself does not provide thread-safety, and that is ok because worker threads own the
// jobs/contexts which hold SqlExecutionCircuitBreakerWrapper objects.
// However, the `delegate` circuit breaker instance referenced by the wrapper has to be thread-safe
// if it is used by multiple threads (i.e. set as a delegate in multiple wrappers at the same time).
public class SqlExecutionCircuitBreakerWrapper implements SqlExecutionCircuitBreaker, Closeable {
    private final State backup = new State();
    private SqlExecutionCircuitBreaker delegate;
    private NetworkSqlExecutionCircuitBreaker networkSqlExecutionCircuitBreaker;

    public SqlExecutionCircuitBreakerWrapper(@NotNull SqlExecutionCircuitBreakerConfiguration configuration) {
        networkSqlExecutionCircuitBreaker = new NetworkSqlExecutionCircuitBreaker(configuration, MemoryTag.NATIVE_CB2);
    }

    /**
     * Backs up current circuit breaker (CB) wrapper state. For an initialized wrapper this means
     * either reference to an atomic CB or a network CB configuration (e.g. socket fd).
     * <p>
     * Intended to be called before init() call and, after these two calls, followed by
     * a {@link #restore()} call to restore the original wrapper state. This is handy in work stealing
     * scenario when doing parallel SQL execution where frame sequence may have to switch between its
     * own tasks, someone else's tasks and back. Without restoring the wrapper state, it may stay
     * initialized with another query's CB leading to data races.
     */
    public void backup() {
        backup.clear();
        if (delegate != null) {
            if (delegate.isThreadSafe()) {
                backup.threadSafeDelegate = delegate;
            } else { // this should have been the network CB
                backup.networkFd = delegate.getFd();
                backup.networkTimeout = delegate.getTimeout();
                backup.networkCancelledFlag = delegate.getCancelledFlag();
            }
            backup.initialized = true;
        }
    }

    @Override
    public void cancel() {
        delegate.cancel();
    }

    @Override
    public boolean checkIfTripped(long millis, long fd) {
        return delegate.checkIfTripped(millis, fd);
    }

    @Override
    public boolean checkIfTripped() {
        return delegate.checkIfTripped();
    }

    @Override
    public void close() {
        networkSqlExecutionCircuitBreaker = Misc.free(networkSqlExecutionCircuitBreaker);
        delegate = null;
    }

    @Override
    public AtomicBoolean getCancelledFlag() {
        return delegate.getCancelledFlag();
    }

    @Override
    public @Nullable SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return delegate.getConfiguration();
    }

    @TestOnly
    public SqlExecutionCircuitBreaker getDelegate() {
        return delegate;
    }

    @Override
    public long getFd() {
        return delegate.getFd();
    }

    @Override
    public int getState() {
        return delegate.getState();
    }

    @Override
    public int getState(long millis, long fd) {
        return delegate.getState(millis, fd);
    }

    @Override
    public long getTimeout() {
        return delegate.getTimeout();
    }

    public void init(SqlExecutionCircuitBreakerWrapper wrapper) {
        init(wrapper.delegate);
    }

    public void init(SqlExecutionCircuitBreaker executionContextCircuitBreaker) {
        if (executionContextCircuitBreaker.isThreadSafe()) {
            delegate = executionContextCircuitBreaker;
        } else {
            networkSqlExecutionCircuitBreaker.of(executionContextCircuitBreaker.getFd());
            networkSqlExecutionCircuitBreaker.setTimeout(executionContextCircuitBreaker.getTimeout());
            networkSqlExecutionCircuitBreaker.resetTimer();
            networkSqlExecutionCircuitBreaker.setCancelledFlag(executionContextCircuitBreaker.getCancelledFlag());
            delegate = networkSqlExecutionCircuitBreaker;
        }
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

    /**
     * Should be called after {@link #backup()} to restore the original wrapper state.
     */
    public void restore() {
        if (backup.initialized) {
            if (backup.threadSafeDelegate != null) {
                delegate = backup.threadSafeDelegate;
            } else {
                networkSqlExecutionCircuitBreaker.of(backup.networkFd);
                networkSqlExecutionCircuitBreaker.setTimeout(backup.networkTimeout);
                networkSqlExecutionCircuitBreaker.resetTimer();
                networkSqlExecutionCircuitBreaker.setCancelledFlag(backup.networkCancelledFlag);
                delegate = networkSqlExecutionCircuitBreaker;
            }
        } else {
            delegate = null;
        }
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelled) {
        delegate.setCancelledFlag(cancelled);
    }

    @Override
    public void setFd(long fd) {
        delegate.setFd(fd);
    }

    @Override
    public void statefulThrowExceptionIfTripped() {
        delegate.statefulThrowExceptionIfTripped();
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        delegate.statefulThrowExceptionIfTrippedNoThrottle();
    }

    @Override
    public void unsetTimer() {
        delegate.unsetTimer();
    }

    private static class State implements Mutable {
        boolean initialized; // true if delegate was initialized at the time of last backup
        AtomicBoolean networkCancelledFlag;
        long networkFd = -1;
        long networkTimeout;
        SqlExecutionCircuitBreaker threadSafeDelegate;

        @Override
        public void clear() {
            initialized = false;
            threadSafeDelegate = null;
            networkFd = -1;
            networkTimeout = 0;
            networkCancelledFlag = null;
        }
    }
}
