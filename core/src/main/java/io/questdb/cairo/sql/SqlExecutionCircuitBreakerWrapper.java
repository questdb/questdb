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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

// does not have to be thread-safe, because worker threads own the jobs/contexts
// which hold SqlExecutionCircuitBreakerWrapper objects
public class SqlExecutionCircuitBreakerWrapper implements SqlExecutionCircuitBreaker, Closeable {
    private SqlExecutionCircuitBreaker delegate;
    private NetworkSqlExecutionCircuitBreaker networkSqlExecutionCircuitBreaker;

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
    public void close() throws IOException {
        delegate = Misc.free(networkSqlExecutionCircuitBreaker);
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

    public void init(SqlExecutionCircuitBreakerWrapper wrapper) {
        init(wrapper.delegate);
    }

    public void init(SqlExecutionCircuitBreaker executionContextCircuitBreaker) {
        final SqlExecutionCircuitBreakerConfiguration sqlExecutionCircuitBreakerConfiguration = executionContextCircuitBreaker.getConfiguration();
        if (sqlExecutionCircuitBreakerConfiguration != null) {
            if (networkSqlExecutionCircuitBreaker == null) {
                networkSqlExecutionCircuitBreaker = new NetworkSqlExecutionCircuitBreaker(sqlExecutionCircuitBreakerConfiguration, MemoryTag.NATIVE_CB2);
            } else {
                networkSqlExecutionCircuitBreaker.resetTimer();
            }
            delegate = networkSqlExecutionCircuitBreaker;
        } else {
            delegate = executionContextCircuitBreaker;
        }
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
}