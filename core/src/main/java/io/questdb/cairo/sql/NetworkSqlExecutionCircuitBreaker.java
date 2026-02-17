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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class NetworkSqlExecutionCircuitBreaker implements SqlExecutionCircuitBreaker, Closeable, Mutable {
    private final int bufferSize;
    private final MillisecondClock clock;
    private final SqlExecutionCircuitBreakerConfiguration configuration;
    private final long defaultMaxTime;
    private final CairoEngine engine;
    private final int memoryTag;
    private final NetworkFacade nf;
    private final int throttle;
    private long buffer;
    private volatile AtomicBoolean cancelledFlag;
    private long fd = -1;
    private volatile long powerUpTime = Long.MAX_VALUE;
    private int secret;
    private int testCount;
    private long timeout;

    public NetworkSqlExecutionCircuitBreaker(CairoEngine engine, @NotNull SqlExecutionCircuitBreakerConfiguration configuration, int memoryTag) {
        this.configuration = configuration;
        this.nf = configuration.getNetworkFacade();
        this.throttle = configuration.getCircuitBreakerThrottle();
        this.bufferSize = configuration.getBufferSize();
        this.memoryTag = memoryTag;
        this.buffer = Unsafe.malloc(this.bufferSize, this.memoryTag);
        this.clock = configuration.getClock();
        long timeout = configuration.getQueryTimeout();
        if (timeout > 0) {
            this.timeout = timeout;
        } else if (timeout == TIMEOUT_FAIL_ON_FIRST_CHECK) {
            this.timeout = -100;
        } else {
            this.timeout = Long.MAX_VALUE;
        }
        this.defaultMaxTime = this.timeout;
        this.engine = engine;
    }

    @Override
    public void cancel() {
        powerUpTime = Long.MIN_VALUE;
        // This call can be concurrent with the call to setCancelledFlag
        AtomicBoolean cf = cancelledFlag;
        if (cf != null) {
            cf.set(true);
        }
    }

    @Override
    public boolean checkIfTripped() {
        return checkIfTripped(powerUpTime, fd);
    }

    @Override
    public boolean checkIfTripped(long millis, long fd) {
        if (clock.getTicks() - timeout > millis) {
            return true;
        }
        if ((cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            return true;
        }
        return testConnection(fd);
    }

    public void clear() {
        secret = -1;
        powerUpTime = Long.MAX_VALUE;
        testCount = 0;
        fd = -1;
        timeout = defaultMaxTime;
    }

    @Override
    public void close() {
        buffer = Unsafe.free(buffer, bufferSize, memoryTag);
        fd = -1;
    }

    @Override
    public AtomicBoolean getCancelledFlag() {
        return cancelledFlag;
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return configuration;
    }

    public long getDefaultMaxTime() {
        return defaultMaxTime;
    }

    @Override
    public long getFd() {
        return fd;
    }

    public int getSecret() {
        return secret;
    }

    @Override
    public int getState() {
        return getState(powerUpTime, fd);
    }

    @Override
    public int getState(long millis, long fd) {
        if (clock.getTicks() - timeout > millis) {
            return STATE_TIMEOUT;
        }
        if ((cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            return STATE_CANCELLED;
        }
        if (testConnection(fd)) {
            return STATE_BROKEN_CONNECTION;
        }
        return STATE_OK;
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public boolean isTimerSet() {
        return powerUpTime < Long.MAX_VALUE;
    }

    public NetworkSqlExecutionCircuitBreaker of(long fd) {
        assert buffer != 0;
        testCount = 0;
        this.fd = fd;
        return this;
    }

    public void resetMaxTimeToDefault() {
        this.timeout = defaultMaxTime;
    }

    @Override
    public void resetTimer() {
        powerUpTime = clock.getTicks();
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelledFlag) {
        this.cancelledFlag = cancelledFlag;
    }

    @Override
    public void setFd(long fd) {
        this.fd = fd;
    }

    public void setSecret(int secret) {
        this.secret = secret;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void statefulThrowExceptionIfTimeout() {
        // Same as statefulThrowExceptionIfTripped but does not check the connection state.
        // Useful to check timeout before trying to send something on the connection.
        if (testCount < throttle) {
            testCount++;
        } else {
            testCount = 0;
            testTimeout();
        }
    }

    @Override
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
        testTimeout();
        testCancelled();
        if (testConnection(fd)) {
            throw CairoException.nonCritical().put("remote disconnected, query aborted [fd=").put(fd).put(']').setInterruption(true);
        }
    }

    @Override
    public void unsetTimer() {
        powerUpTime = Long.MAX_VALUE;
    }

    private boolean isCancelled() {
        return powerUpTime == Long.MIN_VALUE;
    }

    private void testCancelled() {
        if ((cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            throw CairoException.queryCancelled(fd);
        }
    }

    private void testTimeout() {
        long runtime = clock.getTicks() - powerUpTime;
        if (runtime > timeout) {
            if (isCancelled()) {
                throw CairoException.queryCancelled(fd);
            } else {
                throw CairoException.queryTimedOut(fd, runtime, timeout);
            }
        }
    }

    protected boolean testConnection(long fd) {
        if (fd == -1 || !configuration.checkConnection()) {
            return false;
        }
        return nf.testConnection(fd, buffer, bufferSize);
    }
}
