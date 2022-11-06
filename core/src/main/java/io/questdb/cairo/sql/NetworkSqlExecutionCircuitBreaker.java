/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.network.NetworkFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.io.Closeable;

public class NetworkSqlExecutionCircuitBreaker implements SqlExecutionCircuitBreaker, Closeable {
    private final NetworkFacade nf;
    private final int throttle;
    private final int bufferSize;
    private final MillisecondClock clock;
    private final long defaultMaxTime;
    private final SqlExecutionCircuitBreakerConfiguration configuration;
    private long timeout;
    private long buffer;
    private int testCount;
    private long fd = -1;
    private long powerUpTime = Long.MAX_VALUE;
    private final int memoryTag;

    public NetworkSqlExecutionCircuitBreaker(SqlExecutionCircuitBreakerConfiguration configuration, int memoryTag) {
        this.configuration = configuration;
        this.nf = configuration.getNetworkFacade();
        this.throttle = configuration.getCircuitBreakerThrottle();
        this.bufferSize = configuration.getBufferSize();
        this.memoryTag = memoryTag;
        this.buffer = Unsafe.malloc(bufferSize, this.memoryTag);
        this.clock = configuration.getClock();
        long timeout = configuration.getTimeout();
        if (timeout > 0) {
            this.timeout = timeout;
        } else if (timeout == TIMEOUT_FAIL_ON_FIRST_CHECK) {
            this.timeout = -1;
        } else {
            this.timeout = Long.MAX_VALUE;
        }
        this.defaultMaxTime = this.timeout;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferSize, this.memoryTag);
        buffer = 0;
        fd = -1;
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public void unsetTimer() {
        powerUpTime = Long.MAX_VALUE;
    }

    @Override
    public boolean isTimerSet() {
        return powerUpTime < Long.MAX_VALUE;
    }

    public void resetMaxTimeToDefault() {
        this.timeout = defaultMaxTime;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
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
        if (testConnection(this.fd)) {
            throw CairoException.nonCritical().put("remote disconnected, query aborted [fd=").put(fd).put(']').setInterruption(true);
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
        return testConnection(fd);
    }

    @Override
    public void setFd(long fd) {
        this.fd = fd;
    }

    @Override
    public void resetTimer() {
        powerUpTime = clock.getTicks();
    }

    public NetworkSqlExecutionCircuitBreaker of(long fd) {
        assert buffer != 0;
        testCount = 0;
        this.fd = fd;
        return this;
    }

    protected boolean testConnection(long fd) {
        assert fd != -1;
        final int nRead = nf.peek(fd, buffer, bufferSize);

        if (nRead == 0) {
            return false;
        }

        if (nRead < 0) {
            return true;
        }

        int index = 0;
        long ptr = buffer;
        while (index < nRead) {
            byte b = Unsafe.getUnsafe().getByte(ptr + index);
            if (b != (byte) '\r' && b != (byte) '\n') {
                break;
            }
            index++;
        }

        if (index > 0) {
            nf.recv(fd, buffer, index);
        }

        return false;
    }

    private void testTimeout() {
        if (clock.getTicks() - timeout > powerUpTime) {
            throw CairoException.nonCritical().put("timeout, query aborted [fd=").put(fd).put(']').setInterruption(true);
        }
    }
}
