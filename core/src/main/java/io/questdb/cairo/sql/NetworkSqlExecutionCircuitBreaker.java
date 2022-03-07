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
import io.questdb.griffin.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;

import java.io.Closeable;

public class NetworkSqlExecutionCircuitBreaker implements SqlExecutionCircuitBreaker, Closeable {
    private final NetworkFacade nf;
    private final int throttle;
    private final int bufferSize;
    private long buffer;
    private int testCount;
    private long fd = -1;
    private long powerDownDeadline;
    private final MicrosecondClock clock;
    private final long maxTime;

    public NetworkSqlExecutionCircuitBreaker(SqlExecutionCircuitBreakerConfiguration configuration) {
        this.nf = configuration.getNetworkFacade();
        this.throttle = configuration.getCircuitBreakerThrottle();
        this.bufferSize = configuration.getBufferSize();
        this.buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        this.clock = configuration.getClock();
        this.maxTime = configuration.getMaxTime();
    }

    @Override
    public void setState() {
        final long ticks = clock.getTicks();
        // test for overflow
        if ((maxTime > 0) && (ticks > Long.MAX_VALUE - maxTime)) {
            powerDownDeadline = Long.MAX_VALUE;
        } else {
            powerDownDeadline = ticks + maxTime;
        }
    }

    private void testTimeout() {
        if (powerDownDeadline < clock.getTicks()) {
            powerDownDeadline = Long.MIN_VALUE;
            throw CairoException.instance(0).put("timeout, query aborted [fd=").put(fd).put(']').setInterruption(true);
        }
    }

    @Override
    public void statefulThrowExceptionWhenTripped() {
        if (testCount < throttle) {
            testCount++;
        } else {
            testCount = 0;
            testTimeout();
            testConnection();
        }
    }

    private void testConnection() {
        assert fd != -1;
        int nRead = nf.peek(fd, buffer, bufferSize);
        if (nRead == 0) {
            return;
        }
        if (nRead < 0) {
            throw CairoException.instance(0).put("remote disconnected, query aborted [fd=").put(fd).put(']').setInterruption(true);
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
    }

    public NetworkSqlExecutionCircuitBreaker of(long fd) {
        assert buffer != 0;
        testCount = 0;
        this.fd = fd;
        return this;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        buffer = 0;
        fd = -1;
    }
}
