/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public class HttpSqlExecutionInterruptor implements SqlExecutionInterruptor, Closeable {
    private final NetworkFacade nf;
    private final int nIterationsPerCheck;
    private final int bufferSize;
    private long buffer;
    private int nIterationsSinceCheck;
    private long fd = -1;

    public HttpSqlExecutionInterruptor(SqlInterruptorConfiguration configuration) {
        this.nf = configuration.getNetworkFacade();
        this.nIterationsPerCheck = configuration.getCountOfIterationsPerCheck();
        this.bufferSize = configuration.getBufferSize();
        buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
    }

    @Override
    public void checkInterrupted() {
        assert fd != -1;
        if (nIterationsSinceCheck == nIterationsPerCheck) {
            nIterationsSinceCheck = 0;
            checkConnection();
        } else {
            nIterationsSinceCheck++;
        }
    }

    private void checkConnection() {
        int nRead = nf.peek(fd, buffer, bufferSize);
        if (nRead == 0) {
            return;
        }
        if (nRead < 0) {
            throw CairoException.instance(0).put("client fd ").put(fd).put(" is closed").setInterruption(true);
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

    public HttpSqlExecutionInterruptor of(long fd) {
        assert buffer != 0;
        nIterationsSinceCheck = 0;
        this.fd = fd;
        return this;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_HTTP_CONN);
        buffer = 0;
        fd = -1;
    }
}
