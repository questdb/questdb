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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.LineChannel;
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

public class StringChannel implements LineChannel {
    private static final int BUFFER_SIZE = 1024;

    private final byte[] buffer = new byte[BUFFER_SIZE];
    private int pos;


    @Override
    public void close() {
        //empty
    }

    @Override
    public int errno() {
        throw new UnsupportedOperationException("errno() not implemented");
    }

    @Override
    public int receive(long ptr, int len) {
        throw new UnsupportedOperationException("receive() not implemented");
    }

    public void reset() {
        pos = 0;
    }

    @Override
    public void send(long ptr, int len) {
        int remainingCapacity = BUFFER_SIZE - pos;
        if (len > remainingCapacity) {
            throw new IllegalStateException("remaining capacity: " + remainingCapacity + ", request: " + len);
        }
        for (int i = 0; i < len; i++) {
            buffer[i + pos] = Unsafe.getUnsafe().getByte(ptr + i);
        }
        pos += len;
    }

    @Override
    public String toString() {
        return new String(buffer, 0, pos, StandardCharsets.UTF_8);
    }
}
