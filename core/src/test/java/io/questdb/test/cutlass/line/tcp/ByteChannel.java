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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.client.cutlass.line.LineChannel;
import io.questdb.std.Unsafe;

import java.util.Base64;

@SuppressWarnings("unused")
public class ByteChannel implements LineChannel {
    private static final int BUFFER_SIZE = 1024;

    private final byte[] buffer = new byte[BUFFER_SIZE];
    private int pos;


    @Override
    public void close() {
        //empty
    }

    public boolean contain(byte[] elem) {
        if (elem == null || elem.length == 0) return false;
        int elemLen = elem.length;
        for (int i = 0; i <= pos - elemLen; i++) {
            if (buffer[i] != elem[0]) continue;

            boolean match = true;
            for (int j = 1; j < elemLen; j++) {
                if (buffer[i + j] != elem[j]) {
                    match = false;
                    break;
                }
            }
            if (match) return true;
        }
        return false;
    }

    public String encodeBase64String() {
        byte[] validData = new byte[pos];
        System.arraycopy(buffer, 0, validData, 0, pos - 1);
        return Base64.getEncoder().encodeToString(validData);
    }

    public boolean endWith(byte elem) {
        return pos > 0 && buffer[pos - 1] == elem;
    }

    public boolean equals(byte[] other, int begin, int end) {
        if (other == null) {
            return false;
        }
        if (pos < end) {
            return false;
        }

        for (int i = begin; i < end; i++) {
            if (other[i] != buffer[i]) {
                return false;
            }
        }
        return true;
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
}
