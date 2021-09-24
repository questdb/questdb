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

package io.questdb.std.str;

import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public final class StdoutSink extends AbstractCharSink implements Closeable {

    public static final StdoutSink INSTANCE = new StdoutSink();

    private final long stdout = Files.getStdOutFd();
    private final int bufferCapacity = 1024;
    private final long buffer = Unsafe.malloc(bufferCapacity, MemoryTag.NATIVE_DEFAULT);
    private final long limit = buffer + bufferCapacity;
    private long ptr = buffer;

    @Override
    public void close() {
        free();
    }

    @Override
    public void flush() {
        int len = (int) (ptr - buffer);
        if (len > 0) {
            Files.append(stdout, buffer, len);
            ptr = buffer;
        }
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs != null) {
            for (int i = 0, len = cs.length(); i < len; i++) {
                put(cs.charAt(i));
            }
        }
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            put(chars[i + start]);
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (ptr == limit) {
            flush();
        }
        Unsafe.getUnsafe().putByte(ptr++, (byte) c);
        return this;
    }

    private void free() {
        Unsafe.free(buffer, bufferCapacity, MemoryTag.NATIVE_DEFAULT);
    }
}
