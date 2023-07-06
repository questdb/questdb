/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class DirectByteCharSink implements Mutable, ByteSequence, Closeable {
    private final long initialCapacity;
    private long capacity;
    private long hi;
    private long lo;
    private long ptr;

    public DirectByteCharSink(long capacity) {
        ptr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        this.capacity = capacity;
        this.initialCapacity = capacity;
        this.lo = ptr;
        this.hi = ptr + capacity;
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(ptr + index);
    }

    @Override
    public void clear() {
        lo = ptr;
    }

    @Override
    public void close() {
        Unsafe.free(ptr, capacity, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
    }

    @TestOnly
    public long getCapacity() {
        return capacity;
    }

    @Override
    public int length() {
        return (int) (lo - ptr);
    }

    public DirectByteCharSink put(ByteSequence cs) {
        if (cs != null) {
            int l = cs.length();

            if (lo + l >= hi) {
                resize(Math.max(capacity * 2L, (lo - ptr + l) * 2L));
            }

            for (int i = 0; i < l; i++) {
                Unsafe.getUnsafe().putByte(lo + i, cs.byteAt(i));
            }
            this.lo += l;
        }
        return this;
    }

    public DirectByteCharSink put(byte b) {
        if (lo == hi) {
            resize(this.capacity * 2);
        }
        Unsafe.getUnsafe().putByte(lo++, b);
        return this;
    }

    public void resetCapacity() {
        resize(initialCapacity);
        clear();
    }

    @NotNull
    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(this);
    }

    private void resize(long cap) {
        long temp = Unsafe.realloc(ptr, capacity, cap, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        int len = (int) (lo - ptr);
        this.ptr = temp;
        this.capacity = cap;
        this.lo = ptr + len;
        this.hi = ptr + cap;
    }
}
