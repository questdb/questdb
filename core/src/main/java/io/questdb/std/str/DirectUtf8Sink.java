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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * UTF-8 sink backed by native memory.
 */
public class DirectUtf8Sink implements MutableUtf8Sink, DirectUtf8Sequence, Closeable {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final long initialCapacity;
    private long capacity;
    private long hi;
    private long lo;
    private long ptr;

    public DirectUtf8Sink(long initialCapacity) {
        ptr = Unsafe.malloc(initialCapacity, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        this.capacity = initialCapacity;
        this.initialCapacity = initialCapacity;
        this.lo = ptr;
        this.hi = ptr + initialCapacity;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
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
    public long ptr() {
        return ptr;
    }

    @Override
    public DirectUtf8Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            int s = us.size();
            if (lo + s >= hi) {
                resize(Math.max(capacity * 2L, (lo - ptr + s) * 2L));
            }
            for (int i = 0; i < s; i++) {
                Unsafe.getUnsafe().putByte(lo + i, us.byteAt(i));
            }
            this.lo += s;
        }
        return this;
    }

    @Override
    public DirectUtf8Sink put(byte b) {
        if (lo == hi) {
            resize(this.capacity * 2);
        }
        Unsafe.getUnsafe().putByte(lo++, b);
        return this;
    }

    @Override
    public DirectUtf8Sink putAscii(char c) {
        MutableUtf8Sink.super.putAscii(c);
        return this;
    }

    @Override
    public DirectUtf8Sink putAscii(@Nullable CharSequence cs) {
        MutableUtf8Sink.super.putAscii(cs);
        return this;
    }

    public void resetCapacity() {
        resize(initialCapacity);
        clear();
    }

    @Override
    public int size() {
        return (int) (lo - ptr);
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(ptr, lo);
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
