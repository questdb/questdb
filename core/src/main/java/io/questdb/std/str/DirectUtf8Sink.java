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
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.bytes.NativeByteSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * UTF-8 sink backed by native memory.
 */
public class DirectUtf8Sink implements MutableUtf8Sink, BorrowableUtf8Sink, DirectUtf8Sequence, Closeable {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final DirectByteSink sink;
    private boolean ascii;

    public DirectUtf8Sink(long initialCapacity) {
        sink = new DirectByteSink(initialCapacity) {
            @Override
            protected int memoryTag() {
                return MemoryTag.NATIVE_DIRECT_UTF8_SINK;
            }
        };
        ascii = true;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public @NotNull NativeByteSink borrowDirectByteSink() {
        return sink.borrowDirectByteSink();
    }

    @Override
    public byte byteAt(int index) {
        return sink.byteAt(index);
    }

    @TestOnly
    public long capacity() {
        return sink.capacity();
    }

    @Override
    public void clear() {
        sink.clear();
        ascii = true;
    }

    @Override
    public void close() {
        sink.close();
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    @Override
    public long ptr() {
        return sink.ptr();
    }

    @Override
    public DirectUtf8Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            ascii &= us.isAscii();
            final int size = us.size();
            final long dest = sink.checkCapacity(size);
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(dest + i, us.byteAt(i));
            }
            sink.advance(size);
        }
        return this;
    }

    @Override
    public DirectUtf8Sink put(byte b) {
        ascii = false;
        sink.put(b);
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

    @Override
    public DirectUtf8Sink putUtf8(long lo, long hi) {
        ascii = false;
        sink.put(lo, hi);
        return this;
    }

    @Override
    public int size() {
        return sink.size();
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(sink.lo(), sink.hi());
    }
}
