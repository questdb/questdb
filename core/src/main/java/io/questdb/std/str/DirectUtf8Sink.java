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

package io.questdb.std.str;

import io.questdb.std.MemoryTag;
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

    public DirectUtf8Sink(long initialCapacity) {
        this(initialCapacity, true);
    }

    public DirectUtf8Sink(long initialCapacity, boolean alloc) {
        sink = new DirectByteSink(initialCapacity, MemoryTag.NATIVE_DIRECT_UTF8_SINK, !alloc);
    }

    public DirectUtf8Sink(long initialCapacity, boolean alloc, int memoryTag) {
        sink = new DirectByteSink(initialCapacity, memoryTag, !alloc);
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

    public long capacity() {
        return sink.allocatedCapacity();
    }

    @Override
    public void clear() {
        sink.clear();
    }

    @Override
    public void close() {
        sink.close();
    }

    @Override
    public boolean isAscii() {
        return sink.isAscii();
    }

    @Override
    public long ptr() {
        return sink.ptr();
    }

    @Override
    public DirectUtf8Sink put(@Nullable Utf8Sequence us) {
        if (us == null) {
            return this;
        }
        setAscii(isAscii() & us.isAscii());
        final int size = us.size();
        final long dest = sink.ensureCapacity(size);
        us.writeTo(dest, 0, size);
        sink.advance(size);
        return this;
    }

    @Override
    public DirectUtf8Sink put(byte b) {
        assert b < 0 : "b is ascii";
        setAscii(false);
        sink.put(b);
        return this;
    }

    @Override
    public DirectUtf8Sink putAny(byte b) {
        setAscii(isAscii() & b >= 0);
        sink.put(b);
        return this;
    }

    /**
     * Same as {@link #putAny(byte)}, but writes 8 consequent bytes (a long).
     */
    public void putAny8(long w) {
        setAscii(isAscii() & Utf8s.isAscii(w));
        sink.putLong(w);
    }

    @Override
    public DirectUtf8Sink putAscii(char c) {
        sink.put((byte) c);
        return this;
    }

    @Override
    public DirectUtf8Sink putAscii(@Nullable CharSequence cs) {
        MutableUtf8Sink.super.putAscii(cs);
        return this;
    }

    @TestOnly
    public DirectUtf8Sink putDouble(double value) {
        setAscii(false);
        sink.putDouble(value);
        return this;
    }

    @Override
    public DirectUtf8Sink putNonAscii(long lo, long hi) {
        setAscii(false);
        sink.put(lo, hi);
        return this;
    }

    public void reopen() {
        sink.reopen();
    }

    public void reserve(long minCapacity) {
        sink.reserve(minCapacity);
    }

    public void resetCapacity() {
        sink.resetCapacity();
    }

    @Override
    public int size() {
        return sink.size();
    }

    @Override
    public long tailPadding() {
        return sink.tailPadding();
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(sink.lo(), sink.hi());
    }

    private void setAscii(boolean ascii) {
        sink.setAscii(ascii);
    }
}
