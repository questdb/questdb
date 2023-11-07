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

import io.questdb.std.Unsafe;
import io.questdb.std.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * UTF-8 sink backed by on-heap memory.
 */
public class Utf8StringSink implements MutableUtf8Sink {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final int initialCapacity;
    private byte[] buffer;
    private int pos;

    public Utf8StringSink() {
        this(32);
    }

    public Utf8StringSink(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.buffer = new byte[initialCapacity];
        this.pos = 0;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return buffer[index];
    }

    @Override
    public void clear() {
        clear(0);
    }

    public void clear(int pos) {
        this.pos = pos;
    }

    @TestOnly
    public long getCapacity() {
        return buffer.length;
    }

    @Override
    public Utf8StringSink put(long lo, long hi) {
        checkSize(Bytes.checkedLoHiSize(lo, hi, pos));
        for (long p = lo; p < hi; p++) {
            buffer[pos++] = Unsafe.getUnsafe().getByte(p);
        }
        return this;
    }

    @Override
    public Utf8StringSink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            int s = us.size();
            checkSize(s);
            for (int i = 0; i < s; i++) {
                buffer[pos + i] = us.byteAt(i);
            }
            pos += s;
        }
        return this;
    }

    @Override
    public Utf8StringSink put(byte b) {
        checkSize(1);
        buffer[pos++] = b;
        return this;
    }

    public Utf8StringSink repeat(@NotNull CharSequence value, int n) {
        for (int i = 0; i < n; i++) {
            put(value);
        }
        return this;
    }

    public void resetCapacity() {
        this.buffer = new byte[initialCapacity];
        clear();
    }

    @Override
    public int size() {
        return pos;
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(this);
    }

    private void checkSize(int extra) {
        assert extra >= 0;
        int size = pos + extra;
        if (buffer.length > size) {
            return;
        }
        size = Math.max(pos * 2, size);
        final byte[] n = new byte[size];
        System.arraycopy(buffer, 0, n, 0, pos);
        buffer = n;
    }
}
