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
    private boolean ascii;
    private byte[] buffer;
    private int pos;

    public Utf8StringSink() {
        this(32);
    }

    public Utf8StringSink(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.buffer = new byte[initialCapacity];
        this.pos = 0;
        this.ascii = true;
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
        this.ascii = true;
    }

    public void clear(int pos, boolean ascii) {
        this.pos = pos;
        this.ascii = ascii;
    }

    @TestOnly
    public long getCapacity() {
        return buffer.length;
    }

    @Override
    public int intAt(int offset) {
        return Unsafe.byteArrayGetInt(buffer, offset);
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    @Override
    public long longAt(int offset) {
        return Unsafe.byteArrayGetLong(buffer, offset);
    }

    @Override
    public Utf8StringSink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            ascii &= us.isAscii();
            int s = us.size();
            checkCapacity(s);
            for (int i = 0; i < s; i++) {
                buffer[pos + i] = us.byteAt(i);
            }
            pos += s;
        }
        return this;
    }

    @Override
    public Utf8StringSink put(byte b) {
        assert b < 0 : "b is ascii";
        ascii = false;
        return putByte0(b);
    }

    @Override
    public Utf8StringSink putAny(byte b) {
        ascii &= b >= 0;
        return putByte0(b);
    }

    @Override
    public Utf8StringSink putAscii(char c) {
        return putByte0((byte) c);
    }

    @Override
    public Utf8StringSink putNonAscii(long lo, long hi) {
        ascii = false;
        checkCapacity(Bytes.checkedLoHiSize(lo, hi, pos));
        for (long p = lo; p < hi; p++) {
            buffer[pos++] = Unsafe.getUnsafe().getByte(p);
        }
        return this;
    }

    public Utf8StringSink repeat(@NotNull CharSequence value, int n) {
        for (int i = 0; i < n; i++) {
            put(value);
        }
        return this;
    }

    public Utf8StringSink repeat(char value, int n) {
        if (value < 128) {
            // fast path for ASCII
            return putByte0Repeat((byte) value, n);
        }

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
    public short shortAt(int offset) {
        return Unsafe.byteArrayGetShort(buffer, offset);
    }

    @Override
    public int size() {
        return pos;
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(this);
    }

    private void checkCapacity(int extra) {
        assert extra >= 0;
        int size = pos + extra;
        if (buffer.length >= size) {
            return;
        }
        size = Math.max(pos * 2, size);
        final byte[] n = new byte[size];
        System.arraycopy(buffer, 0, n, 0, pos);
        buffer = n;
    }

    @NotNull
    private Utf8StringSink putByte0(byte b) {
        checkCapacity(1);
        buffer[pos++] = b;
        return this;
    }

    @NotNull
    private Utf8StringSink putByte0Repeat(byte b, int n) {
        checkCapacity(n);
        for (int i = 0; i < n; i++) {
            buffer[pos++] = b;
        }
        return this;
    }
}
