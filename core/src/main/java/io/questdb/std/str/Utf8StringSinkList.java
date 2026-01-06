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

import io.questdb.std.BoolList;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A list of UTF-8 strings backed by on-heap memory.
 * Similar to {@link DirectUtf8StringList} but uses Java heap instead of native memory.
 */
public class Utf8StringSinkList implements Mutable, Utf8Sink {
    private final BoolList asciiFlags = new BoolList();
    private final IntList offsets = new IntList();
    private final Utf8StringView view = new Utf8StringView();
    private byte[] buffer;
    private boolean currentElemAscii = true;
    private int pos;

    public Utf8StringSinkList(int initialCapacity) {
        this.buffer = new byte[initialCapacity];
        this.pos = 0;
    }

    @Override
    public void clear() {
        pos = 0;
        offsets.clear();
        asciiFlags.clear();
        currentElemAscii = true;
    }

    /**
     * Returns a view of the element at the specified index.
     * The returned view is reused, so copy the content if needed.
     */
    public Utf8Sequence getQuick(int index) {
        int lo = index == 0 ? 0 : offsets.getQuick(index - 1);
        int hi = offsets.getQuick(index);
        view.of(lo, hi, asciiFlags.get(index));
        return view;
    }

    @Override
    public Utf8Sink put(byte b) {
        checkCapacity(1);
        currentElemAscii = currentElemAscii & b >= 0;
        buffer[pos++] = b;
        return this;
    }

    @Override
    public Utf8Sink put(@NotNull CharSequence cs, int lo, int hi) {
        Utf8Sink.super.put(cs, lo, hi);
        setElem();
        return this;
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence us) {
        if (us == null) {
            return this;
        }
        currentElemAscii = us.isAscii();
        int size = us.size();
        checkCapacity(size);
        for (int i = 0; i < size; i++) {
            buffer[pos++] = us.byteAt(i);
        }
        setElem();
        return this;
    }

    public Utf8StringSinkList put(long lo, long hi) {
        int len = (int) (hi - lo);
        checkCapacity(len);
        for (long p = lo; p < hi; p++) {
            byte b = Unsafe.getUnsafe().getByte(p);
            buffer[pos++] = b;
        }
        return this;
    }

    @Override
    public Utf8Sink putAscii(char c) {
        checkCapacity(1);
        buffer[pos++] = (byte) c;
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        currentElemAscii = false;
        return put(lo, hi);
    }

    /**
     * Marks the end of current element.
     */
    public void setElem() {
        offsets.add(pos);
        asciiFlags.add(currentElemAscii);
        currentElemAscii = true;
    }

    private void checkCapacity(int extra) {
        int required = pos + extra;
        if (buffer.length >= required) {
            return;
        }
        int newSize = Math.max(buffer.length * 2, required);
        byte[] newBuffer = new byte[newSize];
        System.arraycopy(buffer, 0, newBuffer, 0, pos);
        buffer = newBuffer;
    }

    private class Utf8StringView implements Utf8Sequence {
        private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
        private int hi;
        private boolean isAscii;
        private int lo;

        @Override
        public @NotNull CharSequence asAsciiCharSequence() {
            return asciiCharSequence.of(this);
        }

        @Override
        public byte byteAt(int index) {
            return buffer[lo + index];
        }

        @Override
        public boolean isAscii() {
            return isAscii;
        }

        @Override
        public int size() {
            return hi - lo;
        }

        @Override
        public @NotNull String toString() {
            return Utf8s.stringFromUtf8Bytes(this);
        }

        void of(int lo, int hi, boolean ascii) {
            this.lo = lo;
            this.hi = hi;
            this.isAscii = ascii;
        }
    }
}
