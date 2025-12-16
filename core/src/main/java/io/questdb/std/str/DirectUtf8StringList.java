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

package io.questdb.std.str;

import io.questdb.cairo.Reopenable;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.DirectByteSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A list of UTF-8 strings backed by native memory.
 */
public class DirectUtf8StringList implements Mutable, QuietCloseable, Reopenable, Utf8Sink {
    private final DirectLongList offsets;
    private final DirectByteSink sink;
    private final DirectUtf8StringView view = new DirectUtf8StringView();

    public DirectUtf8StringList(long initialCapacity, long initialElementCount) {
        this.sink = new DirectByteSink(initialCapacity, MemoryTag.NATIVE_DIRECT_UTF8_SINK);
        this.offsets = new DirectLongList(initialElementCount, MemoryTag.NATIVE_DIRECT_UTF8_SINK);
    }

    @Override
    public void clear() {
        sink.clear();
        offsets.clear();
    }

    @Override
    public void close() {
        Misc.free(sink);
        Misc.free(offsets);
    }

    /**
     * Returns a view of the element at the specified index.
     * The returned view is reused, so copy the content if needed.
     */
    public DirectUtf8Sequence getQuick(int index) {
        long lo = index == 0 ? 0 : offsets.get(index - 1);
        long hi = offsets.get(index);
        view.of(sink.ptr() + lo, sink.ptr() + hi, sink.isAscii());
        return view;
    }

    public DirectUtf8StringList put(byte b) {
        sink.setAscii(sink.isAscii() & b >= 0);
        sink.put(b);
        return this;
    }

    public DirectUtf8StringList put(long lo, long hi) {
        sink.put(lo, hi);
        return this;
    }

    @Override
    public DirectUtf8StringList put(@Nullable Utf8Sequence us) {
        if (us == null) {
            return this;
        }
        sink.setAscii(sink.isAscii() & us.isAscii());
        final int size = us.size();
        final long dest = sink.ensureCapacity(size);
        for (int i = 0; i < size; i++) {
            Unsafe.getUnsafe().putByte(dest + i, us.byteAt(i));
        }
        sink.advance(size);
        setElem();
        return this;
    }

    public DirectUtf8StringList putAscii(char c) {
        sink.put((byte) c);
        return this;
    }

    @Override
    public DirectUtf8StringList putNonAscii(long lo, long hi) {
        sink.setAscii(false);
        sink.put(lo, hi);
        return this;
    }

    @Override
    public void reopen() {
        offsets.reopen();
        sink.reopen();
    }

    /**
     * Marks the end of current element.
     * All bytes put since the last setElem() call become a new element.
     */
    public void setElem() {
        offsets.add(sink.size());
    }

    /**
     * Returns the number of elements in the list.
     */
    public int size() {
        return (int) offsets.size();
    }

    private static class DirectUtf8StringView implements DirectUtf8Sequence {
        private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
        private long hi;
        private boolean isAscii;
        private long lo;

        @Override
        public @NotNull CharSequence asAsciiCharSequence() {
            return asciiCharSequence.of(this);
        }

        @Override
        public byte byteAt(int index) {
            return Unsafe.getUnsafe().getByte(lo + index);
        }

        @Override
        public boolean isAscii() {
            return isAscii;
        }

        @Override
        public long ptr() {
            return lo;
        }

        @Override
        public int size() {
            return (int) (hi - lo);
        }

        @Override
        public @NotNull String toString() {
            return Utf8s.stringFromUtf8Bytes(lo, hi);
        }

        void of(long lo, long hi, boolean ascii) {
            this.lo = lo;
            this.hi = hi;
            this.isAscii = ascii;
        }
    }
}
