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

import io.questdb.cairo.Reopenable;
import io.questdb.std.BoolList;
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
    private final BoolList asciiFlags = new BoolList();
    private final DirectLongList offsets;
    private final DirectByteSink sink;
    private final DirectUtf8StringView view = new DirectUtf8StringView();
    private boolean currentElemAscii = true;

    public DirectUtf8StringList(long initialCapacity, long initialElementCount) {
        this(initialCapacity, initialElementCount, false);
    }

    public DirectUtf8StringList(long initialCapacity, long initialElementCount, boolean keepClosed) {
        this.sink = new DirectByteSink(initialCapacity, MemoryTag.NATIVE_DIRECT_UTF8_SINK, keepClosed);
        this.offsets = new DirectLongList(initialElementCount, MemoryTag.NATIVE_DIRECT_UTF8_SINK, keepClosed);
    }

    @Override
    public void clear() {
        sink.clear();
        offsets.clear();
        asciiFlags.clear();
        currentElemAscii = true;
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
        view.of(sink.ptr() + lo, sink.ptr() + hi, asciiFlags.get(index));
        return view;
    }

    @Override
    public Utf8Sink put(@NotNull CharSequence cs, int lo, int hi) {
        Utf8Sink sink = Utf8Sink.super.put(cs, lo, hi);
        setElem();
        return sink;
    }

    public DirectUtf8StringList put(byte b) {
        currentElemAscii = currentElemAscii & b >= 0;
        sink.put(b);
        return this;
    }

    public DirectUtf8StringList put(long lo, long hi) {
        sink.put(lo, hi);
        return this;
    }

    /**
     * Appends a Utf8Sequence as a new element in the list.
     * If the sequence is null, no element is added.
     *
     * @param us the sequence to append, or null to skip
     * @return this list for method chaining
     */
    @Override
    public DirectUtf8StringList put(@Nullable Utf8Sequence us) {
        if (us == null) {
            return this;
        }
        currentElemAscii = us.isAscii();
        final int size = us.size();
        final long dest = sink.ensureCapacity(size);
        us.writeTo(dest, 0, size);
        sink.advance(size);
        setElem();
        return this;
    }

    /**
     * Appends a DirectUtf8Sequence as a new element in the list.
     * If the sequence is null, no element is added.
     *
     * @param dus the sequence to append, or null to skip
     * @return this list for method chaining
     */
    @Override
    public DirectUtf8StringList put(@Nullable DirectUtf8Sequence dus) {
        if (dus == null) {
            return this;
        }
        currentElemAscii = dus.isAscii();
        sink.put(dus.lo(), dus.hi());
        setElem();
        return this;
    }

    public DirectUtf8StringList putAscii(char c) {
        sink.put((byte) c);
        return this;
    }

    @Override
    public DirectUtf8StringList putNonAscii(long lo, long hi) {
        currentElemAscii = false;
        sink.put(lo, hi);
        return this;
    }

    @Override
    public void reopen() {
        offsets.reopen();
        sink.reopen();
        asciiFlags.clear();
        currentElemAscii = true;
    }

    /**
     * Marks the end of current element.
     * All bytes put since the last setElem() call become a new element.
     */
    public void setElem() {
        offsets.add(sink.size());
        asciiFlags.add(currentElemAscii);
        currentElemAscii = true;
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
