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

import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FlyweightDirectUtf16Sink implements MutableUtf16Sink, DirectCharSequence, CloneableMutable {
    private long hi;
    private long lo;
    private long ptr;

    public FlyweightDirectUtf16Sink() {
        lo = hi = ptr = 0;
    }

    public long appendPtr() {
        return lo;
    }

    public FlyweightDirectUtf16Sink asCharSequence(long lo, long hi) {
        this.ptr = lo;
        this.lo = hi;
        this.hi = hi;
        return this;
    }

    @Override
    public char charAt(int index) {
        return Unsafe.getUnsafe().getChar(ptr + index * 2L);
    }

    @Override
    public void clear() {
        clear(0);
    }

    public void clear(int len) {
        lo = ptr + len;
    }

    public void close() {
        lo = hi = ptr = 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T copy() {
        return (T) toString();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof CharSequence && Chars.equals(this, (CharSequence) obj);
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(this);
    }

    @Override
    public int length() {
        return (int) (lo - ptr) / 2;
    }

    public FlyweightDirectUtf16Sink of(long lo, long hi) {
        this.ptr = lo;
        this.lo = lo;
        this.hi = hi;
        return this;
    }

    @Override
    public long ptr() {
        return ptr;
    }

    @Override
    public Utf16Sink put(@Nullable CharSequence cs) {
        if (cs != null) {
            int l = cs.length();
            assert checkCapacity(l);
            int l2 = l * 2;
            for (int i = 0; i < l; i++) {
                Unsafe.getUnsafe().putChar(lo + i * 2L, cs.charAt(i));
            }
            this.lo += l2;
        }
        return this;
    }

    @Override
    public Utf16Sink put(char @NotNull [] chars, int start, int len) {
        assert checkCapacity(len);
        int l2 = len * 2;
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(lo + i * 2L, chars[i + start]);
        }

        this.lo += l2;
        return this;
    }

    @Override
    public Utf16Sink put(char c) {
        assert checkCapacity(1);
        Unsafe.getUnsafe().putChar(lo, c);
        lo += 2;
        return this;
    }

    @Override
    public int size() {
        return (int) (lo - ptr);
    }

    @Override
    public @NotNull CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public String toString() {
        return AbstractCharSequence.getString(this);
    }

    private boolean checkCapacity(int nChars) {
        return lo + (2L * nChars) <= hi;
    }
}
