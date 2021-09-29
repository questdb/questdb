/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import java.io.Closeable;

public class DirectCharSink extends AbstractCharSink implements MutableCharSink, Closeable {
    private long ptr;
    private long capacity;
    private long lo;
    private long hi;

    public DirectCharSink(long capacity) {
        ptr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        this.capacity = capacity;
        this.lo = ptr;
        this.hi = ptr + capacity;
    }

    @Override
    public void clear() {
        lo = ptr;
    }

    @Override
    public void close() {
        Unsafe.free(ptr, capacity, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public int length() {
        return (int) (lo - ptr) / 2;
    }

    @Override
    public char charAt(int index) {
        return Unsafe.getUnsafe().getChar(ptr + index * 2L);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs != null) {
            int l = cs.length();
            int l2 = l * 2;

            if (lo + l2 >= hi) {
                resize(Math.max(capacity * 2L, (lo - ptr + l2) * 2L));
            }

            for (int i = 0; i < l; i++) {
                Unsafe.getUnsafe().putChar(lo + i * 2L, cs.charAt(i));
            }
            this.lo += l2;
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (lo == hi) {
            resize(this.capacity * 2);
        }
        Unsafe.getUnsafe().putChar(lo, c);
        lo += 2;
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        int l2 = len * 2;

        if (lo + l2 >= hi) {
            resize((int) Math.max(capacity * 2L, (lo - ptr + l2) * 2L));
        }

        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(lo + i * 2L, chars[i + start]);
        }

        this.lo += l2;
        return this;
    }

    @NotNull
    @Override
    public String toString() {
        return AbstractCharSequence.getString(this);
    }

    private void resize(long cap) {
        long temp = Unsafe.realloc(ptr, capacity, cap, MemoryTag.NATIVE_DEFAULT);
        int len = (int) (lo - ptr);
        this.ptr = temp;
        this.capacity = cap;
        this.lo = ptr + len;
        this.hi = ptr + cap;
    }
}
