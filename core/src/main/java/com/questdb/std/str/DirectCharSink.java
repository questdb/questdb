/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std.str;

import com.questdb.std.Mutable;
import com.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class DirectCharSink extends AbstractCharSink implements CharSequence, Closeable, Mutable, DirectBytes {
    private long ptr;
    private int capacity;
    private long lo;
    private long hi;

    public DirectCharSink(int capacity) {
        ptr = Unsafe.malloc(capacity);
        this.capacity = capacity;
        this.lo = ptr;
        this.hi = ptr + capacity;
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public int byteLength() {
        return (int) (lo - ptr);
    }

    @Override
    public void clear() {
        lo = ptr;
    }

    @Override
    public void close() {
        Unsafe.free(ptr, capacity);
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
        int l = cs.length();
        int l2 = l * 2;

        if (lo + l2 >= hi) {
            resize((int) Math.max(capacity * 2L, (lo - ptr + l2) * 2L));
        }

        for (int i = 0; i < l; i++) {
            Unsafe.getUnsafe().putChar(lo + i * 2, cs.charAt(i));
        }
        this.lo += l2;
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

    @NotNull
    @Override
    public String toString() {
        return AbstractCharSequence.getString(this);
    }

    private void resize(int cap) {
        long temp = Unsafe.malloc(cap);
        int len = (int) (lo - ptr);
        Unsafe.getUnsafe().copyMemory(ptr, temp, len);
        Unsafe.free(ptr, capacity);

        this.ptr = temp;
        this.capacity = cap;
        this.lo = ptr + len;
        this.hi = ptr + cap;
    }
}
