/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.std;

import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;

import java.io.Closeable;

public final class CompositePath extends AbstractCharSequence implements Closeable, LPSZ {
    public static final ObjectFactory<CompositePath> FACTORY = new ObjectFactory<CompositePath>() {
        @Override
        public CompositePath newInstance() {
            return new CompositePath();
        }
    };
    private static final int OVERHEAD = 4;
    private long ptr = 0;
    private long wptr;
    private int capacity;
    private int len;
    private boolean trailingSlash = false;

    public CompositePath() {
        alloc(128);
    }

    public CompositePath $() {
        Unsafe.getUnsafe().putByte(wptr++, (byte) 0);
        return this;
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, capacity + 1);
            ptr = 0;
        }
    }

    public CompositePath concat(CharSequence str) {
        int l = str.length();
        if (l + len + OVERHEAD >= capacity) {
            extend(l + len);
        }

        if (len > 0 && !trailingSlash) {
            Unsafe.getUnsafe().putByte(wptr++, (byte) (Os.type == Os.WINDOWS ? '\\' : '/'));
            len++;
        }

        copy(str, l);
        return this;
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(ptr + index);
    }

    public CompositePath of(CharSequence str) {
        if (str == this) {
            this.len = str.length();
            this.wptr = ptr + len;
            this.trailingSlash = false;
            return this;
        } else {
            this.wptr = ptr;
            this.len = 0;
            this.trailingSlash = false;
            return concat(str);
        }
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = this.wptr = Unsafe.malloc(len + 1);
    }

    private void copy(CharSequence str, int len) {
        char c = 0;
        for (int i = 0; i < len; i++) {
            c = str.charAt(i);
            Unsafe.getUnsafe().putByte(wptr + i, (byte) (Os.type == Os.WINDOWS && c == '/' ? '\\' : c));
        }
        this.trailingSlash = c == '/' || c == '\\';
        this.wptr += len;
        this.len += len;
    }

    private void extend(int len) {
        long p = Unsafe.malloc(len);
        Unsafe.getUnsafe().copyMemory(ptr, p, this.len);
        long d = wptr - ptr;
        Unsafe.free(this.ptr, this.capacity);
        this.ptr = p;
        this.wptr = p + d;
        this.capacity = len;
    }
}
