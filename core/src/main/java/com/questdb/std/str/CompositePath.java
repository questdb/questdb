/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.misc.Chars;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

public final class CompositePath extends AbstractCharSink implements Closeable, LPSZ {
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
        if (1 + (wptr - ptr) >= capacity) {
            extend((int) (16 + (wptr - ptr)));
        }
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
            extend(l + len + OVERHEAD);
        }

        if (len > 0 && !trailingSlash) {
            Path.copyPathSeparator(wptr);
            wptr++;
            len++;
        }

        copy(str, l);
        return this;
    }

    public CompositePath concat(long lpsz) {

        if (len > 0 && !trailingSlash) {
            Path.copyPathSeparator(wptr);
            wptr++;
            len++;
        }

        long p = lpsz;
        while (true) {
            if (len + OVERHEAD >= capacity) {
                extend(len * 2 + OVERHEAD);
            }

            byte b = Unsafe.getUnsafe().getByte(p++);
            if (b == 0) {
                this.trailingSlash = p - lpsz > 2 && ((Os.type == Os.WINDOWS && Unsafe.getUnsafe().getByte(p - 2) == '\\') || Unsafe.getUnsafe().getByte(p - 2) == '/');
                break;
            }

            Unsafe.getUnsafe().putByte(wptr, (byte) (b == '/' && Os.type == Os.WINDOWS ? '\\' : b));
            wptr++;
            len++;
        }

        return this;
    }

    @Override
    public void flush() throws IOException {
        $();
    }

    @Override
    public CompositePath put(CharSequence str) {
        int l = str.length();
        if (l + len >= capacity) {
            extend(l + len);
        }
        Chars.strcpy(str, l, wptr);
        wptr += l;
        len += l;
        return this;
    }

    @Override
    public CompositePath put(char c) {
        if (1 + len >= capacity) {
            extend(16 + len);
        }
        Unsafe.getUnsafe().putByte(wptr++, (byte) c);
        len++;
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

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
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

    public CompositePath of(long lpsz) {
        if (lpsz != ptr) {
            this.wptr = ptr;
            this.len = 0;
            this.trailingSlash = false;
            return concat(lpsz);
        }
        return this;
    }

    @Override
    @NotNull
    public String toString() {
        return ptr == 0 ? "" : AbstractCharSequence.getString(this);
    }

    public void trimBy(int count) {
        if (Unsafe.getUnsafe().getByte(wptr - 1) == 0) {
            wptr -= count;
            len -= count - 1;
        } else {
            wptr -= count;
            len -= count;
        }
    }

    public CompositePath trimTo(int len) {
        this.len = len;
        wptr = ptr + len;
        return this;
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = this.wptr = Unsafe.malloc(len + 1);
    }

    private void copy(CharSequence str, int len) {
        Path.copy(str, 0, len, wptr);
        if (len > 0) {
            char c = str.charAt(len - 1);
            this.trailingSlash = c == '/' || c == '\\';
        } else {
            this.trailingSlash = false;
        }
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
