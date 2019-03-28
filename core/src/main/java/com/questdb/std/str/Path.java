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

import com.questdb.std.Chars;
import com.questdb.std.Files;
import com.questdb.std.Os;
import com.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Builder class that allows JNI layer access CharSequence without copying memory. It is typically used
 * to create file system paths for files and directories and passing them to {@link Files} static methods, those
 * that accept @link {@link LPSZ} as input.
 * <p>
 * Instances of this class can be re-cycled for creating many different paths and
 * must be closed when no longer required.
 * </p>
 */
public class Path extends AbstractCharSink implements Closeable, LPSZ {
    private static final int OVERHEAD = 4;
    private long ptr;
    private long wptr;
    private int capacity;
    private int len;

    public Path() {
        this(128);
    }

    public Path(int capacity) {
        this.capacity = capacity;
        this.ptr = this.wptr = Unsafe.malloc(capacity + 1);
    }

    public Path $() {
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

    /**
     * Removes trailing zero from path to allow reuse of path as parent.
     *
     * @return instance of this
     */
    public Path chopZ() {
        trimTo(this.length());
        return this;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, capacity + 1);
            ptr = 0;
        }
    }

    public Path concat(CharSequence str) {
        return concat(str, 0, str.length());
    }

    public Path concat(long lpsz) {

        ensureSeparator();

        long p = lpsz;
        while (true) {

            if (len + OVERHEAD >= capacity) {
                extend(len * 2 + OVERHEAD);
            }

            byte b = Unsafe.getUnsafe().getByte(p++);
            if (b == 0) {
                break;
            }

            Unsafe.getUnsafe().putByte(wptr, (byte) (b == '/' && Os.type == Os.WINDOWS ? '\\' : b));
            wptr++;
            len++;
        }

        return this;
    }

    public Path concat(CharSequence str, int from, int to) {
        ensureSeparator();
        copy(str, from, to);
        return this;
    }

    @Override
    public void flush() {
        $();
    }

    @Override
    public Path put(CharSequence str) {
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
    public CharSink put(CharSequence cs, int start, int end) {
        int l = end - start;
        if (l + len >= capacity) {
            extend(l + len);
        }
        Chars.strcpy(cs, start, end, wptr);
        wptr += l;
        len += l;
        return this;
    }

    @Override
    public Path put(char c) {
        if (1 + len >= capacity) {
            extend(16 + len);
        }
        Unsafe.getUnsafe().putByte(wptr++, (byte) c);
        len++;
        return this;
    }

    @Override
    public final int length() {
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

    public Path of(CharSequence str) {
        if (str == this) {
            this.len = str.length();
            this.wptr = ptr + len;
            return this;
        } else {
            this.wptr = ptr;
            this.len = 0;
            return concat(str);
        }
    }

    public Path of(CharSequence str, int from, int to) {
        this.wptr = ptr;
        this.len = 0;
        return concat(str, from, to);
    }

    public Path of(long lpsz) {
        if (lpsz != ptr) {
            this.wptr = ptr;
            this.len = 0;
            return concat(lpsz);
        }
        return this;
    }

    @Override
    @NotNull
    public String toString() {
        return ptr == 0 ? "" : AbstractCharSequence.getString(this);
    }

    public Path trimTo(int len) {
        this.len = len;
        wptr = ptr + len;
        return this;
    }

    private void copy(CharSequence str, int from, int to) {
        encodeUtf8(str, from, to);
    }

    protected final void ensureSeparator() {
        if (missingTrailingSeparator()) {
            Unsafe.getUnsafe().putByte(wptr, (byte) Files.SEPARATOR);
            wptr++;
            this.len++;
        }
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

    private boolean missingTrailingSeparator() {
        return len > 0 && Unsafe.getUnsafe().getByte(wptr - 1) != Files.SEPARATOR;
    }

    @Override
    protected void putUtf8Special(char c) {
        if (c == '/' && Os.type == Os.WINDOWS) {
            put('\\');
        } else {
            put(c);
        }
    }
}
