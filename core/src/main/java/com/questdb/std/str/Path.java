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

import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;

import java.io.Closeable;

public final class Path extends AbstractCharSequence implements Closeable, LPSZ {
    public static final char SEPARATOR;
    private long ptr = 0;
    private int capacity;
    private int len;

    public Path() {
        alloc(128);
    }

    public Path(CharSequence str) {
        this.len = str.length();
        alloc(len);
        copyz(str, 0, len, ptr);
    }

    public static void copy(CharSequence str, int from, int len, long addr) {
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i + from);
            Unsafe.getUnsafe().putByte(addr + i, (byte) (Os.type == Os.WINDOWS && c == '/' ? '\\' : c));
        }
    }

    public static void copyPathSeparator(long address) {
        Unsafe.getUnsafe().putByte(address, (byte) SEPARATOR);
    }

    public static void copyz(CharSequence str, int from, int len, long address) {
        copy(str, from, len, address);
        Unsafe.getUnsafe().putByte(address + len, (byte) 0);
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

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(ptr + index);
    }

    public Path of(CharSequence str) {
        return of(str, 0, str.length());
    }

    public Path of(CharSequence str, int from, int len) {
        if (len >= capacity) {
            Unsafe.free(ptr, capacity + 1);
            alloc(len);
        }
        copyz(str, from, len, ptr);
        this.len = len;
        return this;
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = Unsafe.malloc(len + 1);
    }

    static {
        SEPARATOR = Os.type == Os.WINDOWS ? '\\' : '/';
    }
}
