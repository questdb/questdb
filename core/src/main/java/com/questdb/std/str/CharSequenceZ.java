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
import com.questdb.std.Unsafe;

import java.io.Closeable;

public final class CharSequenceZ extends AbstractCharSequence implements Closeable, LPSZ {
    private long ptr = 0;
    private int capacity;
    private int len;

    public CharSequenceZ(CharSequence str) {
        int l = str.length();
        alloc(l);
        cpyz(str, l);
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

    public CharSequenceZ of(CharSequence str) {
        return of(str, str.length());
    }

    public CharSequenceZ of(CharSequence str, int len) {
        if (len >= capacity) {
            close();
            alloc(len);
        }
        cpyz(str, len);
        return this;
    }

//    @NotNull
//    @Override
//    public String toString() {
//        return Chars.toUtf8String(this);
//    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = Unsafe.malloc(capacity + 1);
    }

    private void cpyz(CharSequence str, int len) {
        Chars.strcpy(str, len, ptr);
        Unsafe.getUnsafe().putByte(ptr + len, (byte) 0);
        this.len = len;
    }
}
