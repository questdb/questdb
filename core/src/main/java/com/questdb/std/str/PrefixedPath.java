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

package com.questdb.std.str;

import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.std.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public final class PrefixedPath extends AbstractCharSequence implements Closeable, LPSZ, Sinkable {
    private final int prefixLen;
    private long ptr = 0;
    private int capacity = 0;
    private int len;

    public PrefixedPath(CharSequence prefix) {
        this(prefix, 128);
    }

    PrefixedPath(CharSequence prefix, int minCapacity) {

        int l = prefix.length();

        alloc(Math.max(minCapacity, l * 2));

        Path.copy(prefix, 0, l, ptr);
        char c = prefix.charAt(l - 1);
        if (c != '/' && c != '\\') {
            Path.copyPathSeparator(ptr + l);
            l++;
        }

        Unsafe.getUnsafe().putByte(ptr + l, (byte) 0);
        this.len = this.prefixLen = l;
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public void close() {
        if (ptr > 0) {
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

    public PrefixedPath of(CharSequence str) {
        int l = str.length();
        if (l + prefixLen > capacity) {
            alloc(l + len);
        }
        Path.copyz(str, 0, l, ptr + prefixLen);
        this.len = this.prefixLen + l;
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        long p = this.ptr;
        byte b;
        while ((b = Unsafe.getUnsafe().getByte(p++)) != 0) {
            sink.put((char) b);
        }
    }

    @Override
    @NotNull
    public String toString() {
        if (ptr == 0) {
            return "";
        }

        StringBuilder builder = Misc.getThreadLocalBuilder();
        long p = this.ptr;
        byte b;
        while ((b = Unsafe.getUnsafe().getByte(p++)) != 0) {
            builder.append((char) b);
        }
        return builder.toString();
    }

    private void alloc(int l) {
        long p = Unsafe.malloc(l + 1);
        if (ptr > 0) {
            Unsafe.getUnsafe().copyMemory(ptr, p, len);
            Unsafe.free(ptr, capacity + 1);
        }
        ptr = p;
        this.capacity = l;
    }
}
