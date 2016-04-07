/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.std;

import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public final class PrefixedPath extends AbstractCharSequence implements Closeable, LPSZ {
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

        for (int i = 0; i < l; i++) {
            char c = prefix.charAt(i);
            Unsafe.getUnsafe().putByte(ptr + i, (byte) (Os.type == Os.WINDOWS && c == '/' ? '\\' : c));
        }

        char c = prefix.charAt(l - 1);
        if (c != '/' && c != '\\') {
            Unsafe.getUnsafe().putByte(ptr + l, (byte) (Os.type == Os.WINDOWS ? '\\' : '/'));
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
            Unsafe.getUnsafe().freeMemory(ptr);
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
        long p = ptr + prefixLen;
        for (int i = 0; i < l; i++) {
            char c = str.charAt(i);
            Unsafe.getUnsafe().putByte(p + i, (byte) (Os.type == Os.WINDOWS && c == '/' ? '\\' : c));
        }
        Unsafe.getUnsafe().putByte(p + l, (byte) 0);
        this.len = this.prefixLen + l;
        return this;
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Override
    @NotNull
    public String toString() {
        if (ptr == 0) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        long p = this.ptr;
        byte b;
        while ((b = Unsafe.getUnsafe().getByte(p++)) != 0) {
            builder.append((char) b);
        }
        return builder.toString();
    }

    private void alloc(int l) {
        long p = Unsafe.getUnsafe().allocateMemory(l + 1);
        if (ptr > 0) {
            Unsafe.getUnsafe().copyMemory(ptr, p, len);
            Unsafe.getUnsafe().freeMemory(ptr);
        }
        ptr = p;
        this.capacity = l;
    }
}
