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

import java.io.Closeable;

public final class Path extends AbstractCharSequence implements Closeable, LPSZ {
    private long ptr = 0;
    private int capacity;
    private int len;

    public Path() {
        alloc(128);
    }

    public Path(CharSequence str) {
        int l = str.length();
        alloc(l);
        copy(str, l);
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public void close() {
        if (ptr != 0) {
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

    public Path of(CharSequence str) {
        int l = str.length();
        if (l >= capacity) {
            Unsafe.getUnsafe().freeMemory(ptr);
            alloc(l);
        }
        copy(str, l);
        this.len = l;
        return this;
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = Unsafe.getUnsafe().allocateMemory(len + 1);
    }

    private void copy(CharSequence str, int len) {
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            Unsafe.getUnsafe().putByte(ptr + i, (byte) (Os.type == Os.WINDOWS && c == '/' ? '\\' : c));
        }
        Unsafe.getUnsafe().putByte(ptr + len, (byte) 0);
    }
}
