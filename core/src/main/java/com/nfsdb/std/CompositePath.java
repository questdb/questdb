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

    CompositePath() {
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
            Unsafe.getUnsafe().freeMemory(ptr);
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
        this.wptr = ptr;
        this.len = 0;
        this.trailingSlash = false;
        return concat(str);
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = this.wptr = Unsafe.getUnsafe().allocateMemory(len + 1);
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
        long p = Unsafe.getUnsafe().allocateMemory(len);
        Unsafe.getUnsafe().copyMemory(ptr, p, this.len);
        long d = wptr - ptr;
        Unsafe.getUnsafe().freeMemory(this.ptr);
        this.ptr = p;
        this.wptr = p + d;
        this.capacity = len;
    }
}
