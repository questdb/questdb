/*
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
 */

package com.nfsdb.io.sink;

import com.nfsdb.misc.Unsafe;

public class DirectUnboundedAnsiSink extends AbstractCharSink {
    private final long address;
    private long _wptr;

    public DirectUnboundedAnsiSink(long address) {
        this.address = _wptr = address;
    }

    public void clear(int len) {
        _wptr = address + len;
    }

    /**
     * This is an unbuffered in-memory sink, any data put into it is flushed immediately.
     */
    @Override
    public void flush() {
    }

    @Override
    public CharSink put(CharSequence cs) {
        int len = cs.length();
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(_wptr + i, (byte) cs.charAt(i));
        }
        _wptr += len;
        return this;
    }

    @Override
    public CharSink put(char c) {
        Unsafe.getUnsafe().putByte(_wptr++, (byte) c);
        return this;
    }

    public int length() {
        return (int) (_wptr - address);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        for (long p = address, hi = _wptr; p < hi; p++) {
            b.append((char) Unsafe.getUnsafe().getByte(p));
        }
        return b.toString();
    }
}
