/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.log;

import com.nfsdb.io.sink.AbstractCharSink;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

public class LogRecordSink extends AbstractCharSink {
    private final long address;
    private final long lim;
    private long _wptr;
    private int level;

    public LogRecordSink(int capacity) {
        int c = Numbers.ceilPow2(capacity);
        this.address = _wptr = Unsafe.getUnsafe().allocateMemory(c);
        this.lim = address + c;
    }

    public void clear(int len) {
        _wptr = address + len;
    }

    @Override
    public void flush() {
    }

    @Override
    public CharSink put(CharSequence cs) {
        int rem = (int) (lim - _wptr);
        int len = cs.length();
        int n = rem < len ? rem : len;

        for (int i = 0; i < n; i++) {
            Unsafe.getUnsafe().putByte(_wptr + i, (byte) cs.charAt(i));
        }
        _wptr += n;
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (_wptr < lim) {
            Unsafe.getUnsafe().putByte(_wptr++, (byte) c);
        }
        return this;
    }

    public long getAddress() {
        return address;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
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
