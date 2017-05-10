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
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;

public class DirectUnboundedByteSink extends AbstractCharSink {
    private final long address;
    private long _wptr;

    public DirectUnboundedByteSink(long address) {
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
        Chars.strcpy(cs, len, _wptr);
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
        CharSink b = Misc.getThreadLocalBuilder();
        for (long p = address, hi = _wptr; p < hi; p++) {
            b.put((char) Unsafe.getUnsafe().getByte(p));
        }
        return b.toString();
    }
}
