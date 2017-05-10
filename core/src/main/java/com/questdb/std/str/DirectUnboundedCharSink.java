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

public class DirectUnboundedCharSink extends AbstractCharSink {
    private long address;
    private long _wptr;

    public DirectUnboundedCharSink() {
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
        Chars.strcpyw(cs, len, _wptr);
        _wptr += (len * 2);
        return this;
    }

    @Override
    public CharSink put(char c) {
        Unsafe.getUnsafe().putChar(_wptr, c);
        _wptr += 2;
        return this;
    }

    public int length() {
        return (int) (_wptr - address) / 2;
    }

    public final DirectUnboundedCharSink of(long address) {
        this.address = _wptr = address;
        return this;
    }

    @Override
    public String toString() {
        CharSink b = Misc.getThreadLocalBuilder();
        for (long p = address, hi = _wptr; p < hi; p += 2) {
            b.put(Unsafe.getUnsafe().getChar(p));
        }
        return b.toString();
    }
}
