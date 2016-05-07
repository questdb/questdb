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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.io.sink;

import com.questdb.misc.Unsafe;
import com.questdb.std.CharSink;

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
