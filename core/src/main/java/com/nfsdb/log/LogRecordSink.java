/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

    LogRecordSink(int capacity) {
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
