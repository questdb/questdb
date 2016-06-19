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

package com.questdb.ql.impl.join.asof;

import com.questdb.misc.Unsafe;
import com.questdb.std.CharSink;
import com.questdb.std.DirectCharSequence;

abstract class AbstractVarMemRecord extends AbstractMemRecord {

    private final DirectCharSequence cs = new DirectCharSequence();
    private final DirectCharSequence csB = new DirectCharSequence();
    private char[] strBuf;

    @Override
    public CharSequence getFlyweightStr(int col) {
        long address = address() + getInt(col);
        cs.of(address + 4, address + 4 + Unsafe.getUnsafe().getInt(address) * 2);
        return cs;
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        long address = address() + getInt(col);
        csB.of(address + 4, address + 4 + Unsafe.getUnsafe().getInt(address) * 2);
        return csB;
    }

    @Override
    public CharSequence getStr(int col) {
        long address = address() + getInt(col);
        int len = Unsafe.getUnsafe().getInt(address);

        if (strBuf == null || strBuf.length < len) {
            strBuf = new char[len];
        }

        long lim = address + 4 + len * 2;
        int i = 0;
        for (long p = address + 4; p < lim; p += 2) {
            strBuf[i++] = Unsafe.getUnsafe().getChar(p);
        }

        return new String(strBuf, 0, len);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        long address = address() + getInt(col);
        for (long p = address + 4, n = address + 4 + Unsafe.getUnsafe().getInt(address) * 2; p < n; p += 2) {
            sink.put(Unsafe.getUnsafe().getChar(p));
        }
    }

    @Override
    public int getStrLen(int col) {
        return Unsafe.getUnsafe().getInt(address() + getInt(col));
    }

    protected abstract long address();
}
