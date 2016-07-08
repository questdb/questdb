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

package com.questdb.std;

import com.questdb.misc.Chars;
import com.questdb.misc.Unsafe;

public class DirectCharSequence extends AbstractCharSequence {
    private long lo;
    private long hi;
    private int len;

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    @Override
    public int hashCode() {
        if (lo == hi) {
            return 0;
        }

        int h = 0;
        for (long p = lo; p < hi; p += 2) {
            h = 31 * h + Unsafe.getUnsafe().getChar(p);
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof CharSequence && Chars.equals(this, (CharSequence) obj);
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return Unsafe.getUnsafe().getChar(lo + (index << 1));
    }

    public DirectCharSequence of(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
        this.len = (int) ((hi - lo) / 2);
        return this;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        DirectCharSequence seq = new DirectCharSequence();
        seq.lo = this.lo + start;
        seq.hi = this.lo + end;
        return seq;
    }
}
