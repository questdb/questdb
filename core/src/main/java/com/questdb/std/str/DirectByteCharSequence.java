/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.std.Chars;
import com.questdb.std.Mutable;
import com.questdb.std.ObjectFactory;
import com.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class DirectByteCharSequence extends AbstractCharSequence implements Mutable, ByteSequence, DirectBytes {
    public static final Factory FACTORY = new Factory();
    private long lo;
    private long hi;

    @Override
    public long address() {
        return lo;
    }

    @Override
    public int byteLength() {
        return length();
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(lo + index);
    }

    @Override
    public void clear() {
        this.lo = this.hi = 0;
    }

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    @Override
    public int length() {
        return (int) (hi - lo);
    }

    @Override
    public char charAt(int index) {
        return (char) byteAt(index);
    }

    public void lshift(long delta) {
        this.lo -= delta;
        this.hi -= delta;
    }

    public DirectByteCharSequence of(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
        return this;
    }

    @NotNull
    @Override
    public String toString() {
        return Chars.toUtf8String(lo, hi);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        DirectByteCharSequence seq = new DirectByteCharSequence();
        seq.lo = this.lo + start;
        seq.hi = this.lo + end;
        return seq;
    }

    public static final class Factory implements ObjectFactory<DirectByteCharSequence> {
        @Override
        public DirectByteCharSequence newInstance() {
            return new DirectByteCharSequence();
        }
    }
}
