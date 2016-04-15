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

package com.nfsdb.std;

import com.nfsdb.misc.Unsafe;

public class DirectByteCharSequence extends AbstractCharSequence implements Mutable, ByteSequence {
    public static final Factory FACTORY = new Factory();
    private long lo;
    private long hi;

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
