/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std.str;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

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

    public void shl(long delta) {
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
        return Chars.stringFromUtf8Bytes(lo, hi);
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
