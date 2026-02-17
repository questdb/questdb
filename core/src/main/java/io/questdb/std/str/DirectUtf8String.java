/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.NotNull;

/**
 * A flyweight to an immutable UTF-8 string stored in native memory.
 */
public class DirectUtf8String implements DirectUtf8Sequence, Mutable {
    public static final Factory FACTORY = new Factory();
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private boolean ascii;
    private long hi;
    private long lo;

    /**
     * Trim the left by `count` bytes.
     */
    public void advance(int count) {
        this.lo += count;
        assert lo <= hi;
    }

    /**
     * Trim left by one byte.
     */
    public void advance() {
        advance(1);
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public void clear() {
        this.lo = this.hi = 0;
        this.ascii = false;
    }

    public DirectUtf8Sequence decHi() {
        this.hi--;
        return this;
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    public DirectUtf8String of(long lo, long hi) {
        return of(lo, hi, false);
    }

    public DirectUtf8String of(long lo, long hi, boolean ascii) {
        this.lo = lo;
        this.hi = hi;
        this.ascii = ascii;
        return this;
    }

    public DirectUtf8String of(DirectUtf8String value) {
        return of(value.lo(), value.hi());
    }

    @Override
    public long ptr() {
        return lo;
    }

    public void shl(long delta) {
        this.lo -= delta;
        this.hi -= delta;
    }

    @Override
    public int size() {
        return (int) (hi - lo);
    }

    public void squeeze() {
        this.lo++;
        this.hi--;
    }

    public void squeezeHi(long delta) {
        this.hi -= delta;
    }

    @NotNull
    @Override
    public String toString() {
        return Utf8s.stringFromUtf8Bytes(lo, hi);
    }

    public static final class Factory implements ObjectFactory<DirectUtf8String> {
        @Override
        public DirectUtf8String newInstance() {
            return new DirectUtf8String();
        }
    }
}
