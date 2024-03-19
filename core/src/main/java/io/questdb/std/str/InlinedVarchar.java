/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class InlinedVarchar implements Utf8Sequence {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private byte lastByteMask;

    private long ptr;
    private byte size;
    private long valueMask;
    private boolean isAscii;

    public InlinedVarchar of(long ptr, byte size, boolean isAscii) {
        this.ptr = ptr;
        this.size = size;
        this.isAscii = isAscii;
        this.valueMask = size < 8 ? (1L << 8 * size) - 1 : -1L;
        return this;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        // mask should be 0xff for index < size, and 0 for index >= size
        byte mask = (byte) (index - size >> 24);
        return (byte) (Unsafe.getUnsafe().getByte(ptr + index) & mask);
    }

    @Override
    public long zeroPaddedLongAt(int index) {
        assert index == 0 : String.format("index %,d != 0", index);
        return Unsafe.getUnsafe().getLong(ptr) & valueMask;
    }

    @Override
    public boolean equalsAssumingSameSize(Utf8Sequence other) {
        return zeroPaddedLongAt(0) == other.zeroPaddedLongAt(0) && (size <= 8 || byteAt(8) == other.byteAt(8));
    }

    @Override
    public boolean isAscii() {
        return isAscii;
    }

    @Override
    public int size() {
        return size;
    }

    @NotNull @Override
    public String toString() {
        return Utf8s.stringFromUtf8Bytes(ptr, ptr + size);
    }
}
