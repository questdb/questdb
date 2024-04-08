/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_BYTES;
import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK;

/**
 * An immutable flyweight for a UTF-8 string stored in native memory.
 */
public class Utf8SplitString implements DirectUtf8Sequence, Mutable {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final boolean stable;
    protected long dataLo;
    private boolean ascii;
    private long auxLo;
    private int size;

    public Utf8SplitString(boolean stable) {
        this.stable = stable;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte((index < VARCHAR_INLINED_PREFIX_BYTES ? auxLo : dataLo) + index);
    }

    @Override
    public void clear() {
        this.auxLo = this.dataLo = 0;
        this.ascii = false;
    }

    @Override
    public boolean equalsAssumingSameSize(Utf8Sequence other) {
        return zeroPaddedSixPrefix() == other.zeroPaddedSixPrefix() && dataEquals(other);
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    @Override
    public boolean isStable() {
        return stable;
    }

    @Override
    public long longAt(int offset) {
        return Unsafe.getUnsafe().getLong(dataLo + offset);
    }

    public Utf8SplitString of(long auxLo, long dataLo, int size, boolean ascii) {
        this.auxLo = auxLo;
        this.dataLo = dataLo;
        this.size = size;
        this.ascii = ascii;
        return this;
    }

    @Override
    public long ptr() {
        // Always return pointer to the data vector since it contains the full string.
        return dataLo;
    }

    @Override
    public int size() {
        return size;
    }

    @NotNull
    @Override
    public String toString() {
        Utf16Sink utf16Sink = Misc.getThreadLocalSink();
        Utf8s.utf8ToUtf16(this, utf16Sink);
        return utf16Sink.toString();
    }

    @Override
    public long zeroPaddedSixPrefix() {
        return Unsafe.getUnsafe().getLong(auxLo) & VARCHAR_INLINED_PREFIX_MASK;
    }

    private boolean dataEquals(Utf8Sequence other) {
        int i = VARCHAR_INLINED_PREFIX_BYTES;
        for (int n = size() - Long.BYTES + 1; i < n; i += Long.BYTES) {
            if (longAt(i) != other.longAt(i)) {
                return false;
            }
        }
        for (int n = size(); i < n; i++) {
            if (byteAt(i) != other.byteAt(i)) {
                return false;
            }
        }
        return true;
    }
}
