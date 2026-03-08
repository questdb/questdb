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

import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK;

/**
 * An immutable flyweight for a UTF-8 string stored in a VARCHAR column. It may be
 * stored in two formats:
 * <br>
 * - fully inlined into the auxiliary vector (if up to 9 bytes). In this case, dataLo == prefixLo.
 * - fully stored in the data vector, plus the first 6 bytes in the auxiliary vector
 */
public class Utf8SplitString implements DirectUtf8Sequence, Mutable {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final StableStringSource stableSource;
    private boolean ascii;
    private long dataLim;
    private long dataLo;
    private long prefixLo;
    private int size;

    public Utf8SplitString() {
        this.stableSource = StableStringSource.UNSTABLE_SOURCE;
    }

    public Utf8SplitString(StableStringSource stableSource) {
        this.stableSource = stableSource;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(dataLo + index);
    }

    @Override
    public void clear() {
        this.prefixLo = this.dataLo = 0;
        this.ascii = false;
    }

    @Override
    public int intAt(int offset) {
        return Unsafe.getUnsafe().getInt(dataLo + offset);
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    @Override
    public boolean isStable() {
        return stableSource.isStable();
    }

    @Override
    public long longAt(int offset) {
        return Unsafe.getUnsafe().getLong(dataLo + offset);
    }

    /**
     * @param prefixLo address of the first UTF-8 byte of the prefix inlined into the auxiliary vector
     * @param dataLo   address of the first UTF-8 byte of the full string value.
     *                 When the full value is inlined into the auxiliary vector, this must be equal to prefixLo.
     * @param dataLim  end ptr of the contiguously addressable buffer containing the full value.
     *                 this is usually past the end of the full value, used to compute the `tailPadding` value.
     * @param size     size in bytes of the UTF-8 value
     * @param ascii    whether the value is all-ASCII
     * @return this
     */
    public Utf8SplitString of(long prefixLo, long dataLo, long dataLim, int size, boolean ascii) {
        if (dataLim < dataLo + size) {
            throw CairoException.critical(0)
                    .put("varchar is outside of file boundary [dataLim=")
                    .put(dataLim)
                    .put(", dataLo=")
                    .put(dataLo)
                    .put(", size=")
                    .put(size)
                    .put(']');
        }
        this.prefixLo = prefixLo;
        this.dataLo = dataLo;
        this.dataLim = dataLim;
        this.size = size;
        this.ascii = ascii;
        return this;
    }

    @Override
    public long ptr() {
        return dataLo;
    }

    @Override
    public short shortAt(int offset) {
        return Unsafe.getUnsafe().getShort(dataLo + offset);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long tailPadding() {
        return dataLim - dataLo - size;
    }

    @Override
    public @NotNull String toString() {
        Utf16Sink utf16Sink = Misc.getThreadLocalSink();
        Utf8s.utf8ToUtf16(this, utf16Sink);
        return utf16Sink.toString();
    }

    @Override
    public long zeroPaddedSixPrefix() {
        return Unsafe.getUnsafe().getLong(prefixLo) & VARCHAR_INLINED_PREFIX_MASK;
    }
}
