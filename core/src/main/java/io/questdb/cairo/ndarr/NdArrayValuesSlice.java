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

package io.questdb.cairo.ndarr;

import io.questdb.std.Unsafe;
import io.questdb.std.bytes.DirectSequence;

/**
 * A flyweight to an immutable byte sequence containing flattened NdArray values.
 * <p><strong>IMPORTANT</strong>: The values are accessed by element index,
 * not by address offset.</p>
 */
public class NdArrayValuesSlice implements DirectSequence {
    private long ptr = 0;
    private int size = 0;

    public boolean getBoolean(int elemIndex) {
        assert elemIndex >= 0;
        final int byteIndex = elemIndex / 8;
        final int bitIndex = elemIndex % 8;
        final byte bits = getByte(byteIndex);
        return ((bits & (1 << bitIndex)) != 0);
    }

    public byte getByte(int elemIndex) {
        assert elemIndex >= 0;
        assert elemIndex < size;
        final long addr = ptr + elemIndex;
        return Unsafe.getUnsafe().getByte(addr);
    }

    public double getDouble(int elemIndex) {
        assert elemIndex >= 0;
        assert size % Double.BYTES == 0;
        assert ((elemIndex + 1) * Double.BYTES) <= size;
        final long addr = ptr + ((long) elemIndex * Double.BYTES);
        return Unsafe.getUnsafe().getDouble(addr);
    }

    public float getFloat(int elemIndex) {
        assert elemIndex >= 0;
        assert size % Float.BYTES == 0;
        assert ((elemIndex + 1) * Float.BYTES) <= size;
        final long addr = ptr + ((long) elemIndex * Float.BYTES);
        return Unsafe.getUnsafe().getFloat(addr);
    }

    public int getInt(int elemIndex) {
        assert elemIndex >= 0;
        assert size % Integer.BYTES == 0;
        assert ((elemIndex + 1) * Integer.BYTES) <= size;
        final long addr = ptr + ((long) elemIndex * Integer.BYTES);
        return Unsafe.getUnsafe().getInt(addr);
    }

    public long getLong(int elemIndex) {
        assert elemIndex >= 0;
        assert size % Long.BYTES == 0;
        assert ((elemIndex + 1) * Long.BYTES) <= size;
        final long addr = ptr + ((long) elemIndex * Long.BYTES);
        return Unsafe.getUnsafe().getLong(addr);
    }

    public short getShort(int elemIndex) {
        assert elemIndex >= 0;
        assert size % Short.BYTES == 0;
        assert ((elemIndex + 1) * Short.BYTES) <= size;
        final long addr = ptr + ((long) elemIndex * Short.BYTES);
        return Unsafe.getUnsafe().getShort(addr);
    }

    public NdArrayValuesSlice of(long ptr, int size) {
        assert ptr > 0;
        assert size > 0;
        this.ptr = ptr;
        this.size = size;
        return this;
    }

    /**
     * Address of the start of the buffer
     */
    @Override
    public long ptr() {
        assert ptr != 0;
        return ptr;
    }

    /**
     * Invalidate
     */
    public void reset() {
        ptr = 0;
        size = 0;
    }

    /**
     * Buffer size, as byte count.
     */
    @Override
    public int size() {
        return size;
    }
}
