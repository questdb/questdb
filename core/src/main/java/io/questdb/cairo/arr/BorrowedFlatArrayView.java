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

package io.questdb.cairo.arr;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Unsafe;

/**
 * Immutable view over the backing native memory of an array. Does not own the memory.
 * <p>
 * It can access an array of any element type. This means that there isn't one definite
 * length of the array it represents -- it depends on the assumed element type.
 */
public final class BorrowedFlatArrayView implements FlatArrayView {
    private short elemType = ColumnType.UNDEFINED;
    private int length;
    private long ptr;
    private int size;

    @Override
    public void appendToMemFlat(MemoryA mem) {
        assert ptr != 0;
        mem.putBlockOfBytes(ptr, size);
    }

    @Override
    public short elemType() {
        return elemType;
    }

    @Override
    public double getDouble(int elemIndex) {
        assert elemIndex >= 0 && elemIndex < length;
        final long addr = ptr + ((long) elemIndex * Double.BYTES);
        return Unsafe.getUnsafe().getDouble(addr);
    }

    @Override
    public long getLong(int elemIndex) {
        assert elemIndex >= 0 && elemIndex < length;
        final long addr = ptr + ((long) elemIndex * Long.BYTES);
        return Unsafe.getUnsafe().getLong(addr);
    }

    @Override
    public int length() {
        return length;
    }

    public BorrowedFlatArrayView of(long ptr, short elemType, int length) {
        assert ptr > 0 : "ptr <= 0";
        assert length > 0 : "length <= 0";
        this.elemType = elemType;
        this.ptr = ptr;
        this.length = length;
        this.size = length * ColumnType.sizeOf(elemType);
        return this;
    }

    public long ptr() {
        return this.ptr;
    }

    public void reset() {
        this.ptr = 0;
        this.length = 0;
        this.size = 0;
    }

    /**
     * Subtracts the delta from the memory pointer. Called by the same-named method
     * in {@link io.questdb.cutlass.line.tcp.LineTcpParser#shl ILP parser}.
     */
    public void shl(long delta) {
        this.ptr -= delta;
    }

    public int size() {
        return this.size;
    }
}
