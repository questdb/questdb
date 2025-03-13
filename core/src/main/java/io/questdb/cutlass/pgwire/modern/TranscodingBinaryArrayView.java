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

package io.questdb.cutlass.pgwire.modern;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

public class TranscodingBinaryArrayView extends PGWireArrayView implements FlatArrayView, Mutable {
    private final MemoryAR mem;

    public TranscodingBinaryArrayView(MemoryAR mem) {
        this.mem = mem;
        this.flatViewLength = 1;
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA dst) {
        int size = this.flatViewLength;
        switch (elemType()) {
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
                // TODO optimize to Vect.memcpy()
                for (int i = 0; i < size; i++) {
                    dst.putLong(getLongAtAbsoluteIndex(i));
                }
                break;
            default:
                throw new UnsupportedOperationException("not implemented yet");
        }
    }

    @Override
    public void clear() {
        shape.clear();
        strides.clear();
        flatViewLength = 1;
        type = ColumnType.UNDEFINED;
    }

    @Override
    public short elemType() {
        return ColumnType.decodeArrayElementType(type);
    }

    @Override
    public double getDoubleAtAbsoluteIndex(int elemIndex) {
        return mem.getDouble((long) elemIndex * Double.BYTES);
    }

    @Override
    public long getLongAtAbsoluteIndex(int elemIndex) {
        return mem.getLong((long) elemIndex * Long.BYTES);
    }

    @Override
    public int length() {
        return flatViewLength;
    }

    @Override
    void setPtrAndCalculateStrides(long lo, long hi, int pgOidType) {
        short componentNativeType;
        switch (pgOidType) {
            case PGOids.PG_FLOAT8:
                componentNativeType = ColumnType.DOUBLE;
                for (int i = 0; i < flatViewLength; i++) {
                    assert lo + 4 <= hi;
                    int size = Numbers.bswap(Unsafe.getUnsafe().getInt(lo));
                    lo += 4;
                    if (size == -1) {
                        mem.putDouble(Double.NaN);
                        continue;
                    }
                    assert size == Double.BYTES;
                    assert lo + Double.BYTES <= hi;
                    double val = Double.longBitsToDouble(Numbers.bswap(Unsafe.getUnsafe().getLong(lo)));
                    lo += Double.BYTES;
                    mem.putDouble(val);
                }
                break;
            default:
                throw new UnsupportedOperationException("not implemented yet");
        }

        this.type = ColumnType.encodeArrayType(componentNativeType, shape.size());
        resetToDefaultStrides();
    }
}
