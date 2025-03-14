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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

final class PgNonNullBinaryArrayView extends PGWireArrayView implements FlatArrayView, Mutable {
    private long hi;
    private long lo;

    public PgNonNullBinaryArrayView() {
        this.flatViewLength = 1;
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem) {
        int size = this.flatViewLength;
        switch (ColumnType.decodeArrayElementType(type)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
                for (int i = 0; i < size; i++) {
                    mem.putLong(getLong(i));
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
        lo = 0;
        hi = 0;
        type = ColumnType.UNDEFINED;
    }

    @Override
    public short elemType() {
        return ColumnType.decodeArrayElementType(type);
    }

    @Override
    public double getDoubleAtAbsoluteIndex(int flatIndex) {
        final long addr = lo + Integer.BYTES + ((long) flatIndex * (Double.BYTES + Integer.BYTES));
        assert addr < hi;
        long networkOrderVal = Unsafe.getUnsafe().getLong(addr);
        return Double.longBitsToDouble(Numbers.bswap(networkOrderVal));
    }

    @Override
    public long getLongAtAbsoluteIndex(int flatIndex) {
        final long addr = lo + Integer.BYTES + ((long) flatIndex * (Long.BYTES + Integer.BYTES));
        assert addr < hi;
        long networkOrderVal = Unsafe.getUnsafe().getLong(addr);
        return Numbers.bswap(networkOrderVal);
    }

    @Override
    public int length() {
        return 0;
    }

    @Override
    void setPtrAndCalculateStrides(long lo, long hi, int pgOidType) {
        short componentNativeType;
        int elementSize;
        switch (pgOidType) {
            case PGOids.PG_INT8:
                componentNativeType = ColumnType.LONG;
                elementSize = Long.BYTES;
                break;
            case PGOids.PG_FLOAT8:
                componentNativeType = ColumnType.DOUBLE;
                elementSize = Double.BYTES;
                break;
            default:
                throw new UnsupportedOperationException("not implemented yet");
        }

        // validate that there are no nulls in the array
        for (long p = lo; p < hi; p += Integer.BYTES + elementSize) {
            // non need to swap bytes since -1 is always -1, regardless of endianness
            if (Unsafe.getUnsafe().getInt(p) == -1) {
                throw CairoException.nonCritical().put("nulls not supported in arrays");
            }
        }
        this.lo = lo;
        this.hi = hi;
        this.type = ColumnType.encodeArrayType(componentNativeType, shape.size());
        resetToDefaultStrides();
    }
}
