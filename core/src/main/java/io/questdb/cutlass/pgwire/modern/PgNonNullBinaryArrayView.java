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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

final class PgNonNullBinaryArrayView implements ArrayView, Mutable {
    private final IntList dimSizes = new IntList();
    private final IntList strides = new IntList();
    private long hi;
    private long lo;
    private int size = 1;
    private int type;

    @Override
    public void appendWithDefaultStrides(MemoryA mem) {
        switch (ColumnType.decodeArrayElementType(type)) {
            case ColumnType.LONG:
                for (int i = 0, n = size; i < n; i++) {
                    mem.putLong(getLongAtFlatIndex(i));
                }
                break;
            case ColumnType.DOUBLE:
                for (int i = 0, n = size; i < n; i++) {
                    mem.putDouble(getDoubleAtFlatIndex(i));
                }
                break;
            default:
                throw new UnsupportedOperationException("not implemented yet");
        }
    }

    @Override
    public void clear() {
        dimSizes.clear();
        strides.clear();
        size = 1;
        lo = 0;
        hi = 0;
        type = ColumnType.UNDEFINED;
    }

    @Override
    public int getDimCount() {
        return dimSizes.size();
    }

    @Override
    public int getDimLen(int dimension) {
        return dimSizes.getQuick(dimension);
    }

    @Override
    public double getDoubleAtFlatIndex(int flatIndex) {
        final long addr = lo + Integer.BYTES + ((long) flatIndex * (Double.BYTES + Integer.BYTES));
        assert addr < hi;
        long networkOrderVal = Unsafe.getUnsafe().getLong(addr);
        return Double.longBitsToDouble(Numbers.bswap(networkOrderVal));
    }

    @Override
    public int getFlatElemCount() {
        return size;
    }

    @Override
    public long getLongAtFlatIndex(int flatIndex) {
        final long addr = lo + Integer.BYTES + ((long) flatIndex * (Long.BYTES + Integer.BYTES));
        assert addr < hi;
        long networkOrderVal = Unsafe.getUnsafe().getLong(addr);
        return Numbers.bswap(networkOrderVal);
    }

    @Override
    public int getStride(int dimension) {
        return strides.getQuick(dimension);
    }

    @Override
    public int getType() {
        return type;
    }

    void addDimSize(int size) {
        dimSizes.add(size);
        this.size *= size;
    }

    void setPtrAndCalculateStrides(long lo, long hi, int pgOidType) {
        short componentNativeType;
        switch (pgOidType) {
            case PGOids.PG_INT8:
                componentNativeType = ColumnType.LONG;
                break;
            case PGOids.PG_FLOAT8:
                componentNativeType = ColumnType.DOUBLE;
                break;
            default:
                throw new UnsupportedOperationException("not implemented yet");
        }

        strides.clear();
        int stride = 1;
        for (int i = dimSizes.size() - 1; i > 0; i--) {
            strides.add(stride);
            stride *= dimSizes.getQuick(i);
        }
        strides.add(stride);
        this.lo = lo;
        this.hi = hi;
        this.type = ColumnType.encodeArrayType(componentNativeType, dimSizes.size());
    }
}
