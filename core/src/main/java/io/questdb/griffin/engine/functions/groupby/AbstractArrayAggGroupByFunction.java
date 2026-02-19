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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Shared base for element-wise array aggregate GROUP BY functions (amax, amin, asum).
 * <p>
 * Per-group state in MapValue uses 2 LONG slots:
 * <pre>
 * valueIndex:     LONG → ptr to compact block [int dataSize][int dim0][double acc[N]]
 * valueIndex + 1: LONG → N (accumulator length)
 * </pre>
 * ptr == 0 means all inputs were NULL so far (result is NULL).
 * Uninitialized positions use Double.NaN as sentinel for "no finite value seen".
 */
public abstract class AbstractArrayAggGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {
    private static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES; // dataSize + dim0
    protected final Function arg;
    private final BorrowedArray borrowedArray = new BorrowedArray();
    private final int nDims;
    private GroupByAllocator allocator;
    private int valueIndex;

    public AbstractArrayAggGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.type = arg.getType();
        this.nDims = ColumnType.decodeArrayDimensionality(type);
    }

    @Override
    public void close() {
        Misc.free(arg);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        ArrayView array = arg.getArray(record);
        if (array == null || array.isNull() || array.getFlatViewLength() == 0) {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
            return;
        }
        int n = array.getFlatViewLength();
        long blockSize = HEADER_SIZE + (long) n * Double.BYTES;
        long ptr = allocator.malloc(blockSize);
        int dataSize = n * Double.BYTES;
        Unsafe.getUnsafe().putInt(ptr, dataSize);
        Unsafe.getUnsafe().putInt(ptr + Integer.BYTES, n);
        long dataPtr = ptr + HEADER_SIZE;
        for (int i = 0; i < n; i++) {
            Unsafe.getUnsafe().putDouble(dataPtr + (long) i * Double.BYTES, array.getDouble(i));
        }
        mapValue.putLong(valueIndex, ptr);
        mapValue.putLong(valueIndex + 1, n);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        ArrayView array = arg.getArray(record);
        if (array == null || array.isNull() || array.getFlatViewLength() == 0) {
            return;
        }
        long ptr = mapValue.getLong(valueIndex);
        if (ptr == 0) {
            computeFirst(mapValue, record, rowId);
            return;
        }
        int accLen = (int) mapValue.getLong(valueIndex + 1);
        int inputLen = array.getFlatViewLength();
        if (inputLen > accLen) {
            long oldBlockSize = HEADER_SIZE + (long) accLen * Double.BYTES;
            long newBlockSize = HEADER_SIZE + (long) inputLen * Double.BYTES;
            ptr = allocator.realloc(ptr, oldBlockSize, newBlockSize);
            int dataSize = inputLen * Double.BYTES;
            Unsafe.getUnsafe().putInt(ptr, dataSize);
            Unsafe.getUnsafe().putInt(ptr + Integer.BYTES, inputLen);
            long dataPtr = ptr + HEADER_SIZE;
            for (int i = accLen; i < inputLen; i++) {
                Unsafe.getUnsafe().putDouble(dataPtr + (long) i * Double.BYTES, Double.NaN);
            }
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, inputLen);
        }
        long dataPtr = ptr + HEADER_SIZE;
        for (int i = 0; i < inputLen; i++) {
            double inputVal = array.getDouble(i);
            if (Numbers.isFinite(inputVal)) {
                long addr = dataPtr + (long) i * Double.BYTES;
                double accVal = Unsafe.getUnsafe().getDouble(addr);
                if (Numbers.isFinite(accVal)) {
                    Unsafe.getUnsafe().putDouble(addr, combine(accVal, inputVal));
                } else {
                    Unsafe.getUnsafe().putDouble(addr, inputVal);
                }
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public ArrayView getArray(Record rec) {
        long ptr = rec.getLong(valueIndex + 1);
        if (ptr == 0) {
            ptr = rec.getLong(valueIndex);
        } else {
            ptr = rec.getLong(valueIndex);
        }
        if (ptr == 0) {
            return ArrayConstant.NULL;
        }
        ArrayTypeDriver.getCompactPlainValue(ptr, type, nDims, borrowedArray);
        return borrowedArray;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // ptr
        columnTypes.add(ColumnType.LONG); // length
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0) {
            destValue.putLong(valueIndex, srcPtr);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            return;
        }
        int destLen = (int) destValue.getLong(valueIndex + 1);
        int srcLen = (int) srcValue.getLong(valueIndex + 1);
        if (srcLen > destLen) {
            long oldBlockSize = HEADER_SIZE + (long) destLen * Double.BYTES;
            long newBlockSize = HEADER_SIZE + (long) srcLen * Double.BYTES;
            destPtr = allocator.realloc(destPtr, oldBlockSize, newBlockSize);
            Unsafe.getUnsafe().putInt(destPtr, srcLen * Double.BYTES);
            Unsafe.getUnsafe().putInt(destPtr + Integer.BYTES, srcLen);
            long dataPtr = destPtr + HEADER_SIZE;
            for (int i = destLen; i < srcLen; i++) {
                Unsafe.getUnsafe().putDouble(dataPtr + (long) i * Double.BYTES, Double.NaN);
            }
            destValue.putLong(valueIndex, destPtr);
            destValue.putLong(valueIndex + 1, srcLen);
        }
        long destDataPtr = destPtr + HEADER_SIZE;
        long srcDataPtr = srcPtr + HEADER_SIZE;
        for (int i = 0; i < srcLen; i++) {
            double srcVal = Unsafe.getUnsafe().getDouble(srcDataPtr + (long) i * Double.BYTES);
            if (Numbers.isFinite(srcVal)) {
                long addr = destDataPtr + (long) i * Double.BYTES;
                double destVal = Unsafe.getUnsafe().getDouble(addr);
                if (Numbers.isFinite(destVal)) {
                    Unsafe.getUnsafe().putDouble(addr, combine(destVal, srcVal));
                } else {
                    Unsafe.getUnsafe().putDouble(addr, srcVal);
                }
            }
        }
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    protected abstract double combine(double accVal, double inputVal);
}
