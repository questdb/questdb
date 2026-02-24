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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Concatenates {@code DOUBLE[]} arrays into a single flat {@code DOUBLE[]} during GROUP BY / SAMPLE BY.
 * <p>
 * Buffer layout in native memory (managed by {@link GroupByAllocator}):
 * <pre>
 * | count: INT (4 bytes) | capacity: INT (4 bytes) | d0: 8 bytes | d1: 8 bytes | ...
 * </pre>
 * The count field at offset 0 doubles as the shape descriptor for {@link BorrowedArray}.
 * <p>
 * A single LONG slot in the map value stores the buffer pointer (0 = null/empty group).
 */
public class ArrayAggDoubleArrayGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {
    private static final int CAPACITY_OFFSET = Integer.BYTES;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    private static final int INITIAL_CAPACITY = 16;
    private final Function arg;
    private final BorrowedArray borrowedArray = new BorrowedArray();
    private final boolean ordered;
    private GroupByAllocator allocator;
    private int valueIndex;

    public ArrayAggDoubleArrayGroupByFunction(@NotNull Function arg, boolean ordered) {
        this.arg = arg;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.ordered = ordered;
    }

    @Override
    public void close() {
        Misc.free(arg);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        ArrayView arr = arg.getArray(record);
        if (arr.isNull()) {
            mapValue.putLong(valueIndex, 0);
            return;
        }
        int len = arr.getFlatViewLength();
        if (len == 0) {
            mapValue.putLong(valueIndex, 0);
            return;
        }
        int capacity = Math.max(INITIAL_CAPACITY, Numbers.ceilPow2(len));
        long ptr = allocator.malloc(HEADER_SIZE + (long) capacity * Double.BYTES);
        Unsafe.getUnsafe().putInt(ptr, len);
        Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, capacity);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + (long) i * Double.BYTES, arr.getDouble(i));
        }
        mapValue.putLong(valueIndex, ptr);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        ArrayView arr = arg.getArray(record);
        if (arr.isNull()) {
            return;
        }
        int len = arr.getFlatViewLength();
        if (len == 0) {
            return;
        }
        long ptr = mapValue.getLong(valueIndex);
        if (ptr == 0) {
            // First non-null array in this group (previous rows were all null).
            int capacity = Math.max(INITIAL_CAPACITY, Numbers.ceilPow2(len));
            ptr = allocator.malloc(HEADER_SIZE + (long) capacity * Double.BYTES);
            Unsafe.getUnsafe().putInt(ptr, len);
            Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, capacity);
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + (long) i * Double.BYTES, arr.getDouble(i));
            }
            mapValue.putLong(valueIndex, ptr);
            return;
        }
        int count = Unsafe.getUnsafe().getInt(ptr);
        int capacity = Unsafe.getUnsafe().getInt(ptr + CAPACITY_OFFSET);
        int newCount = count + len;
        if (newCount < 0) {
            throw CairoException.nonCritical().put("array_agg: array exceeds maximum capacity");
        }
        if (newCount > capacity) {
            int newCapacity = Numbers.ceilPow2(newCount);
            if (newCapacity < newCount) {
                throw CairoException.nonCritical().put("array_agg: array exceeds maximum capacity");
            }
            long oldSize = HEADER_SIZE + (long) capacity * Double.BYTES;
            long newSize = HEADER_SIZE + (long) newCapacity * Double.BYTES;
            ptr = allocator.realloc(ptr, oldSize, newSize);
            Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, newCapacity);
            mapValue.putLong(valueIndex, ptr);
        }
        long base = ptr + HEADER_SIZE + (long) count * Double.BYTES;
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(base + (long) i * Double.BYTES, arr.getDouble(i));
        }
        Unsafe.getUnsafe().putInt(ptr, newCount);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public ArrayView getArray(Record rec) {
        long ptr = rec.getLong(valueIndex);
        if (ptr == 0) {
            return ArrayConstant.NULL;
        }
        int count = Unsafe.getUnsafe().getInt(ptr);
        if (count == 0) {
            return ArrayConstant.NULL;
        }
        return borrowedArray.of(type, ptr, ptr + HEADER_SIZE, count * Double.BYTES);
    }

    @Override
    public String getName() {
        return "array_agg";
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
        columnTypes.add(ColumnType.LONG); // buffer pointer
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
        int srcCount = Unsafe.getUnsafe().getInt(srcPtr);
        if (srcCount == 0) {
            return;
        }

        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0) {
            destValue.putLong(valueIndex, srcPtr);
            return;
        }
        int destCount = Unsafe.getUnsafe().getInt(destPtr);
        if (destCount == 0) {
            destValue.putLong(valueIndex, srcPtr);
            return;
        }

        int destCapacity = Unsafe.getUnsafe().getInt(destPtr + CAPACITY_OFFSET);
        int newCount = destCount + srcCount;
        if (newCount < 0) {
            throw CairoException.nonCritical().put("array_agg: merged array exceeds maximum capacity");
        }
        if (newCount > destCapacity) {
            int newCapacity = Numbers.ceilPow2(newCount);
            if (newCapacity < newCount) {
                throw CairoException.nonCritical().put("array_agg: merged array exceeds maximum capacity");
            }
            long oldSize = HEADER_SIZE + (long) destCapacity * Double.BYTES;
            long newSize = HEADER_SIZE + (long) newCapacity * Double.BYTES;
            destPtr = allocator.realloc(destPtr, oldSize, newSize);
            Unsafe.getUnsafe().putInt(destPtr + CAPACITY_OFFSET, newCapacity);
            destValue.putLong(valueIndex, destPtr);
        }
        Unsafe.getUnsafe().copyMemory(
                srcPtr + HEADER_SIZE,
                destPtr + HEADER_SIZE + (long) destCount * Double.BYTES,
                (long) srcCount * Double.BYTES
        );
        Unsafe.getUnsafe().putInt(destPtr, newCount);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return !ordered;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("array_agg(").val(arg).val(')');
    }
}
