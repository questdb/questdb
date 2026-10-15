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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByBatchKernels;
import io.questdb.griffin.engine.groupby.GroupByDoubleList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.Numbers.LONG_NULL;

public class PercentileDiscDoubleGroupByFunction extends DoubleFunction implements UnaryFunction, GroupByFunction {
    // the list slot of a group whose result finalizeValue() has put in the slot after it
    protected static final long FINALIZED = -1;
    // groups above this size select by radix, linear whatever their values' distribution
    protected static final int RADIX_SELECT_MIN_SIZE = 64 * 1024;
    protected final Function arg;
    protected final GroupByDoubleList listA;
    protected final GroupByDoubleList listB;
    protected final Function percentileFunc;
    protected final int percentilePos;
    protected int valueIndex;

    public PercentileDiscDoubleGroupByFunction(@NotNull CairoConfiguration configuration, @NotNull Function arg, @NotNull Function percentileFunc, int percentilePos) {
        this.arg = arg;
        this.percentileFunc = percentileFunc;
        int initialCapacity = 4;
        listA = new GroupByDoubleList(initialCapacity, Double.NaN);
        listB = new GroupByDoubleList(initialCapacity, Double.NaN);
        this.percentilePos = percentilePos;
    }

    @Override
    public void clear() {
        listA.resetPtr();
        listB.resetPtr();
    }

    @Override
    public void close() {
        Misc.free(arg);
        Misc.free(percentileFunc);
    }

    @Override
    public void computeBatchKernel(MapValue mapValue, int rowCount, GroupByBatchKernels.Args args) {
        // computeNext() per row
        final long valuesAddr = args.address(0);
        long listPtr = mapValue.getLong(valueIndex);
        for (int i = 0; i < rowCount; i++) {
            final double value = Unsafe.getDouble(valuesAddr + ((long) i << 3));
            if (Numbers.isFinite(value)) {
                listPtr = append(listPtr, value);
            }
        }
        mapValue.putLong(valueIndex, listPtr);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            listA.of(0).add(value);
            mapValue.putLong(valueIndex, listA.ptr());
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            listA.of(mapValue.getLong(valueIndex));
            listA.add(value);
            mapValue.putLong(valueIndex, listA.ptr());
        }
    }

    @Override
    public void computeKeyedBatchKernel(
            FlyweightPackedMapValue mapValue,
            long baseValueAddr,
            long batchAddr,
            int rowCount,
            GroupByBatchKernels.Args args
    ) {
        // computeFirst() / computeNext() per row: a new entry's slot holds the empty list, 0, as
        // computeFirst() leaves it for a value it does not take
        final long valuesAddr = args.address(0);
        final long slotOffset = mapValue.getOffset(valueIndex);
        for (int i = 0; i < rowCount; i++) {
            final double value = Unsafe.getDouble(valuesAddr + ((long) i << 3));
            if (Numbers.isFinite(value)) {
                final long slot = baseValueAddr + Map.decodeBatchOffset(Unsafe.getLong(batchAddr + ((long) i << 3))) + slotOffset;
                Unsafe.putLong(slot, append(Unsafe.getLong(slot), value));
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public Function getBatchKernelArg(int index) {
        return arg;
    }

    @Override
    public int getBatchKernelArgCount() {
        return 1;
    }

    @Override
    public int getBatchKernelArgType(int index) {
        return ColumnType.DOUBLE;
    }

    @Override
    public void finalizeValue(MapValue mapValue) {
        final long listPtr = mapValue.getLong(valueIndex);
        if (listPtr <= 0) {
            return;
        }
        listA.of(listPtr);
        final int size = listA.size();
        final double percentile = percentileFunc.getDouble(null);
        // an invalid percentile is left to the getter, to report
        if (size == 0 || Numbers.isNull(percentile) || percentile < 0 || percentile > 1) {
            return;
        }
        mapValue.putDouble(valueIndex + 1, select(size, percentile));
        mapValue.putLong(valueIndex, FINALIZED);
    }

    @Override
    public double getDouble(Record record) {
        final long listPtr = record.getLong(valueIndex);
        if (listPtr == FINALIZED) {
            return record.getDouble(valueIndex + 1);
        }
        if (listPtr <= 0) {
            return Double.NaN;
        }
        listA.of(listPtr);
        final int size = listA.size();
        if (size == 0) {
            return Double.NaN;
        }
        return select(size, SqlUtil.validatePercentile(percentileFunc.getDouble(record), percentilePos));
    }

    @Override
    public String getName() {
        return "percentile_disc";
    }

    @Override
    public boolean isFinalizable() {
        return percentileFunc.isConstant() || percentileFunc.isRuntimeConstant();
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext sqlExecutionContext) throws SqlException {
        super.init(symbolTableSource, sqlExecutionContext);
        arg.init(symbolTableSource, sqlExecutionContext);
        percentileFunc.init(symbolTableSource, sqlExecutionContext);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // the list of the group's values, or FINALIZED
        columnTypes.add(ColumnType.DOUBLE); // the result, once FINALIZED
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isOrderSensitive() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr <= 0) {
            return; // source is null/empty, nothing to merge
        }

        final long destPtr = destValue.getLong(valueIndex);
        if (destPtr <= 0) {
            destValue.putLong(valueIndex, srcPtr);
            return; // destination is null/empty, take source as-is
        }

        listA.of(destPtr);
        listB.of(srcPtr);

        final long outPtr = listA.size() > listB.size() ? listA.add(listB) : listB.add(listA);
        destValue.putLong(valueIndex, outPtr);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        listA.setAllocator(allocator);
        listB.setAllocator(allocator);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("percentile_disc(").val(arg).val(',').val(percentileFunc).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }

    // the percentile of the size values of listA
    protected double select(int size, double percentile) {
        final int n = (int) SqlUtil.getPercentileDiscIndex(percentile, size);
        if (size > RADIX_SELECT_MIN_SIZE) {
            return listA.radixSelect(0, size - 1, n);
        }
        // Use QuickSelect instead of full sorting - O(n) vs O(n log n)
        listA.quickSelect(0, size - 1, n);
        return listA.getQuick(n);
    }

    // the list at listPtr (0 for none) with the value appended, maybe moved
    private long append(long listPtr, double value) {
        if (listPtr > 0 && GroupByDoubleList.tryAppend(listPtr, value)) {
            return listPtr;
        }
        listA.of(Math.max(listPtr, 0)).add(value);
        return listA.ptr();
    }
}
