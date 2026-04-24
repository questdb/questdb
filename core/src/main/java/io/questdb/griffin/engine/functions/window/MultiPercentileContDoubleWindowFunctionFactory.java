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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.DoubleSort;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;

public class MultiPercentileContDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final ArrayColumnTypes COLUMN_TYPES = new ArrayColumnTypes();
    private static final String NAME = "percentile_cont";
    private static final String SIGNATURE = NAME + "(DD[])";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arg = args.getQuick(0);
        Function percentilesFunc = args.getQuick(1);
        int percentilesPos = argPositions.getQuick(1);

        if (!percentilesFunc.isConstant()) {
            throw SqlException.$(percentilesPos, "percentile argument must be a constant");
        }

        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, false);

        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        // Percentile functions only support default frame over whole partition
        // Default frame is whole partition when there's no ORDER BY
        if (!windowContext.isDefaultFrame() || windowContext.isOrdered()) {
            throw SqlException.$(position, "percentile_cont window function only supports whole partition frames");
        }

        if (partitionByRecord != null) {
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    partitionByKeyTypes,
                    COLUMN_TYPES
            );
            return new MultiPercentileContOverPartitionFunction(
                    map,
                    partitionByRecord,
                    partitionBySink,
                    arg,
                    percentilesFunc,
                    percentilesPos,
                    configuration
            );
        } else {
            return new MultiPercentileContOverWholeResultSetFunction(arg, percentilesFunc, percentilesPos, configuration);
        }
    }

    // Handles percentile_cont() over (partition by x) with multiple percentiles
    static class MultiPercentileContOverPartitionFunction extends BasePartitionedWindowFunction implements WindowArrayFunction {
        private static final long CAPACITY_OFFSET = 0;
        private static final long DATA_OFFSET = 16;
        private static final int INITIAL_CAPACITY = 16;
        private static final long SIZE_OFFSET_BLOCK = 8;

        private final MemoryARW listMemory;
        private final Function percentilesFunc;
        private final int percentilesPos;
        private final MemoryARW resultMemory;
        private final int type;
        private DirectArray result;

        public MultiPercentileContOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                Function percentilesFunc,
                int percentilesPos,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.percentilesFunc = percentilesFunc;
            this.percentilesPos = percentilesPos;
            this.listMemory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            this.resultMemory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(percentilesFunc);
            Misc.free(listMemory);
            Misc.free(resultMemory);
            Misc.free(result);
        }

        @Override
        public ArrayView getArray(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            if (value != null) {
                long resultPtr = value.getLong(0);
                long resultCount = resultMemory.getLong(resultPtr);

                if (result == null) {
                    result = new DirectArray();
                    result.setType(type);
                }

                result.setDimLen(0, (int) resultCount);
                result.applyShape();

                for (int i = 0; i < resultCount; i++) {
                    result.putDouble(i, resultMemory.getDouble(resultPtr + 8 + i * 8L));
                }
                return result;
            }

            if (result == null) {
                result = new DirectArray();
                result.setType(type);
            }
            result.ofNull();
            return result;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void init(io.questdb.cairo.sql.SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            percentilesFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                long listPtr;

                if (value.isNew()) {
                    // Allocate: [capacity(8) | size(8) | data(INITIAL_CAPACITY * 8)]
                    long bytes = DATA_OFFSET + INITIAL_CAPACITY * 8L;
                    listPtr = listMemory.appendAddressFor(bytes) - listMemory.getPageAddress(0);
                    listMemory.putLong(listPtr + CAPACITY_OFFSET, INITIAL_CAPACITY);
                    listMemory.putLong(listPtr + SIZE_OFFSET_BLOCK, 1);
                    listMemory.putDouble(listPtr + DATA_OFFSET, d);
                } else {
                    listPtr = value.getLong(0);
                    long size = listMemory.getLong(listPtr + SIZE_OFFSET_BLOCK);
                    long capacity = listMemory.getLong(listPtr + CAPACITY_OFFSET);

                    if (size >= capacity) {
                        // Grow: allocate 2x capacity, copy values via memcpy, abandon old block
                        long newCapacity = capacity * 2;
                        long bytes = DATA_OFFSET + newCapacity * 8L;
                        long newPtr = listMemory.appendAddressFor(bytes) - listMemory.getPageAddress(0);
                        listMemory.putLong(newPtr + CAPACITY_OFFSET, newCapacity);
                        listMemory.putLong(newPtr + SIZE_OFFSET_BLOCK, size + 1);
                        long baseAddr = listMemory.getPageAddress(0);
                        Vect.memcpy(baseAddr + newPtr + DATA_OFFSET, baseAddr + listPtr + DATA_OFFSET, size * 8);
                        listMemory.putDouble(newPtr + DATA_OFFSET + size * 8, d);
                        listPtr = newPtr;
                    } else {
                        // Append in-place: capacity allows it
                        listMemory.putDouble(listPtr + DATA_OFFSET + size * 8, d);
                        listMemory.putLong(listPtr + SIZE_OFFSET_BLOCK, size + 1);
                    }
                }

                value.putLong(0, listPtr);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            // Write the array result to the WindowSPI at the correct column position
            spi.putArray(recordOffset, columnIndex, getArray(record));
        }

        @Override
        public void preparePass2() {
            // Sort all lists and calculate percentiles for each percentile value
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();

            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                long listPtr = value.getLong(0);
                long size = listMemory.getLong(listPtr + SIZE_OFFSET_BLOCK);

                if (size > 0) {
                    // Get percentiles array
                    ArrayView percentiles = percentilesFunc.getArray(record);
                    FlatArrayView view = percentiles.flatView();
                    int percentileCount = view.length();

                    // Sort the list (values start at listPtr + DATA_OFFSET)
                    DoubleSort.sort(listMemory.getPageAddress(0) + listPtr + DATA_OFFSET, 0, size - 1);

                    // Allocate result array: count (8 bytes) + percentile values
                    long resultPtr = resultMemory.appendAddressFor(8 + percentileCount * 8L) - resultMemory.getPageAddress(0);
                    resultMemory.putLong(resultPtr, percentileCount);

                    // Calculate each percentile with interpolation
                    for (int i = 0; i < percentileCount; i++) {
                        double percentile = view.getDoubleAtAbsIndex(i);
                        double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilesPos);

                        // Calculate continuous percentile with interpolation
                        double position = multiplier * (size - 1);
                        int lowerIndex = (int) Math.floor(position);
                        int upperIndex = (int) Math.ceil(position);

                        double resultValue;
                        if (lowerIndex == upperIndex) {
                            resultValue = listMemory.getDouble(listPtr + DATA_OFFSET + lowerIndex * 8L);
                        } else {
                            double lowerValue = listMemory.getDouble(listPtr + DATA_OFFSET + lowerIndex * 8L);
                            double upperValue = listMemory.getDouble(listPtr + DATA_OFFSET + upperIndex * 8L);
                            double fraction = position - lowerIndex;
                            resultValue = lowerValue + (upperValue - lowerValue) * fraction;
                        }
                        resultMemory.putDouble(resultPtr + 8 + i * 8L, resultValue);
                    }

                    // Update map value to point to result
                    value.putLong(0, resultPtr);
                }
            }
        }

        @Override
        public void reopen() {
            super.reopen();
            listMemory.close();
            resultMemory.close();
            result = Misc.free(result);
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(listMemory);
            Misc.free(resultMemory);
            result = Misc.free(result);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(percentilesFunc).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            listMemory.truncate();
            resultMemory.truncate();
            result = Misc.free(result);
        }

    }

    // Handles percentile_cont() over () - whole result set with multiple percentiles
    static class MultiPercentileContOverWholeResultSetFunction extends BaseWindowFunction implements Reopenable, WindowArrayFunction {
        private final MemoryARW listMemory;
        private final Function percentilesFunc;
        private final int percentilesPos;
        private final int type;
        private boolean isResultValid;
        private DirectArray result;
        private double[] results;
        private long size;

        public MultiPercentileContOverWholeResultSetFunction(Function arg, Function percentilesFunc, int percentilesPos, CairoConfiguration configuration) {
            super(arg);
            this.percentilesFunc = percentilesFunc;
            this.percentilesPos = percentilesPos;
            this.listMemory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(percentilesFunc);
            Misc.free(listMemory);
            Misc.free(result);
            results = null;
        }

        @Override
        public ArrayView getArray(Record rec) {
            if (result == null) {
                result = new DirectArray();
                result.setType(type);
            }

            if (isResultValid) {
                result.setDimLen(0, results.length);
                result.applyShape();
                for (int i = 0; i < results.length; i++) {
                    result.putDouble(i, results[i]);
                }
                return result;
            }

            result.ofNull();
            return result;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void init(io.questdb.cairo.sql.SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            percentilesFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                listMemory.putDouble(size * 8, d);
                size++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            // Write the array result to the WindowSPI at the correct column position
            spi.putArray(recordOffset, columnIndex, getArray(record));
        }

        @Override
        public void preparePass2() {
            if (size == 0) {
                isResultValid = false;
                return;
            }

            ArrayView percentiles = percentilesFunc.getArray(null);
            FlatArrayView view = percentiles.flatView();
            int percentileCount = view.length();

            // Sort the list
            DoubleSort.sort(listMemory.getPageAddress(0), 0, size - 1);

            // Pre-allocate or reuse results array
            if (results == null || results.length < percentileCount) {
                results = new double[percentileCount];
            }

            for (int i = 0; i < percentileCount; i++) {
                double percentile = view.getDoubleAtAbsIndex(i);
                double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilesPos);

                // Calculate continuous percentile with interpolation
                double position = multiplier * (size - 1);
                int lowerIndex = (int) Math.floor(position);
                int upperIndex = (int) Math.ceil(position);

                if (lowerIndex == upperIndex) {
                    results[i] = listMemory.getDouble(lowerIndex * 8L);
                } else {
                    double lowerValue = listMemory.getDouble(lowerIndex * 8L);
                    double upperValue = listMemory.getDouble(upperIndex * 8L);
                    double fraction = position - lowerIndex;
                    results[i] = lowerValue + (upperValue - lowerValue) * fraction;
                }
            }

            isResultValid = true;
        }

        @Override
        public void reopen() {
            listMemory.close();
            size = 0;
            result = Misc.free(result);
            isResultValid = false;
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(listMemory);
            size = 0;
            result = Misc.free(result);
            isResultValid = false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(percentilesFunc).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            listMemory.truncate();
            size = 0;
            result = Misc.free(result);
            isResultValid = false;
        }
    }

    static {
        COLUMN_TYPES.add(ColumnType.LONG); // list pointer
    }
}
