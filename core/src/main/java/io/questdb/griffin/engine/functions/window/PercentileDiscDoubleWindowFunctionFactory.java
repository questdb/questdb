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
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.DoubleSort;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class PercentileDiscDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final ArrayColumnTypes COLUMN_TYPES = new ArrayColumnTypes();
    private static final String NAME = "percentile_disc";
    private static final String SIGNATURE = NAME + "(DD)";

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
        Function percentileFunc = args.getQuick(1);
        int percentilePos = argPositions.getQuick(1);

        if (!percentileFunc.isConstant()) {
            throw SqlException.$(percentilePos, "percentile argument must be a constant");
        }

        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, false);

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(arg, NAME, rowsLo, rowsHi, framingMode == WindowExpression.FRAMING_RANGE, partitionByRecord);
        }

        // Percentile functions only support default frame over whole partition
        // Default frame is whole partition when there's no ORDER BY
        if (!windowContext.isDefaultFrame() || windowContext.isOrdered()) {
            throw SqlException.$(position, "percentile_disc window function only supports whole partition frames");
        }

        if (partitionByRecord != null) {
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    partitionByKeyTypes,
                    COLUMN_TYPES
            );
            return new PercentileDiscOverPartitionFunction(
                    map,
                    partitionByRecord,
                    partitionBySink,
                    arg,
                    percentileFunc,
                    percentilePos,
                    configuration
            );
        } else {
            return new PercentileDiscOverWholeResultSetFunction(arg, percentileFunc, percentilePos, configuration);
        }
    }

    // Handles percentile_disc() over (partition by x)
    static class PercentileDiscOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {
        private static final long CAPACITY_OFFSET = 0;
        private static final long DATA_OFFSET = 16;
        private static final int INITIAL_CAPACITY = 16;
        private static final long SIZE_OFFSET_BLOCK = 8;

        private final MemoryARW listMemory;
        private final Function percentileFunc;
        private final int percentilePos;
        private double result;

        public PercentileDiscOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                Function percentileFunc,
                int percentilePos,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.percentileFunc = percentileFunc;
            this.percentilePos = percentilePos;
            this.listMemory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
        }

        @Override
        public void close() {
            super.close();
            Misc.free(percentileFunc);
            Misc.free(listMemory);
        }

        @Override
        public double getDouble(Record rec) {
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
        public void init(io.questdb.cairo.sql.SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            percentileFunc.init(symbolTableSource, executionContext);
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            if (value != null) {
                long listPtr = value.getLong(0);
                result = listMemory.getDouble(listPtr + DATA_OFFSET);
            } else {
                result = Double.NaN;
            }

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void preparePass2() {
            // Sort all lists and calculate percentiles
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();

            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                long listPtr = value.getLong(0);
                long size = listMemory.getLong(listPtr + SIZE_OFFSET_BLOCK);

                if (size > 0) {
                    // Get percentile value
                    double percentile = percentileFunc.getDouble(record);
                    double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);

                    // Sort the list (values start at listPtr + DATA_OFFSET)
                    DoubleSort.sort(listMemory.getPageAddress(0) + listPtr + DATA_OFFSET, 0, size - 1);

                    // Calculate index
                    int N = (int) Math.max(0, Math.ceil(size * multiplier) - 1);
                    double result = listMemory.getDouble(listPtr + DATA_OFFSET + N * 8L);

                    // Store result back at first value position
                    listMemory.putDouble(listPtr + DATA_OFFSET, result);
                }
            }
        }

        @Override
        public void reopen() {
            super.reopen();
            listMemory.close();
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(listMemory);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(percentileFunc).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            listMemory.truncate();
        }

    }

    // Handles percentile_disc() over () - whole result set
    static class PercentileDiscOverWholeResultSetFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private final MemoryARW listMemory;
        private final Function percentileFunc;
        private final int percentilePos;
        private double result;
        private long size;

        public PercentileDiscOverWholeResultSetFunction(Function arg, Function percentileFunc, int percentilePos, CairoConfiguration configuration) {
            super(arg);
            this.percentileFunc = percentileFunc;
            this.percentilePos = percentilePos;
            this.listMemory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
        }

        @Override
        public void close() {
            super.close();
            Misc.free(percentileFunc);
            Misc.free(listMemory);
        }

        @Override
        public double getDouble(Record rec) {
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
        public void init(io.questdb.cairo.sql.SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            percentileFunc.init(symbolTableSource, executionContext);
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void preparePass2() {
            if (size == 0) {
                result = Double.NaN;
                return;
            }

            double percentile = percentileFunc.getDouble(null);
            double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);

            // Sort the list
            DoubleSort.sort(listMemory.getPageAddress(0), 0, size - 1);

            // Calculate index
            int N = (int) Math.max(0, Math.ceil(size * multiplier) - 1);
            result = listMemory.getDouble(N * 8L);
        }

        @Override
        public void reopen() {
            listMemory.close();
            size = 0;
            result = Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(listMemory);
            size = 0;
            result = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(percentileFunc).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            listMemory.truncate();
            size = 0;
            result = Double.NaN;
        }

    }

    static {
        COLUMN_TYPES.add(ColumnType.LONG); // list pointer
    }
}
