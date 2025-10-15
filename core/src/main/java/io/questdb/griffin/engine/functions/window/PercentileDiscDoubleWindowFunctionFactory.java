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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
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
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class PercentileDiscDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String NAME = "percentile_disc";
    private static final String SIGNATURE = NAME + "(DD)";

    private static final ArrayColumnTypes COLUMN_TYPES = new ArrayColumnTypes();

    static {
        COLUMN_TYPES.add(ColumnType.LONG); // list pointer
    }

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

        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, false);

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(arg, NAME, rowsLo, rowsHi, framingMode == WindowColumn.FRAMING_RANGE, partitionByRecord);
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
        private final CairoConfiguration configuration;
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
            this.configuration = configuration;
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
                long size;

                if (value.isNew()) {
                    // Allocate space for size (8 bytes) + first value (8 bytes)
                    long allocPtr = listMemory.appendAddressFor(16) - listMemory.getPageAddress(0);
                    listMemory.putLong(allocPtr, 1); // size
                    listMemory.putDouble(allocPtr + 8, d); // first value
                    listPtr = allocPtr;
                    size = 1;
                } else {
                    listPtr = value.getLong(0);
                    size = listMemory.getLong(listPtr);
                    // Allocate new block for size + all values
                    long newPtr = listMemory.appendAddressFor(8 + (size + 1) * 8) - listMemory.getPageAddress(0);
                    listMemory.putLong(newPtr, size + 1); // new size
                    // Copy old values
                    for (long i = 0; i < size; i++) {
                        listMemory.putDouble(newPtr + 8 + i * 8, listMemory.getDouble(listPtr + 8 + i * 8));
                    }
                    listMemory.putDouble(newPtr + 8 + size * 8, d);
                    listPtr = newPtr;
                    size++;
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
                result = listMemory.getDouble(listPtr + 8); // First value is at listPtr + 8
            } else {
                result = Double.NaN;
            }

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void preparePass2() {
            // Sort all lists and calculate percentiles
            io.questdb.cairo.sql.RecordCursor cursor = map.getCursor();
            io.questdb.cairo.map.MapRecord record = map.getRecord();

            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                long listPtr = value.getLong(0);
                long size = listMemory.getLong(listPtr);

                if (size > 0) {
                    // Get percentile value
                    double percentile = percentileFunc.getDouble(record);
                    double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);

                    // Sort the list (values start at listPtr + 8)
                    quickSort(listPtr + 8, 0, size - 1);

                    // Calculate index
                    int N = (int) Math.max(0, Math.ceil(size * multiplier) - 1);
                    double result = listMemory.getDouble(listPtr + 8 + N * 8);

                    // Store result back at listPtr + 8 (first value position)
                    listMemory.putDouble(listPtr + 8, result);
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

        private void quickSort(long listPtr, long left, long right) {
            if (left < right) {
                long pi = partition(listPtr, left, right);
                quickSort(listPtr, left, pi - 1);
                quickSort(listPtr, pi + 1, right);
            }
        }

        private long partition(long listPtr, long left, long right) {
            double pivot = listMemory.getDouble(listPtr + right * 8);
            long i = left - 1;

            for (long j = left; j < right; j++) {
                if (listMemory.getDouble(listPtr + j * 8) < pivot) {
                    i++;
                    swap(listPtr, i, j);
                }
            }
            swap(listPtr, i + 1, right);
            return i + 1;
        }

        private void swap(long listPtr, long i, long j) {
            double temp = listMemory.getDouble(listPtr + i * 8);
            listMemory.putDouble(listPtr + i * 8, listMemory.getDouble(listPtr + j * 8));
            listMemory.putDouble(listPtr + j * 8, temp);
        }
    }

    // Handles percentile_disc() over () - whole result set
    static class PercentileDiscOverWholeResultSetFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private final CairoConfiguration configuration;
        private final MemoryARW listMemory;
        private final Function percentileFunc;
        private final int percentilePos;
        private double result;
        private long size;

        public PercentileDiscOverWholeResultSetFunction(Function arg, Function percentileFunc, int percentilePos, CairoConfiguration configuration) {
            super(arg);
            this.percentileFunc = percentileFunc;
            this.percentilePos = percentilePos;
            this.configuration = configuration;
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
            quickSort(0, size - 1);

            // Calculate index
            int N = (int) Math.max(0, Math.ceil(size * multiplier) - 1);
            result = listMemory.getDouble(N * 8);
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

        private void quickSort(long left, long right) {
            if (left < right) {
                long pi = partition(left, right);
                quickSort(left, pi - 1);
                quickSort(pi + 1, right);
            }
        }

        private long partition(long left, long right) {
            double pivot = listMemory.getDouble(right * 8);
            long i = left - 1;

            for (long j = left; j < right; j++) {
                if (listMemory.getDouble(j * 8) < pivot) {
                    i++;
                    swap(i, j);
                }
            }
            swap(i + 1, right);
            return i + 1;
        }

        private void swap(long i, long j) {
            double temp = listMemory.getDouble(i * 8);
            listMemory.putDouble(i * 8, listMemory.getDouble(j * 8));
            listMemory.putDouble(j * 8, temp);
        }
    }
}
