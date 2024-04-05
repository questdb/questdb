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

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.*;

public class SumDoubleWindowFunctionFactory implements FunctionFactory {

    private static final String NAME = "sum";
    private static final String SIGNATURE = NAME + "(D)";
    private static final ArrayColumnTypes SUM_COLUMN_TYPES;

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isWindow() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (!windowContext.isDefaultFrame()) {
            if (rowsLo > 0) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            }
            if (rowsHi > 0) {
                if (rowsHi != Long.MAX_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports _number_ PRECEDING and CURRENT ROW only");
                } else if (rowsLo != Long.MIN_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
                }
            }
        }

        int exclusionKind = windowContext.getExclusionKind();
        int exclusionKindPos = windowContext.getExclusionKindPos();
        if (exclusionKind != WindowColumn.EXCLUDE_NO_OTHERS
                && exclusionKind != WindowColumn.EXCLUDE_CURRENT_ROW) {
            throw SqlException.$(exclusionKindPos, "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported");
        }

        if (exclusionKind == WindowColumn.EXCLUDE_CURRENT_ROW) {
            // assumes frame doesn't use 'following'
            if (rowsHi == Long.MAX_VALUE) {
                throw SqlException.$(exclusionKindPos, "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary");
            }

            if (rowsHi == 0) {
                rowsHi = -1;
            }
            if (rowsHi < rowsLo) {
                throw SqlException.$(exclusionKindPos, "end of window is higher than start of window due to exclusion mode");
            }
        }

        int framingMode = windowContext.getFramingMode();
        if (framingMode == WindowColumn.FRAMING_GROUPS) {
            throw SqlException.$(position, "function not implemented for given window parameters");
        }

        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowColumn.FRAMING_RANGE) {
                // moving sum over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            SUM_COLUMN_TYPES
                    );

                    return new SumOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            SUM_COLUMN_TYPES
                    );

                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new SumOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.DOUBLE);// current frame sum
                    columnTypes.add(ColumnType.LONG);  // number of (non-null) values in current frame
                    columnTypes.add(ColumnType.LONG);  // native array start offset, requires updating on resize
                    columnTypes.add(ColumnType.LONG);   // native buffer size
                    columnTypes.add(ColumnType.LONG);   // native buffer capacity
                    columnTypes.add(ColumnType.LONG);   // index of first buffered element

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem = Vm.getARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                    // moving sum over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new SumOverPartitionRangeFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            mem,
                            initialBufferSize,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            SUM_COLUMN_TYPES
                    );

                    return new SumOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsLo == rowsHi) {
                    return new SumOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            SUM_COLUMN_TYPES
                    );

                    return new SumOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.DOUBLE);// sum
                    columnTypes.add(ColumnType.LONG);// current frame size
                    columnTypes.add(ColumnType.LONG);// position of current oldest element
                    columnTypes.add(ColumnType.LONG);// start offset of native array

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    MemoryARW mem = Vm.getARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving sum over preceding N rows
                    return new SumOverPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            mem
                    );
                }
            }
        } else { // no partition key
            if (framingMode == WindowColumn.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new SumOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new SumOverUnboundedRowsFrameFunction(args.get(0));
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // moving sum over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new SumOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new SumOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsLo == rowsHi) {
                    return new SumOverCurrentRowFunction(args.get(0));
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new SumOverWholeResultSetFunction(args.get(0));
                } //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new SumOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class SumOverCurrentRowFunction extends AvgDoubleWindowFunctionFactory.AvgOverCurrentRowFunction {
        SumOverCurrentRowFunction(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    // handles sum() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class SumOverPartitionFunction extends AvgDoubleWindowFunctionFactory.AvgOverPartitionFunction {

        public SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void preparePass2() {
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                long count = value.getLong(1);
                if (count > 0) {
                    double sum = value.getDouble(0);
                    value.putDouble(0, sum);
                }
            }
        }
    }

    // Handles sum() over (partition by x order by ts range between [undobuned | y] preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value.
    static class SumOverPartitionRangeFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverPartitionRangeFrameFunction {

        public SumOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    // handles sum() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    static class SumOverPartitionRowsFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverPartitionRowsFrameFunction {
        public SumOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory);
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), sum);
        }
    }

    // Handles sum() over ([order by ts] range between [unbounded | x] preceding and [ x preceding | current row ] ); no partition by key
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value .
    static class SumOverRangeFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverRangeFrameFunction {
        public SumOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }


        @Override
        public double getDouble(Record rec) {
            return externalSum;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    // Handles sum() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    static class SumOverRowsFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverRowsFrameFunction {

        public SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        @Override
        public double getDouble(Record rec) {
            return externalSum;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void toTop() {
            super.toTop();
            externalSum = 0;
        }
    }

    // Handles:
    // - sum(a) over (partition by x rows between unbounded preceding and current row)
    // - sum(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedDoubleWindowFunction {

        private double sum;

        public SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double sum;
            long count;

            if (value.isNew()) {
                sum = 0;
                count = 0;
            } else {
                sum = value.getDouble(0);
                count = value.getLong(1);
            }

            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;

                value.putDouble(0, sum);
                value.putLong(1, count);
            }

            this.sum = count != 0 ? sum : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), sum);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    // Handles sum() over (rows between unbounded preceding and current row); there's no partition by.
    static class SumOverUnboundedRowsFrameFunction extends BaseDoubleWindowFunction {

        private long count = 0;
        private double externalSum;
        private double sum = 0.0;

        public SumOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }

            externalSum = count != 0 ? sum : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return externalSum;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void reset() {
            super.reset();
            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();

            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    // sum() over () - empty clause, no partition by no order by, no frame == default frame
    static class SumOverWholeResultSetFunction extends BaseDoubleWindowFunction {
        private long count;
        private double externalSum;
        private double sum;

        public SumOverWholeResultSetFunction(Function arg) {
            super(arg);
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void preparePass2() {
            externalSum = count > 0 ? sum : Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
        }

        @Override
        public void toTop() {
            super.toTop();

            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    static {
        SUM_COLUMN_TYPES = new ArrayColumnTypes();
        SUM_COLUMN_TYPES.add(ColumnType.DOUBLE);
        SUM_COLUMN_TYPES.add(ColumnType.LONG);
    }
}
