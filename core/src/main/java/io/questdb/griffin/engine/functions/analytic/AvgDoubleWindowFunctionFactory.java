/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.model.AnalyticColumn;
import io.questdb.std.*;

import java.util.Arrays;

public class AvgDoubleWindowFunctionFactory implements FunctionFactory {

    private static final ArrayColumnTypes AVG_COLUMN_TYPES;

    private static final String NAME = "avg";
    private static final String SIGNATURE = NAME + "(D)";

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
        final AnalyticContext analyticContext = sqlExecutionContext.getAnalyticContext();
        if (analyticContext.isEmpty()) {
            throw SqlException.emptyAnalyticContext(position);
        }

        long rowsLo = analyticContext.getRowsLo();
        long rowsHi = analyticContext.getRowsHi();

        if (!analyticContext.isDefaultFrame()) {
            if (rowsLo > 0) {
                throw SqlException.$(analyticContext.getRowsLoKindPos(), "frame boundaries that use FOLLOWING are not supported");
            }
            if (rowsHi > 0) {
                throw SqlException.$(analyticContext.getRowsHiKindPos(), "frame boundaries that use FOLLOWING are not supported");
            }
        }

        int exclusionKind = analyticContext.getExclusionKind();
        int exclusionKindPos = analyticContext.getExclusionKindPos();
        if (exclusionKind != AnalyticColumn.EXCLUDE_NO_OTHERS
                && exclusionKind != AnalyticColumn.EXCLUDE_CURRENT_ROW) {
            throw SqlException.$(exclusionKindPos, "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported");
        }

        if (exclusionKind == AnalyticColumn.EXCLUDE_CURRENT_ROW) {
            // assumes frame doesn't use 'following'

            if (rowsHi == 0) {
                rowsHi = -1;
            }
            if (rowsHi < rowsLo) {
                throw SqlException.$(exclusionKindPos, "start of window is not lower than end of window due to exclusion mode");
            }
        }

        long windowSize = Math.abs(rowsLo != Long.MIN_VALUE ? rowsLo : rowsHi);

        if (windowSize > configuration.getSqlAnalyticMaxFrameSize()) {
            throw SqlException.$(position, "window buffer size exceeds configured limit [maxSize=").put(configuration.getSqlAnalyticMaxFrameSize()).put(",actual=").put(windowSize).put(']');
        }

        int framingMode = analyticContext.getFramingMode();
        RecordSink partitionBySink = analyticContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = analyticContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = analyticContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == AnalyticColumn.FRAMING_RANGE) {
                if (!analyticContext.isOrdered()
                        && analyticContext.isDefaultFrame()) {
                    // moving average over whole partition (no order by, default frame)
                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    return new AvgOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }

                throw SqlException.$(position, "function not implemented for given window paramters");

            } else if (framingMode == AnalyticColumn.FRAMING_GROUPS) {

                throw SqlException.$(position, "function not implemented for given window paramters");

            } else if (framingMode == AnalyticColumn.FRAMING_ROWS) {

                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE
                        && rowsHi == 0) {
                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    return new AvgOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0
                        && rowsLo == rowsHi) {
                    return new AvgOverCurrentRowFunction(args.get(0));
                } //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    final int bufferSize;
                    if (rowsLo > Long.MIN_VALUE) {
                        bufferSize = (int) Math.abs(rowsLo);
                    } else {
                        bufferSize = (int) Math.abs(rowsHi);
                    }

                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.DOUBLE);// sum
                    columnTypes.add(ColumnType.LONG);// current frame size
                    columnTypes.add(ColumnType.INT);// position of current oldest element

                    for (long i = 0; i < bufferSize; i++) {
                        columnTypes.add(ColumnType.DOUBLE);
                    }

                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    // moving average over preceding N rows
                    return new AvgOverPartitionRowsFrameFunction(
                            map,
                            rowsLo,
                            rowsHi,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
            }
        } else { // no partition key

            if (framingMode == AnalyticColumn.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!analyticContext.isOrdered()) {
                    return new AvgOverWholeResultSetFunction(args.get(0));
                }

                throw SqlException.$(position, "function not implemented for given window paramters");

            } else if (framingMode == AnalyticColumn.FRAMING_GROUPS) {

                throw SqlException.$(position, "function not implemented for given window paramters");

            } else if (framingMode == AnalyticColumn.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new AvgOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0
                        && rowsLo == rowsHi) {
                    return new AvgOverCurrentRowFunction(args.get(0));
                } //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    return new AvgOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window paramters");
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class AvgOverCurrentRowFunction extends DoubleFunction implements AnalyticFunction, ScalarFunction {

        private final Function arg;

        private int columnIndex;

        AvgOverCurrentRowFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public double getDouble(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {

        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), arg.getDouble(record));
        }

        @Override
        public void reset() {

        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }
    }

    // handles avg() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    private static class AvgOverPartitionFunction extends BasePartitionedAvgFunction {

        public AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public int getAnalyticType() {
            return AnalyticFunction.TWO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                long count;
                double sum;

                if (value.isNew()) {
                    count = 1;
                    sum = d;
                } else {
                    count = value.getLong(1) + 1;
                    sum = value.getDouble(0) + d;
                }
                value.putDouble(0, sum);
                value.putLong(1, count);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, AnalyticSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            double val = value != null ? value.getDouble(0) : Double.NaN;

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), val);
        }

        @Override
        public void preparePass2() {
            // TODO: maybe skip this step and compute in pass2 ?
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                long count = value.getLong(1);
                if (count > 0) {
                    double sum = value.getDouble(0);
                    value.putDouble(0, sum / count);
                }
            }
        }
    }

    // handles mavg() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    private static class AvgOverPartitionRowsFrameFunction extends BasePartitionedAvgFunction {

        private final int bufferSize;

        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoUnbounded;
        private final int frameSize;

        public AvgOverPartitionRowsFrameFunction(Map map, long rowsLo, long rowsHi, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo);
                bufferSize = (int) Math.abs(rowsLo);//number of values we need to keep to compute over frame
                frameLoUnbounded = false;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoUnbounded = true;
            }
            frameIncludesCurrentValue = rowsHi == 0;
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            // map stores:
            // 0 - sum, never store NaN in it
            // 1 - current number of non-null rows in frame
            // 2 - (0-based) index of oldest value [0, bufferSize]
            // 3 - value[Lo]
            // 4 - value[Lo+1]
            // ...
            // 4 + bufferSize - value[current_row-1]
            // we've to keep nulls in window and reject them when computing avg

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long count;
            double sum;
            int loIdx;//current index of lo frame value ('oldest')
            double avg;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    sum = d;
                    count = 1;
                    avg = d;
                } else {
                    sum = 0.0;
                    avg = Double.NaN;
                    count = 0;
                }

                for (int i = 4, max = 3 + bufferSize; i < max; i++) {
                    value.putDouble(i, Double.NaN);
                }
            } else {
                sum = value.getDouble(0);
                count = value.getLong(1);
                loIdx = value.getInt(2);

                //compute value using top frame element (that could be current or previous row)
                double hiValue = frameIncludesCurrentValue ? d : value.getDouble(3 + (loIdx + frameSize) % bufferSize);
                if (Numbers.isFinite(hiValue)) {
                    count++;
                    sum += hiValue;
                }

                //here sum is correct for current row
                avg = count != 0 ? sum / count : Double.NaN;

                if (!frameLoUnbounded) {
                    //remove the oldest element with newest
                    double loValue = value.getDouble(3 + loIdx);
                    if (Numbers.isFinite(loValue)) {
                        sum -= loValue;
                        count--;
                    }
                }
            }

            value.putDouble(0, sum);
            value.putLong(1, count);
            value.putLong(2, (loIdx + 1) % bufferSize);
            value.putDouble(3 + loIdx, d);

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }
    }

    // Handles mavg() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    private static class AvgOverRowsFrameFunction extends BaseAvgFunction {

        private final double[] buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoUnbounded;
        private final int frameSize;
        private long count;
        private int loIdx = 0;
        private double sum = 0.0;

        public AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi) {
            super(arg);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo);
                bufferSize = (int) Math.abs(rowsLo);//number of values we need to keep to compute over frame
                frameLoUnbounded = false;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoUnbounded = true;
            }
            buffer = new double[bufferSize];
            Arrays.fill(buffer, Double.NaN);
            frameIncludesCurrentValue = rowsHi == 0;
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            double d = arg.getDouble(record);

            //compute value using top frame element (that could be current or previous row)
            double hiValue = frameIncludesCurrentValue ? d : buffer[(loIdx + frameSize) % bufferSize];
            if (Numbers.isFinite(hiValue)) {
                sum += hiValue;
                count++;
            }

            //here sum is correct for current row
            double avg = count != 0 ? sum / count : Double.NaN;

            if (!frameLoUnbounded) {
                //remove the oldest element with newest
                double loValue = buffer[loIdx];
                if (Numbers.isFinite(loValue)) {
                    sum -= loValue;
                    count--;
                }
            }

            //overwrite oldest element
            buffer[loIdx] = d;
            loIdx = (loIdx + 1) % bufferSize;

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }
    }

    // Handles mavg() over (partition by x rows between unbounded preceding and current row).
    // Doesn't require value buffering.
    private static class AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedAvgFunction {

        public AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
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
            if (Double.isFinite(d)) {
                sum += d;
                count++;

                value.putDouble(0, sum);
                value.putLong(1, count);
            }

            double avg = count != 0 ? sum / count : Double.NaN;
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }
    }

    // Handles mavg() over (rows between unbounded preceding and current row); there's no partititon by.
    private static class AvgOverUnboundedRowsFrameFunction extends BaseAvgFunction {

        private long count = 0;
        private double sum = 0.0;

        public AvgOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            double d = arg.getDouble(record);
            if (Double.isFinite(d)) {
                sum += d;
                count++;
            }

            double avg = count != 0 ? sum / count : Double.NaN;
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }
    }

    // mavg() over () - empty clause, no partition by no order by, no frame == default frame
    private static class AvgOverWholeResultSetFunction extends DoubleFunction implements AnalyticFunction {

        private final Function arg;
        private double avg;
        private int columnIndex;
        private long count;
        private double sum;

        public AvgOverWholeResultSetFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public int getAnalyticType() {
            return AnalyticFunction.TWO_PASS;
        }

        @Override
        public double getDouble(Record rec) {
            throw new UnsupportedOperationException();
            //never called ! unused, unless we rework analytic functions
//            if (count > 0) {
//                return sum / count;
//            }
//
//            return Double.NaN;
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {

        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, AnalyticSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void preparePass2() {
            avg = count > 0 ? sum / count : Double.NaN;
        }

        @Override
        public void reset() {

        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

    }

    private static abstract class BaseAvgFunction extends DoubleFunction implements AnalyticFunction, ScalarFunction, Reopenable {
        protected final Function arg;
        protected int columnIndex;

        public BaseAvgFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public double getDouble(Record rec) {
            //unused
            throw new UnsupportedOperationException();
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {

        }

        @Override
        public void reopen() {

        }

        @Override
        public void reset() {

        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over ()");
        }
    }

    private static abstract class BasePartitionedAvgFunction extends BaseAvgFunction {
        protected final Map map;
        protected final VirtualRecord partitionByRecord;
        protected final RecordSink partitionBySink;

        public BasePartitionedAvgFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(arg);
            this.map = map;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
        }

        @Override
        public void close() {
            Misc.free(map);
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public void reopen() {
            map.reopen();
        }

        @Override
        public void reset() {
            map.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    static {
        AVG_COLUMN_TYPES = new ArrayColumnTypes();
        AVG_COLUMN_TYPES.add(ColumnType.DOUBLE);
        AVG_COLUMN_TYPES.add(ColumnType.LONG);
    }
}
