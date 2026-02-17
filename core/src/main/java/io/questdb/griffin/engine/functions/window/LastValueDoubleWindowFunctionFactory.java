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
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

// Returns value evaluated at the row that is the last row of the window frame.
public class LastValueDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES;
    public static final String NAME = "last_value";
    private static final String SIGNATURE = NAME + "(D)";

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
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE,
                    windowContext.getPartitionByRecord()
            );
        }
        return windowContext.isIgnoreNulls() ?
                this.generateIgnoreNullsFunction(position, args, configuration, windowContext) :
                this.generateRespectNullsFunction(position, args, configuration, windowContext);
    }

    private Function generateIgnoreNullsFunction(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving last over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_COLUMN_TYPES
                    );

                    return new LastNotNullValueOverPartitionFunction(
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
                            LAST_VALUE_COLUMN_TYPES
                    );

                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new LastNotNullValueOverUnboundedPartitionRowsFrameFunction(
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
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES
                    );

                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                    // moving last over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new LastNotNullValueOverPartitionRangeFrameFunction(
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
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_COLUMN_TYPES
                    );

                    return new LastNotNullValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new LastNotNullValueOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_COLUMN_TYPES
                    );

                    return new LastNotNullValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
                // between [unbounded | x] preceding and [x preceding | current row] (but not unbounded preceding to current row )
                else {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES
                    );
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving average over preceding N rows
                    return new LastNotNullValueOverPartitionRowsFrameFunction(
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
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new LastNotNullValueOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    // if lower bound is unbounded then it's the same as over ()
                    return new LastNotNullOverUnboundedRowsFrameFunction(args.get(0));
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();

                    // last_value() ignore nulls over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new LastNotNullValueOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new LastNotNullOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new LastNotNullValueOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new LastNotNullValueOverWholeResultSetFunction(args.get(0));
                } // between [unbounded | x] preceding and [y preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new LastNotNullValueOverRowsFrameFunction(
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

    private Function generateRespectNullsFunction(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving last over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_COLUMN_TYPES
                    );

                    return new LastValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between [unbounded preceding | x preceding] and current row
                else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new LastValueIncludeCurrentPartitionRowsFrameFunction(
                            rowsLo,
                            true,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // range between [unbounded | x] preceding and [x preceding]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES
                    );

                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                    // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new LastValueOverPartitionRangeFrameFunction(
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
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // whole partition
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_COLUMN_TYPES
                    );

                    return new LastValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } //between [unbounded preceding | x preceding] and current row
                else if (rowsHi == 0) {
                    return new LastValueIncludeCurrentPartitionRowsFrameFunction(
                            rowsLo,
                            false,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } //between [unbounded | x] preceding and x preceding
                else {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES
                    );

                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving last over preceding N rows
                    return new LastValueOverPartitionRowsFrameFunction(
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
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new LastValueOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0));
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // last_value() over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new LastValueOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new LastValueOverWholeResultSetFunction(args.get(0));
                } else if (rowsHi == 0) {
                    return new LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0));
                } else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new LastValueOverRowsFrameFunction(
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

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    // Handles last_value() ignore nulls over (rows between unbounded preceding and current row); there's no partition by.
    public static class LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private double lastValue = Double.NaN;

        public LastNotNullOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                lastValue = d;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reset() {
            super.reset();
            lastValue = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Double.NaN;
        }
    }

    // handle last_value() ignore nulls (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private double value = Double.NaN;

        LastNotNullValueOverCurrentRowFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }


        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles last_value() ignore nulls over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        public LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                double d = arg.getDouble(record);
                if (Double.isFinite(d)) {
                    MapValue value = key.createValue();
                    value.putDouble(0, d);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            double val = value != null ? value.getDouble(0) : Double.NaN;
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles last_value() ignore nulls over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class LastNotNullValueOverPartitionRangeFrameFunction extends LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public LastNotNullValueOverPartitionRangeFrameFunction(
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
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            // map stores
            // 0 - native array start offset (relative to memory address)
            // 1 - size of ring buffer (number of elements stored in it; not all of them need to belong to frame)
            // 2 - capacity of ring buffer
            // 3 - index of last (the newest) valid buffer element
            // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;

            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (Numbers.isFinite(d)) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDouble(startOffset + Long.BYTES, d);
                    size = 1;
                    lastValue = frameIncludesCurrentValue ? d : Double.NaN;
                } else {
                    lastValue = Double.NaN;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    // remove element greater than maxDiff
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                // add new element
                if (Numbers.isFinite(d)) {
                    if (size == capacity) { //buffer full
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    // add element to buffer
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                // only need to keep one element that greater than minDiff
                long lastIndex = -1;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        lastIndex = (int) (idx % capacity);
                        size--;
                    } else {
                        break;
                    }
                }

                // Move one position forward to serve as the last_value
                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Double.NaN;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // handles last_value() ignore nulls over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        //number of values we need to keep to compute over frame
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // holds fixed-size ring buffers of double values
        private final MemoryARW memory;
        private double lastValue = Double.NaN;

        public LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi); //rowsHi=0 is covered by another function
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    this.lastValue = d;
                } else {
                    this.lastValue = Double.NaN;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                this.lastValue = value.getDouble(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (Numbers.isFinite(d) && frameIncludesCurrentValue) {
                    this.lastValue = d;
                } else if (frameLoBounded) {
                    double last = memory.getDouble(startOffset + (loIdx + frameSize - 1) % bufferSize * Double.BYTES);
                    if (Numbers.isFinite(last)) {
                        this.lastValue = last;
                    } else if (!Numbers.isFinite(this.lastValue)) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            double v = memory.getDouble(startOffset + (loIdx + i) % bufferSize * Double.BYTES);
                            if (Numbers.isFinite(v)) {
                                this.lastValue = v;
                                break;
                            }
                        }
                    } // else keep lastValue
                } else {
                    double last = memory.getDouble(startOffset + loIdx % bufferSize * Double.BYTES);
                    if (Numbers.isFinite(last)) {
                        this.lastValue = last;
                    } else {
                        this.lastValue = value.getDouble(0);
                    }
                }
            }

            double nextLastValue = this.lastValue;
            // set lastValue as invalid
            if (frameLoBounded && memory.getDouble(startOffset + loIdx % bufferSize * Double.BYTES) == this.lastValue) {
                nextLastValue = Double.NaN;
            }
            value.putDouble(0, nextLastValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);//not necessary because it doesn't change
            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize + 1 - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Double.NaN;
        }
    }

    // Handles last_value() ignore nulls over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class LastNotNullValueOverRangeFrameFunction extends LastValueOverRangeFrameFunction {

        public LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            if (frameLoBounded) {
                // remove element greater than maxDiff
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;
            double d = arg.getDouble(record);
            // add new element
            if (Numbers.isFinite(d)) {
                if (size == capacity) { // buffer full
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    // call above can end up resizing and thus changing memory start address
                    long oldAddress = memory.getPageAddress(0) + startOffset;

                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        //we can't simply copy because that'd leave a gap in the middle
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }

                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }

                // add element to buffer
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            // only need to keep one element that greater than minDiff
            long lastIndex = -1;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) >= minDiff) {
                    lastIndex = (int) (idx % capacity);
                    size--;
                } else {
                    break;
                }
            }

            // Move one position forward to serve as the last_value
            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                this.lastValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                this.lastValue = Double.NaN;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles last_value() ignore nulls over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        // holds fixed-size ring buffers of double values
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private double cacheValue = Double.NaN;
        private double lastValue = Double.NaN;
        private int loIdx = 0;

        public LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            this.lastValue = this.cacheValue;
            if (Numbers.isFinite(d) && frameIncludesCurrentValue) {
                this.lastValue = d;
            } else if (frameLoBounded) {
                double last = buffer.getDouble((long) (loIdx + frameSize - 1) % bufferSize * Double.BYTES);
                if (Numbers.isFinite(last)) {
                    this.lastValue = last;
                } else if (!Numbers.isFinite(this.lastValue)) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        double v = buffer.getDouble((long) (loIdx + i) % bufferSize * Double.BYTES);
                        if (Numbers.isFinite(v)) {
                            this.lastValue = v;
                            break;
                        }
                    }
                } // else keep lastValue
            } else {
                double last = buffer.getDouble((long) loIdx % bufferSize * Double.BYTES);
                if (Numbers.isFinite(last)) {
                    this.lastValue = last;
                } else {
                    this.lastValue = cacheValue;
                }
            }

            this.cacheValue = this.lastValue;
            // set lastValue as invalid
            if (frameLoBounded && buffer.getDouble((long) loIdx % bufferSize * Double.BYTES) == this.lastValue) {
                this.cacheValue = Double.NaN;
            }
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Double.NaN;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Double.NaN;
            cacheValue = Double.NaN;
            loIdx = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize + 1 - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Double.NaN;
            cacheValue = Double.NaN;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - last_value(a) ignore nulls over (partition by x rows between unbounded preceding and [current row | x preceding ])
    // - last_value(a) ignore nulls over (partition by x order by ts range between unbounded preceding and [current row | x preceding])
    public static class LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private double value = Double.NaN;

        public LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            double d = arg.getDouble(record);
            if (mapValue.isNew()) {
                mapValue.putDouble(0, d);
                value = d;
            } else {
                if (Numbers.isFinite(d)) {
                    mapValue.putDouble(0, d);
                    value = d;
                } else {
                    value = mapValue.getDouble(0);
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
            return value;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // last_value() ignore nulls over () - empty clause, no partition by no order by
    public static class LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private boolean found;
        private double value = Double.NaN;

        public LastNotNullValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getPassCount() {
            return TWO_PASS;
        }


        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                double d = arg.getDouble(record);
                if (Numbers.isFinite(d)) {
                    found = true;
                    this.value = d;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Double.NaN;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Double.NaN;
            found = false;
        }
    }

    // Handles last_value() over (rows/range between [unbounded preceding x preceding] and current row); there's no partition by.
    public static class LastValueIncludeCurrentFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final boolean isRange;
        private final long rowsLo;
        private double value = Double.NaN;

        public LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return value;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            if (isRange) {
                sink.val("range between ");
            } else {
                sink.val("rows between ");
            }
            if (rowsLo == Long.MIN_VALUE) {
                sink.val("unbounded");
            } else {
                sink.val(Math.abs(rowsLo));
            }
            sink.val(" preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Double.NaN;
        }
    }

    // Handles:
    // - last_value(a) over (partition by x rows between [unbounded preceding | x preceding] and current row)
    // - last_value(a) over (partition by x order by ts range between  [unbounded preceding | x preceding] and current row)
    public static class LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final boolean isRange;
        private final long rowsLo;
        private double value = Double.NaN;

        public LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return value;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            if (isRange) {
                sink.val(" range between ");
            } else {
                sink.val(" rows between ");
            }
            if (rowsLo == Long.MIN_VALUE) {
                sink.val("unbounded");
            } else {
                sink.val(Math.abs(rowsLo));
            }
            sink.val(" preceding and current row)");
        }
    }

    // handles last_value() over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class LastValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        public LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            double val;
            if (value.isNew()) {
                double d = arg.getDouble(record);
                value.putDouble(0, d);
                val = d;
            } else {
                val = value.getDouble(0);
            }
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles last_value() over (partition by x order by ts range between y preceding and z preceding)
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        protected final boolean frameLoBounded;
        // list of [size, startOffset] pairs marking free space within mem
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        // holds resizable ring buffers
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected double lastValue = Double.NaN;

        public LastValueOverPartitionRangeFrameFunction(
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
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            // map stores
            // 0 - native array start offset (relative to memory address)
            // 1 - size of ring buffer (number of elements stored in it; not all of them need to belong to frame)
            // 2 - capacity of ring buffer
            // 3 - index of last (the newest) valid buffer element
            // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;

            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putDouble(startOffset + Long.BYTES, d);
                size = 1;
                lastValue = Double.NaN;
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    // remove element greater than maxDiff
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                // only need to keep one element that greater than minDiff
                long lastIndex = -1;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        lastIndex = (int) (idx % capacity);
                        size--;
                    } else {
                        break;
                    }
                }

                // Move one position forward to serve as the last_value
                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Double.NaN;
                }

                // add new element
                if (size == capacity) { //buffer full
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                // add element to buffer
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (!frameLoBounded) {
                sink.val("unbounded");
            } else {
                sink.val(maxDiff);
            }
            sink.val(" preceding and ");
            sink.val(minDiff).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Double.NaN;
        }
    }

    // handles last_value() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        // holds fixed-size ring buffers of double values
        private final MemoryARW memory;
        private final long rowLo;
        private double lastValue = Double.NaN;

        public LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - (0-based) index of oldest value
            // 1 - native array start offset (relative to memory address)
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            double d = arg.getDouble(record);
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            lastValue = memory.getDouble(startOffset + loIdx % bufferSize * Double.BYTES);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putDouble(startOffset + loIdx % bufferSize * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            lastValue = Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            if (rowLo == Long.MAX_VALUE) {
                sink.val("unbounded");
            } else {
                sink.val(Math.abs(rowLo));
            }
            sink.val(" preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Double.NaN;
        }
    }

    // Handles last_value() over ([order by ts] range between x preceding and y preceding ); no partition by key
    public static class LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        // holds resizable ring buffers
        // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]
        // note: we ignore nulls to reduce memory usage
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected long capacity;
        protected long firstIdx;
        protected double lastValue = Double.NaN;
        protected long size;
        protected long startOffset;

        public LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);
            long newFirstIdx = firstIdx;
            // remove element greater than maxDiff
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            // only need to keep one element that greater than minDiff
            long lastIndex = -1;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) >= minDiff) {
                    lastIndex = (int) (idx % capacity);
                    size--;
                } else {
                    break;
                }
            }

            // Move one position forward to serve as the last_value
            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Double.NaN;
            }

            // add new element
            if (size == capacity) { // buffer full
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                // call above can end up resizing and thus changing memory start address
                long oldAddress = memory.getPageAddress(0) + startOffset;

                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    //we can't simply copy because that'd leave a gap in the middle
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }

                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }

            // add element to buffer
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            sink.val(minDiff).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    // Handles last_value() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        private double lastValue = Double.NaN;
        private int loIdx = 0;

        public LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getDouble((long) loIdx * Double.BYTES);
            buffer.putDouble((long) loIdx * Double.BYTES, arg.getDouble(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return lastValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Double.NaN;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Double.NaN;
            loIdx = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val(" rows between ");
            if (rowsLo == Long.MIN_VALUE) {
                sink.val(" unbounded preceding and ");
            } else {
                sink.val(Math.abs(rowsLo));
                sink.val(" preceding between and ");
            }
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Double.NaN;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // last_value() over () - empty clause, no partition by no order by, no frame == default frame
    public static class LastValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private boolean found;
        private double value = Double.NaN;

        public LastValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = arg.getDouble(record);
                found = true;
            }
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Double.NaN;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Double.NaN;
            found = false;
        }
    }

    static {
        LAST_VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_VALUE_COLUMN_TYPES.add(ColumnType.DOUBLE);
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // position of current oldest element
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native array start offset, requires updating on resize
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native buffer size
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native buffer capacity
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // index of last buffered element

        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer
    }
}
