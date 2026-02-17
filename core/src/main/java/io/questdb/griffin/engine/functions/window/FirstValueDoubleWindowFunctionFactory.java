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

// Returns value evaluated at the row that is the first row of the window frame.
public class FirstValueDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "first_value";
    protected static final ArrayColumnTypes FIRST_VALUE_COLUMN_TYPES;
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
                    windowContext.getPartitionByRecord());
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
                // moving first_value() ignore nulls over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstNotNullValueOverPartitionFunction(
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
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(
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
                    columnTypes.add(ColumnType.LONG);  // number of values in current frame
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
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new FirstNotNullValueOverPartitionRangeFrameFunction(
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
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new FirstValueOverCurrentRowFunction(args.get(0), true);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstNotNullValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row] (but not unbounded preceding to current row )
                else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);// position of current oldest element
                    columnTypes.add(ColumnType.LONG);// start offset of native array
                    columnTypes.add(ColumnType.LONG);// count of values in buffer
                    columnTypes.add(ColumnType.LONG);// count of values in buffer

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving first over preceding N rows
                    return new FirstNotNullValueOverPartitionRowsFrameFunction(
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
                    return new FirstNotNullValueOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    // if lower bound is unbounded then it's the same as over ()
                    return new FirstNotNullValueOverWholeResultSetFunction(args.get(0));
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // first_value() ignore nulls over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new FirstNotNullValueOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and [current row | unbounded following]
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new FirstNotNullValueOverWholeResultSetFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new FirstValueDoubleWindowFunctionFactory.FirstValueOverCurrentRowFunction(args.get(0), true);
                } // between [unbounded | x] preceding and [y preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new FirstNotNullValueOverRowsFrameFunction(
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
                // moving average over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstValueOverPartitionFunction(
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
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new FirstValueOverUnboundedPartitionRowsFrameFunction(
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
                    columnTypes.add(ColumnType.LONG);  // number of values in current frame
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
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new FirstValueOverPartitionRangeFrameFunction(
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
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new FirstValueOverCurrentRowFunction(args.get(0), false);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row] (but not unbounded preceding to current row )
                else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);// position of current oldest element
                    columnTypes.add(ColumnType.LONG);// start offset of native array
                    columnTypes.add(ColumnType.LONG);// count of values in buffer

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    // moving average over preceding N rows
                    return new FirstValueOverPartitionRowsFrameFunction(
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
                    return new FirstValueOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    // if lower bound is unbounded then it's the same as over ()
                    return new FirstValueOverWholeResultSetFunction(args.get(0));
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // first_value() over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new FirstValueOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and [current row | unbounded following]
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new FirstValueOverWholeResultSetFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new FirstValueOverCurrentRowFunction(args.get(0), false);
                } // between [unbounded | x] preceding and [y preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new FirstValueOverRowsFrameFunction(
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

    // handles first_value() ignore nulls over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class FirstNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        public FirstNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
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

    // Handles first_value() ignore nulls over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class FirstNotNullValueOverPartitionRangeFrameFunction extends FirstValueOverPartitionRangeFrameFunction {
        public FirstNotNullValueOverPartitionRangeFrameFunction(
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
        public void computeNext(Record record) {
            // map stores
            // 0 - native array start offset (relative to memory address)
            // 1 - size of ring buffer (number of elements stored in it; not all of them need to belong to frame)
            // 2 - capacity of ring buffer
            // 3 - index of first (the oldest) valid buffer element
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

            if (mapValue.isNew()) {
                double d = arg.getDouble(record);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (Numbers.isFinite(d)) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDouble(startOffset + Long.BYTES, d);
                    size = 1;
                    if (frameIncludesCurrentValue) {
                        this.firstValue = d;
                    } else {
                        this.firstValue = Double.NaN;
                    }
                } else {
                    size = 0;
                    this.firstValue = Double.NaN;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) { // use firstIdx as a flag
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            firstValue = memory.getDouble(startOffset + Long.BYTES);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue = Double.NaN;
                        }
                    } else {
                        // first value always in first index case when frameLoBounded == false
                        firstValue = memory.getDouble(startOffset + Long.BYTES);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            this.firstValue = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                double d = arg.getDouble(record);
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

                if (!findNewFirstValue) {
                    this.firstValue = frameIncludesCurrentValue ? d : Double.NaN;
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

    // handles first_value() ignore nulls over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class FirstNotNullValueOverPartitionRowsFrameFunction extends FirstValueOverPartitionRowsFrameFunction {

        public FirstNotNullValueOverPartitionRowsFrameFunction(
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
        public void computeNext(Record record) {
            // map stores:
            // 0 - (0-based) index of oldest value [0, bufferSize]
            // 1 - native array start offset (relative to memory address)
            // 2 - first not null index
            // 3 - count of values in buffer if frameLoUnBounded

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

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
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = memory.getDouble(startOffset);
                    return;
                }

                double d = arg.getDouble(record);
                if (firstNotNullIdx == -1 && Numbers.isFinite(d)) {
                    firstNotNullIdx = count;
                    memory.putDouble(startOffset, d);
                    this.firstValue = frameIncludesCurrentValue ? d : Double.NaN;
                } else {
                    this.firstValue = Double.NaN;
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                double d = arg.getDouble(record);
                if (firstNotNullIdx != -1 && Numbers.isFinite(memory.getDouble(startOffset + loIdx * Double.BYTES))) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    this.firstValue = memory.getDouble(startOffset + firstNotNullIdx * Double.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        double res = memory.getDouble(startOffset + (loIdx + i) % bufferSize * Double.BYTES);
                        if (Numbers.isFinite(res)) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            this.firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        this.firstValue = frameIncludesCurrentValue ? d : Double.NaN;
                    }

                }

                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putDouble(startOffset + loIdx * Double.BYTES, d);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles first_value() ignore nulls over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class FirstNotNullValueOverRangeFrameFunction extends FirstValueOverRangeFrameFunction implements Reopenable, WindowDoubleFunction {

        public FirstNotNullValueOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) { // use firstIdx as a flag firstValue has in frame.
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        firstValue = memory.getDouble(startOffset + Long.BYTES);
                    } else {
                        firstValue = Double.NaN;
                    }
                } else {
                    // first value always in first index case when not frameLoBounded
                    firstValue = memory.getDouble(startOffset + Long.BYTES);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            // find new bottom border of range frame and remove unneeded elements
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) { // find the first not null value
                        findNewFirstValue = true;
                        this.firstValue = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    }
                    break;
                }
            }
            firstIdx = newFirstIdx;
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                if (size == capacity) { //buffer full
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

            if (!findNewFirstValue) {
                this.firstValue = frameIncludesCurrentValue ? d : Double.NaN;
            }
        }


        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles first_value() ignore nulls over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class FirstNotNullValueOverRowsFrameFunction extends FirstValueOverRowsFrameFunction implements Reopenable, WindowDoubleFunction {
        private long firstNotNullIdx = -1;

        public FirstNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getDouble(0);
                    return;
                }

                double d = arg.getDouble(record);
                if (firstNotNullIdx == -1 && Numbers.isFinite(d)) {
                    firstNotNullIdx = count;
                    buffer.putDouble(0, d);
                    this.firstValue = frameIncludesCurrentValue ? d : Double.NaN;
                } else {
                    this.firstValue = Double.NaN;
                }
                count++;
            } else {
                double d = arg.getDouble(record);
                if (firstNotNullIdx != -1 && Numbers.isFinite(buffer.getDouble((long) loIdx * Double.BYTES))) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    this.firstValue = buffer.getDouble(firstNotNullIdx * Double.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        double res = buffer.getDouble((long) (loIdx + i) % bufferSize * Double.BYTES);
                        if (Numbers.isFinite(res)) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            this.firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        this.firstValue = frameIncludesCurrentValue ? d : Double.NaN;
                    }
                }

                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putDouble((long) loIdx * Double.BYTES, d);
                loIdx = (loIdx + 1) % bufferSize;
            }
        }


        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    // Handles:
    // - first_value(a) ignore nulls over (partition by x rows between unbounded preceding and [current row | x preceding ])
    // - first_value(a) ignore nulls over (partition by x order by ts range between unbounded preceding and [current row | x preceding])
    static class FirstNotNullValueOverUnboundedPartitionRowsFrameFunction extends FirstValueOverUnboundedPartitionRowsFrameFunction {
        public FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.findValue();
            if (mapValue != null) {
                this.value = mapValue.getDouble(0);
            } else {
                double d = arg.getDouble(record);
                if (Numbers.isFinite(d)) {
                    mapValue = key.createValue();
                    mapValue.putDouble(0, d);
                    this.value = d;
                } else {
                    this.value = Double.NaN;
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // handles:
    // first_value() ignore nulls over () - empty clause, no partition by no order by, no frame == default frame
    // first_value() ignore nulls over (rows between unbounded preceding and current row); there's no partition by.
    public static class FirstNotNullValueOverWholeResultSetFunction extends FirstValueOverWholeResultSetFunction {

        public FirstNotNullValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                double d = arg.getDouble(record);
                if (Numbers.isFinite(d)) {
                    this.value = d;
                    this.found = true;
                }
            }
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
            if (!found) {
                double d = arg.getDouble(record);
                if (Numbers.isFinite(d)) {
                    this.value = d;
                    this.found = true;
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
            found = false;
            value = Double.NaN;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Double.NaN;
        }
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class FirstValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final boolean ignoreNulls;
        private double value;

        FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
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
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles first_value() over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class FirstValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private double firstValue;

        public FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                firstValue = arg.getDouble(record);
                value.putDouble(0, firstValue);
            } else {
                firstValue = value.getDouble(0);
            }
        }

        @Override
        public double getDouble(Record rec) {
            return firstValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    // Handles first_value() over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        protected final boolean frameIncludesCurrentValue;
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
        protected double firstValue;

        public FirstValueOverPartitionRangeFrameFunction(
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

            frameIncludesCurrentValue = rangeHi == 0;
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
            // 0 - current number of rows in in-memory frame
            // 1 - native array start offset (relative to memory address)
            // 2 - size of ring buffer (number of elements stored in it; not all of them need to belong to frame)
            // 3 - capacity of ring buffer
            // 4 - index of first (the oldest) valid buffer element
            // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
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

                if (frameIncludesCurrentValue) {
                    firstValue = d;
                    frameSize = 1;
                } else {
                    firstValue = Double.NaN;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize > 0) {
                    firstValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new bottom border of range frame and remove unneeded elements
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

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

                // find new top border of range frame and add new elements
                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);

                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    if (size > 0) {
                        long idx = firstIdx % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                            newFirstIdx = idx;
                        }
                    }

                    firstIdx = newFirstIdx;
                }

                if (frameSize != 0) {
                    firstValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    firstValue = Double.NaN;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return firstValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            firstValue = Double.NaN;
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
            sink.val(maxDiff);
            sink.val(" preceding and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    // handles first_value() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        // holds fixed-size ring buffers of double values
        protected final MemoryARW memory;
        protected double firstValue;

        public FirstValueOverPartitionRowsFrameFunction(
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
                frameSize = 1;// if there's no lower bound then first element that enters frame wins
                bufferSize = (int) Math.abs(rowsHi);//rowsHi=0 is covered by another function
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - (0-based) index of oldest value [0, bufferSize]
            // 1 - native array start offset (relative to memory address)
            // 2 - count of values in buffer

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            long count;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);

                if (!frameLoBounded && count == bufferSize) {
                    // loIdx already points at the 'oldest' element because frame is 1-el. big and buffer is full
                    firstValue = memory.getDouble(startOffset + loIdx * Double.BYTES);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                //firstValue = memory.getDouble(startOffset + loIdx % bufferSize * Double.BYTES);
                firstValue = memory.getDouble(startOffset + (loIdx + bufferSize - count) % bufferSize * Double.BYTES);
            } /*else if (count > bufferSize - frameSize) {
                firstValue = memory.getDouble(startOffset + (loIdx + bufferSize - count) % bufferSize * Double.BYTES);
            } */ else {
                firstValue = Double.NaN;
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);

            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return firstValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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
        }
    }

    // Handles first_value() over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        protected final boolean frameIncludesCurrentValue;
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
        protected double firstValue;
        protected long frameSize;
        protected long size;
        protected long startOffset;

        public FirstValueOverRangeFrameFunction(
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
            memory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize > 0) {
                firstValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            // add new element (even if it's null)
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

            // find new top border of range frame and add new elements
            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                        newFirstIdx = idx;
                    }
                }
                firstIdx = newFirstIdx;
            }

            if (frameSize != 0) {
                firstValue = memory.getDouble(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                firstValue = Double.NaN;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return firstValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
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
            sink.val(maxDiff);
            sink.val(" preceding and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            firstValue = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }
    }

    // Handles first_value() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected long count = 0;
        protected double firstValue;
        protected int loIdx = 0;

        public FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);

            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use FirstValueOverWholeResultSetFunction in case of (Long.MIN_VALUE, 0) range

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);//number of values we need to keep to compute over frame
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
            if (!frameLoBounded && count > (bufferSize - frameSize)) {
                firstValue = buffer.getDouble((loIdx + bufferSize - count) % bufferSize * Double.BYTES);
                return;
            }

            double d = arg.getDouble(record);

            if (count > bufferSize - frameSize) {//we've some elements in the frame
                firstValue = buffer.getDouble((loIdx + bufferSize - count) % bufferSize * Double.BYTES);
            } else if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else {
                firstValue = Double.NaN;
            }

            count = Math.min(count + 1, bufferSize);

            //overwrite oldest element
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return firstValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Double.NaN;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue = Double.NaN;
            loIdx = 0;
            count = 0;
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
            firstValue = Double.NaN;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - first_value(a) over (partition by x rows between unbounded preceding and [current row | x preceding ])
    // - first_value(a) over (partition by x order by ts range between unbounded preceding and [current row | x preceding])
    static class FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected double value;

        public FirstValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                double d = arg.getDouble(record);
                mapValue.putDouble(0, d);
                value = d;
            } else {
                value = mapValue.getDouble(0);
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
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // handles:
    // first_value() over () - empty clause, no partition by no order by, no frame == default frame
    // first_value() over (rows between unbounded preceding and current row); there's no partition by.
    public static class FirstValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        protected boolean found;
        protected double value = Double.NaN;

        public FirstValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                this.value = arg.getDouble(record);
                this.found = true;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return this.value;
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
            found = false;
            value = Double.NaN;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Double.NaN;
        }
    }

    static {
        FIRST_VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_COLUMN_TYPES.add(ColumnType.DOUBLE);
    }
}
