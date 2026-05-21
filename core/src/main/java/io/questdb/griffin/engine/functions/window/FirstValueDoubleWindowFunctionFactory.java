/*+*****************************************************************************
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
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

// Returns value evaluated at the row that is the first row of the window frame.
public class FirstValueDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "first_value";
    protected static final ArrayColumnTypes FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES;
    protected static final ArrayColumnTypes FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV;
    protected static final ArrayColumnTypes FIRST_VALUE_COLUMN_TYPES;
    protected static final ArrayColumnTypes FIRST_VALUE_COLUMN_TYPES_LV;
    protected static final ArrayColumnTypes FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES;
    protected static final ArrayColumnTypes FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV;
    protected static final ArrayColumnTypes FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES;
    protected static final ArrayColumnTypes FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV;
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? FIRST_VALUE_COLUMN_TYPES_LV : FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes,
                            liveView,
                            configuration
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV
                                    : FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES
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
                            timestampIndex,
                            partitionByKeyTypes,
                            liveView
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? FIRST_VALUE_COLUMN_TYPES_LV : FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes,
                            liveView,
                            configuration
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
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
                                mem,
                                partitionByKeyTypes,
                                liveView
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? FIRST_VALUE_COLUMN_TYPES_LV : FIRST_VALUE_COLUMN_TYPES
                    );

                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new FirstValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes,
                            liveView,
                            configuration
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV
                                    : FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES
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
                            timestampIndex,
                            partitionByKeyTypes,
                            liveView
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? FIRST_VALUE_COLUMN_TYPES_LV : FIRST_VALUE_COLUMN_TYPES
                    );

                    return new FirstValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes,
                            liveView,
                            configuration
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving first_value over preceding N rows
                        return new FirstValueOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                partitionByKeyTypes,
                                liveView
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), val);
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
                int timestampIdx,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx,
                    partitionByKeyTypes, liveView);
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
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
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

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putLong(1, 0L);
                value.putLong(3, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            memory.truncate();
            freeList.clear();
            tombstoneCount = 0;
            long srcOffset = 0;
            final long partitionCount = source.getLong(srcOffset);
            srcOffset += Long.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                srcOffset = LiveViewSnapshotKeyCodec.readKey(key, source, srcOffset, keyColumnTypes);
                MapValue value = key.createValue();
                final long size = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long capacity = Math.max(size, initialBufferSize);
                final long newStartOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                for (long i = 0; i < size; i++) {
                    memory.putLong(newStartOffset + i * RECORD_SIZE, source.getLong(srcOffset));
                    srcOffset += Long.BYTES;
                    memory.putDouble(newStartOffset + i * RECORD_SIZE + Long.BYTES, source.getDouble(srcOffset));
                    srcOffset += Double.BYTES;
                }
                value.putLong(0, newStartOffset);
                value.putLong(1, size);
                value.putLong(2, capacity);
                value.putLong(3, 0L);
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            final long liveCount;
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                liveCount = map.size();
            } else {
                long count = 0;
                while (cursor.hasNext()) {
                    if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                        count++;
                    }
                }
                liveCount = count;
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                final long startOffset = value.getLong(0);
                final long size = value.getLong(1);
                final long capacity = value.getLong(2);
                final long firstIdx = value.getLong(3);
                sink.putLong(size);
                for (long i = 0; i < size; i++) {
                    final long idx = (firstIdx + i) % capacity;
                    sink.putLong(memory.getLong(startOffset + idx * RECORD_SIZE));
                    sink.putDouble(memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES));
                }
            }
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
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory,
                    partitionByKeyTypes, liveView,
                    FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV, 4);
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
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
                loIdx = 0;
                final int freeN = freeList.size();
                if (freeN > 0) {
                    // Reuse a slab reclaimed from a tombstoned partition. The
                    // capacity slot is always bufferSize here, so only the
                    // startOffset matters.
                    startOffset = freeList.getQuick(freeN - 1);
                    freeList.setPos(freeN - 2);
                } else {
                    startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                }
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

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's bounded-ROWS frame to
            // empty: loIdx=0, firstNotNullIdx=-1, count=0, ring slots back to
            // NaN.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                final long startOffset = value.getLong(1);
                value.putLong(0, 0L);
                value.putLong(2, -1L);
                value.putLong(3, 0L);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            memory.truncate();
            freeList.clear();
            tombstoneCount = 0;
            long srcOffset = 0;
            final long partitionCount = source.getLong(srcOffset);
            srcOffset += Long.BYTES;
            final long ringBytes = (long) bufferSize * Double.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                srcOffset = LiveViewSnapshotKeyCodec.readKey(key, source, srcOffset, keyColumnTypes);
                MapValue value = key.createValue();
                final long loIdx = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long firstNotNullIdx = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long partitionCountVal = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(newStartOffset + (long) i * Double.BYTES, source.getDouble(srcOffset));
                    srcOffset += Double.BYTES;
                }
                value.putLong(0, loIdx);
                value.putLong(1, newStartOffset);
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, partitionCountVal);
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            final long liveCount;
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                liveCount = map.size();
            } else {
                long count = 0;
                while (cursor.hasNext()) {
                    if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                        count++;
                    }
                }
                liveCount = count;
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                sink.putLong(value.getLong(0));
                sink.putLong(value.getLong(2));
                sink.putLong(value.getLong(3));
                final long startOffset = value.getLong(1);
                for (int i = 0; i < bufferSize; i++) {
                    sink.putDouble(memory.getDouble(startOffset + (long) i * Double.BYTES));
                }
            }
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
        public FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, ColumnTypes partitionByKeyTypes, boolean liveView, CairoConfiguration configuration) {
            super(map, partitionByRecord, partitionBySink, arg, partitionByKeyTypes, liveView, configuration);
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
                    if (tombstoneValueIndex >= 0) {
                        mapValue.putByte(tombstoneValueIndex, (byte) 0);
                    }
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
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

            if (value.isNew() || value.getByte(1) == 0) {
                firstValue = arg.getDouble(record);
                value.putDouble(0, firstValue);
                value.putByte(1, (byte) 1);
            } else {
                firstValue = value.getDouble(0);
            }
        }

        @Override
        public double getDouble(Record rec) {
            return firstValue;
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            value.putByte(1, (byte) 0);
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
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
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
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
                int timestampIdx,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;

            frameIncludesCurrentValue = rangeHi == 0;

            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 5;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
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
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
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
        public Map getPartitionMap() {
            return map;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            firstValue = Double.NaN;
            tombstoneCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putLong(0, 0L);
                value.putLong(2, 0L);
                value.putLong(4, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            memory.truncate();
            freeList.clear();
            tombstoneCount = 0;
            long srcOffset = 0;
            final long partitionCount = source.getLong(srcOffset);
            srcOffset += Long.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                srcOffset = LiveViewSnapshotKeyCodec.readKey(key, source, srcOffset, keyColumnTypes);
                MapValue value = key.createValue();
                final long frameSize = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long size = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long capacity = Math.max(size, initialBufferSize);
                final long newStartOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                for (long i = 0; i < size; i++) {
                    memory.putLong(newStartOffset + i * RECORD_SIZE, source.getLong(srcOffset));
                    srcOffset += Long.BYTES;
                    memory.putDouble(newStartOffset + i * RECORD_SIZE + Long.BYTES, source.getDouble(srcOffset));
                    srcOffset += Double.BYTES;
                }
                value.putLong(0, frameSize);
                value.putLong(1, newStartOffset);
                value.putLong(2, size);
                value.putLong(3, capacity);
                value.putLong(4, 0L);
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            final long liveCount;
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                liveCount = map.size();
            } else {
                long count = 0;
                while (cursor.hasNext()) {
                    if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                        count++;
                    }
                }
                liveCount = count;
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                sink.putLong(value.getLong(0));
                final long startOffset = value.getLong(1);
                final long size = value.getLong(2);
                final long capacity = value.getLong(3);
                final long firstIdx = value.getLong(4);
                sink.putLong(size);
                for (long i = 0; i < size; i++) {
                    final long idx = (firstIdx + i) % capacity;
                    sink.putLong(memory.getLong(startOffset + idx * RECORD_SIZE));
                    sink.putDouble(memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES));
                }
            }
        }

        @Override
        public int snapshotFormatVersion() {
            return 1;
        }

        @Override
        public int snapshotMinSupportedVersion() {
            return 1;
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            tombstoneCount = 0;
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
        // (capacity, startOffset) pairs marking free space within memory. Each
        // entry is a ring slab evicted from a tombstoned partition by
        // retainPartitions (via newCompactionScratch). computeNext's isNew
        // branch (in both this class and the FirstNotNullValue subclass) pops
        // the last pair before falling back to memory.appendAddressFor. The
        // capacity slot mirrors the bounded-RANGE freeList convention; bounded
        // ROWS slabs are always bufferSize doubles long.
        protected final LongList freeList = new LongList();
        // Deep copy of the partition-by key column types. The factory's
        // partitionByKeyTypes buffer is reused across compiles; keeping a copy
        // here outlives that lifetime for partition compaction and the snapshot
        // codec.
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        // Full value-layout (including tombstone slot) for the partition
        // compaction scratch Map. Null for non-live-view compiles.
        protected final ArrayColumnTypes mapValueTypes;
        // holds fixed-size ring buffers of double values
        protected final MemoryARW memory;
        // Value-slot index of the per-partition tombstone byte; -1 outside LV.
        protected double firstValue;
        // Single-writer (refresh worker), not volatile.

        public FirstValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            this(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory,
                    partitionByKeyTypes, liveView,
                    FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV, 3);
        }

        protected FirstValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                ArrayColumnTypes valueTypesLv,
                int longSlotCount
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
            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = valueTypesLv.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(valueTypesLv.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = longSlotCount;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
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
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
                loIdx = 0;
                count = 0;
                final int freeN = freeList.size();
                if (freeN > 0) {
                    // Reuse a slab reclaimed from a tombstoned partition. The
                    // capacity slot is always bufferSize here, so only the
                    // startOffset matters.
                    startOffset = freeList.getQuick(freeN - 1);
                    freeList.setPos(freeN - 2);
                } else {
                    startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                }
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
                firstValue = memory.getDouble(startOffset + (loIdx + bufferSize - count) % bufferSize * Double.BYTES);
            } else {
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            tombstoneCount = 0;
            // memory will allocate on first use
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's bounded-ROWS frame to
            // empty: loIdx=0, count=0, ring slots back to NaN so the next row
            // in the new anchor bucket re-anchors cleanly. The ring's
            // startOffset (slot 1) stays allocated and reuse begins from
            // index 0.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                final long startOffset = value.getLong(1);
                value.putLong(0, 0L);
                value.putLong(2, 0L);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            memory.truncate();
            freeList.clear();
            tombstoneCount = 0;
            long srcOffset = 0;
            final long partitionCount = source.getLong(srcOffset);
            srcOffset += Long.BYTES;
            final long ringBytes = (long) bufferSize * Double.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                srcOffset = LiveViewSnapshotKeyCodec.readKey(key, source, srcOffset, keyColumnTypes);
                MapValue value = key.createValue();
                final long loIdx = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long partitionCountVal = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(newStartOffset + (long) i * Double.BYTES, source.getDouble(srcOffset));
                    srcOffset += Double.BYTES;
                }
                value.putLong(0, loIdx);
                value.putLong(1, newStartOffset);
                value.putLong(2, partitionCountVal);
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            // Two-pass walk so the partition count written first matches the
            // entries that follow even if tombstoneCount drifts between cycles.
            // Tombstoned entries are skipped; the restored Map starts at the
            // post-compaction shape.
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            final long liveCount;
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                liveCount = map.size();
            } else {
                long count = 0;
                while (cursor.hasNext()) {
                    if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                        count++;
                    }
                }
                liveCount = count;
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                sink.putLong(value.getLong(0));
                sink.putLong(value.getLong(2));
                final long startOffset = value.getLong(1);
                for (int i = 0; i < bufferSize; i++) {
                    sink.putDouble(memory.getDouble(startOffset + (long) i * Double.BYTES));
                }
            }
        }

        @Override
        public int snapshotFormatVersion() {
            return 1;
        }

        @Override
        public int snapshotMinSupportedVersion() {
            return 1;
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            freeList.clear();
            tombstoneCount = 0;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), firstValue);
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

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        // Full value layout (including tombstone slot) for the
        // newCompactionScratch Map. Null outside live-view mode.
        protected final ArrayColumnTypes mapValueTypes;
        // Value-slot index of the per-partition tombstone byte; -1 outside LV.
        protected double value;
        // Single-writer (refresh worker), not volatile.

        public FirstValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, ColumnTypes partitionByKeyTypes, boolean liveView, CairoConfiguration configuration) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew() && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (mapValue.isNew() || mapValue.getByte(1) == 0) {
                double d = arg.getDouble(record);
                mapValue.putDouble(0, d);
                mapValue.putByte(1, (byte) 1);
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reopen() {
            super.reopen();
            tombstoneCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            mapValue.putByte(1, (byte) 0);
            if (mapValue.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
            } else if (tombstoneValueIndex >= 0 && mapValue.getByte(tombstoneValueIndex) != 1) {
                mapValue.putByte(tombstoneValueIndex, (byte) 1);
                tombstoneCount++;
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            tombstoneCount = 0;
            long offset = 0;
            final long partitionCount = source.getLong(offset);
            offset += Long.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                offset = LiveViewSnapshotKeyCodec.readKey(key, source, offset, keyColumnTypes);
                MapValue value = key.createValue();
                value.putDouble(0, source.getDouble(offset));
                offset += Double.BYTES;
                value.putByte(1, source.getByte(offset));
                offset += Byte.BYTES;
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            // Two-pass walk: count non-tombstoned partitions, then emit each.
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            final long liveCount;
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                liveCount = map.size();
            } else {
                long count = 0;
                while (cursor.hasNext()) {
                    if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                        count++;
                    }
                }
                liveCount = count;
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : FIRST_VALUE_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                sink.putDouble(value.getDouble(0));
                sink.putByte(value.getByte(1));
            }
        }

        @Override
        public int snapshotFormatVersion() {
            return 1;
        }

        @Override
        public int snapshotMinSupportedVersion() {
            return 1;
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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

        @Override
        public void toTop() {
            super.toTop();
            tombstoneCount = 0;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
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
        // Live-view ANCHOR contract: explicit "initialized" byte signals "no value
        // captured yet for this partition" so resetPartition can re-arm the slot
        // without relying on MapValue.isNew() (only fires on the first access).
        FIRST_VALUE_COLUMN_TYPES.add(ColumnType.BYTE); // initialized flag

        FIRST_VALUE_COLUMN_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // captured value
        FIRST_VALUE_COLUMN_TYPES_LV.add(ColumnType.BYTE);   // initialized flag
        FIRST_VALUE_COLUMN_TYPES_LV.add(ColumnType.BYTE);   // tombstone (anchor-driven compaction)

        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // number of values in current frame
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native array start offset
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native buffer size
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native buffer capacity
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // index of first buffered element

        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // number of values in current frame (used by FirstValue; unused slot for FirstNotNullValue)
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // native array start offset
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // native buffer size
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // native buffer capacity
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // index of first buffered element
        FIRST_VALUE_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // position of current oldest element
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer

        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // position of current oldest element
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // start offset of native array
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // count of values in buffer
        FIRST_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // position of current oldest element
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // first not null index
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer

        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // position of current oldest element
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // start offset of native array
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // first not null index
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // count of values in buffer
        FIRST_NOT_NULL_VALUE_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)
    }
}
