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

public class MaxDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final DoubleComparator GREATER_THAN = (a, b) -> Double.compare(a, b) > 0;
    public static final ArrayColumnTypes MAX_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_COLUMN_TYPES_LV;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV;
    public static final String NAME = "max";
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
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving max over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MAX_COLUMN_TYPES
                    );

                    return new MaxMinOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            GREATER_THAN,
                            NAME
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? MAX_COLUMN_TYPES_LV : MAX_COLUMN_TYPES
                    );

                    return new MaxMinOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            GREATER_THAN,
                            NAME,
                            partitionByKeyTypes,
                            liveView,
                            configuration
                    );
                } // range between {unbounded | x} preceding and {x preceding | current row}, except unbounded preceding to current row
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        final ArrayColumnTypes valueTypes;
                        if (rowsLo == Long.MIN_VALUE) {
                            valueTypes = liveView ? MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV : MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
                        } else {
                            valueTypes = liveView ? MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV : MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES;
                        }
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                valueTypes
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(
                                    configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(),
                                    MemoryTag.NATIVE_CIRCULAR_BUFFER
                            );
                        }

                        // moving max over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new MaxMinOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                dequeMem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                GREATER_THAN,
                                NAME,
                                partitionByKeyTypes,
                                liveView,
                                configuration
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? MAX_COLUMN_TYPES_LV : MAX_COLUMN_TYPES
                    );

                    return new MaxMinOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            GREATER_THAN,
                            NAME,
                            partitionByKeyTypes,
                            liveView,
                            configuration
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new MaxMinOverCurrentRowFunction(args.get(0), NAME);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MAX_COLUMN_TYPES
                    );

                    return new MaxMinOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            GREATER_THAN,
                            NAME
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        final ArrayColumnTypes valueTypes;
                        if (rowsLo == Long.MIN_VALUE) {
                            valueTypes = liveView ? MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV : MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;
                        } else {
                            valueTypes = liveView ? MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV : MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES;
                        }
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, valueTypes);
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(
                                    configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(),
                                    MemoryTag.NATIVE_CIRCULAR_BUFFER
                            );
                        }

                        // moving max over preceding N rows
                        return new MaxMinOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                dequeMem,
                                GREATER_THAN,
                                NAME,
                                partitionByKeyTypes,
                                liveView,
                                configuration
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else { // no partition key
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new MaxMinOverWholeResultSetFunction(args.get(0), GREATER_THAN, NAME);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new MaxMinOverUnboundedRowsFrameFunction(args.get(0), GREATER_THAN, NAME);
                } // range between {unbounded | x} preceding and {x preceding | current row}
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(
                                    configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(),
                                    MemoryTag.NATIVE_CIRCULAR_BUFFER
                            );
                        }
                        // moving max over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new MaxMinOverRangeFrameFunction(
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                configuration,
                                mem,
                                dequeMem,
                                timestampIndex,
                                GREATER_THAN,
                                NAME
                        );
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new MaxMinOverUnboundedRowsFrameFunction(args.get(0), GREATER_THAN, NAME);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new MaxMinOverCurrentRowFunction(args.get(0), NAME);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new MaxMinOverWholeResultSetFunction(args.get(0), GREATER_THAN, NAME);
                } // between {unbounded | x} preceding and {x preceding | current row}
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );
                    MemoryARW dequeMem = null;
                    if (rowsLo != Long.MIN_VALUE) {
                        dequeMem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                    }
                    return new MaxMinOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem,
                            dequeMem,
                            GREATER_THAN,
                            NAME
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // Avoid autoboxing by not using the Comparator functional interface with generic parameters.
    @FunctionalInterface
    public interface DoubleComparator {
        boolean compare(double a, double b);
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class MaxMinOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final String name;
        private double value;

        MaxMinOverCurrentRowFunction(Function arg, String name) {
            super(arg);
            this.name = name;
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
            return name;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles max() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class MaxMinOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final DoubleComparator comparator;
        private final String name;

        public MaxMinOverPartitionFunction(Map map,
                                           VirtualRecord partitionByRecord,
                                           RecordSink partitionBySink,
                                           Function arg,
                                           DoubleComparator comparator,
                                           String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.name = name;
            this.comparator = comparator;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (!value.isNew() && value.getByte(1) == 1) {
                    if (comparator.compare(d, value.getDouble(0))) {
                        value.putDouble(0, d);
                    }
                } else {
                    value.putDouble(0, d);
                    value.putByte(1, (byte) 1);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            double val = value != null && value.getByte(1) == 1 ? value.getDouble(0) : Double.NaN;

            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), val);
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            value.putByte(1, (byte) 0);
        }
    }

    // Handles max() over (partition by x order by ts range between {unbounded | y} preceding and {z preceding | current row})
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When the lower bound is unbounded, we only need to keep one maximum value in history.
    // However, when the lower bound is not unbounded, we need a monotonically deque to maintain the history of records.
    public static class MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private static final int DEQUE_RECORD_SIZE = Double.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final DoubleComparator comparator;
        private final CairoConfiguration configuration;
        private final LongList dequeFreeList = new LongList();
        private final int dequeInitialBufferSize;
        // holds another resizable ring buffers as monotonically decreasing deque
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        // list of {size, startOffset} pairs marking free space within mem
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        private final ArrayColumnTypes mapValueTypes;
        private final long maxDiff;
        // holds resizable ring buffers
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        // current max value
        private double maxMin;

        public MaxMinOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int initialBufferSize,
                int timestampIdx,
                DoubleComparator comparator,
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE; // maxDiff must be used only if frameLoBounded
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            frameIncludesCurrentValue = rangeHi == 0;

            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.liveView = liveView;
            this.configuration = configuration;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                final ArrayColumnTypes srcValueTypes = frameLoBounded
                        ? MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV
                        : MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = srcValueTypes.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(srcValueTypes.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = frameLoBounded ? 9 : 6;
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
            dequeFreeList.clear();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;

            long dequeStartOffset = 0;
            long dequeCapacity = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;
            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }

                if (Numbers.isFinite(d)) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDouble(startOffset + Long.BYTES, d);

                    if (frameIncludesCurrentValue) {
                        this.maxMin = d;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        this.maxMin = Double.NaN;
                        frameSize = 0;
                        size = 1;
                    }

                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putDouble(dequeStartOffset, d);
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    this.maxMin = Double.NaN;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    dequeStartOffset = mapValue.getLong(5);
                    dequeCapacity = mapValue.getLong(6);
                    dequeStartIndex = mapValue.getLong(7);
                    dequeEndIndex = mapValue.getLong(8);

                    // find new bottom border of range frame and remove unneeded elements
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            // if rangeHi < 0, some elements from the window can be not in the frame
                            if (frameSize > 0) {
                                if (dequeStartIndex != dequeEndIndex &&
                                        dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE) ==
                                                memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES)) {
                                    dequeStartIndex++;
                                }
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

                // add new element if not null
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

                // find new top border of range frame and add new elements
                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            double value = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            while (dequeStartIndex != dequeEndIndex &&
                                    comparator.compare(value, dequeMemory.getDouble(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE))) {
                                dequeEndIndex--;
                            }

                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) { // deque full
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }

                            dequeMemory.putDouble(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, value);
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        this.maxMin = Double.NaN;
                    } else {
                        this.maxMin = dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE);
                    }
                } else {
                    double oldMax = mapValue.getDouble(5);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (Numbers.isNull(oldMax) || comparator.compare(val, oldMax)) {
                                oldMax = val;
                            }

                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }

                    firstIdx = newFirstIdx;
                    this.maxMin = oldMax;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
            if (frameLoBounded) {
                mapValue.putLong(5, dequeStartOffset);
                mapValue.putLong(6, dequeCapacity);
                mapValue.putLong(7, dequeStartIndex);
                mapValue.putLong(8, dequeEndIndex);
            } else {
                mapValue.putDouble(5, this.maxMin);
            }
        }

        @Override
        public void compactPartitionMap() {
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                return;
            }
            Map scratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
            try {
                MapRecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    MapValue srcValue = record.getValue();
                    if (srcValue.getByte(tombstoneValueIndex) == 1) {
                        // Reclaim both the primary ring slab and, for the
                        // bounded variant, the monotonic deque slab so
                        // future expandRingBuffer calls can reuse them.
                        // Primary slab: capacity at slot 3, startOffset at
                        // slot 1. Deque slab (bounded only): capacity at
                        // slot 6, startOffset at slot 5.
                        freeList.add(srcValue.getLong(3), srcValue.getLong(1));
                        if (frameLoBounded) {
                            dequeFreeList.add(srcValue.getLong(6), srcValue.getLong(5));
                        }
                        continue;
                    }
                    long srcKeyHash = record.keyHashCode();
                    MapKey dstKey = scratch.withKey();
                    record.copyToKey(dstKey);
                    MapValue dstValue = dstKey.createValue(srcKeyHash);
                    record.copyValue(dstValue);
                }
                Misc.free(map);
                map = scratch;
                scratch = null;
                tombstoneCount = 0;
            } finally {
                Misc.free(scratch);
            }
        }

        @Override
        public double getDouble(Record rec) {
            return maxMin;
        }

        @Override
        public String getName() {
            return name;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            maxMin = Double.NaN;
            tombstoneCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
            dequeFreeList.clear();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's frame and the deque
            // (when bounded). Ring slabs stay allocated; the next post-reset
            // row writes from index 0.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putLong(0, 0L);
                // slot 1 (startOffset) stays.
                value.putLong(2, 0L);
                // slot 3 (capacity) stays.
                value.putLong(4, 0L);
                if (frameLoBounded) {
                    // slots 5 (dequeStartOffset) and 6 (dequeCapacity) stay.
                    value.putLong(7, 0L);
                    value.putLong(8, 0L);
                } else {
                    value.putDouble(5, Double.NaN);
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
            if (dequeMemory != null) {
                dequeMemory.truncate();
            }
            dequeFreeList.clear();
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
                if (frameLoBounded) {
                    final long dequeSize = source.getLong(srcOffset);
                    srcOffset += Long.BYTES;
                    for (long i = 0; i < size; i++) {
                        memory.putLong(newStartOffset + i * RECORD_SIZE, source.getLong(srcOffset));
                        srcOffset += Long.BYTES;
                        memory.putDouble(newStartOffset + i * RECORD_SIZE + Long.BYTES, source.getDouble(srcOffset));
                        srcOffset += Double.BYTES;
                    }
                    final long dequeCapacity = Math.max(dequeSize, dequeInitialBufferSize);
                    final long newDequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                    for (long i = 0; i < dequeSize; i++) {
                        dequeMemory.putDouble(newDequeStartOffset + i * DEQUE_RECORD_SIZE, source.getDouble(srcOffset));
                        srcOffset += Double.BYTES;
                    }
                    value.putLong(0, frameSize);
                    value.putLong(1, newStartOffset);
                    value.putLong(2, size);
                    value.putLong(3, capacity);
                    value.putLong(4, 0L);
                    value.putLong(5, newDequeStartOffset);
                    value.putLong(6, dequeCapacity);
                    value.putLong(7, 0L);
                    value.putLong(8, dequeSize);
                } else {
                    final double maxMinVal = source.getDouble(srcOffset);
                    srcOffset += Double.BYTES;
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
                    value.putDouble(5, maxMinVal);
                }
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
            final ArrayColumnTypes baseTypes = frameLoBounded
                    ? MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES
                    : MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : baseTypes.getColumnCount();
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
                if (frameLoBounded) {
                    final long dequeStartOffset = value.getLong(5);
                    final long dequeCapacity = value.getLong(6);
                    final long dequeStartIndex = value.getLong(7);
                    final long dequeEndIndex = value.getLong(8);
                    final long dequeSize = dequeEndIndex - dequeStartIndex;
                    sink.putLong(dequeSize);
                    for (long i = 0; i < size; i++) {
                        final long idx = (firstIdx + i) % capacity;
                        sink.putLong(memory.getLong(startOffset + idx * RECORD_SIZE));
                        sink.putDouble(memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES));
                    }
                    for (long i = 0; i < dequeSize; i++) {
                        final long idx = (dequeStartIndex + i) % dequeCapacity;
                        sink.putDouble(dequeMemory.getDouble(dequeStartOffset + idx * DEQUE_RECORD_SIZE));
                    }
                } else {
                    sink.putDouble(value.getDouble(5));
                    for (long i = 0; i < size; i++) {
                        final long idx = (firstIdx + i) % capacity;
                        sink.putLong(memory.getLong(startOffset + idx * RECORD_SIZE));
                        sink.putDouble(memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES));
                    }
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
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff);
            } else {
                sink.val("unbounded");
            }
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
            dequeFreeList.clear();
            if (dequeMemory != null) {
                dequeMemory.truncate();
            }
            tombstoneCount = 0;
        }
    }

    // handles max() over (partition by x {order by o} rows between y and z)
    // removable cumulative aggregation
    public static class MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        private final DoubleComparator comparator;
        private final CairoConfiguration configuration;
        private final int dequeBufferSize;
        // (capacity, startOffset) pairs marking free space within dequeMemory.
        // Mirrors freeList for the monotonic deque slab; only populated when
        // frameLoBounded is true.
        private final LongList dequeFreeList = new LongList();
        // holds another resizable ring buffers as monotonically decreasing deque
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // (capacity, startOffset) pairs marking free space within memory. Each
        // entry is a primary-ring slab evicted from a tombstoned partition by
        // compactPartitionMap. computeNext's isNew branch pops the last pair
        // before falling back to memory.appendAddressFor. The capacity slot
        // mirrors the bounded-RANGE freeList convention; bounded ROWS slabs
        // are always bufferSize doubles long.
        private final LongList freeList = new LongList();
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        private final ArrayColumnTypes mapValueTypes;
        // holds fixed-size ring buffers of double values
        private final MemoryARW memory;
        private final String name;
        private double maxMin;

        public MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                DoubleComparator comparator,
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;

            this.memory = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.liveView = liveView;
            this.configuration = configuration;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                final ArrayColumnTypes srcValueTypes = frameLoBounded
                        ? MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV
                        : MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = srcValueTypes.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(srcValueTypes.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = frameLoBounded ? 5 : 3;
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
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            freeList.clear();
            dequeFreeList.clear();
        }

        @Override
        public void compactPartitionMap() {
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                return;
            }
            Map scratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
            try {
                MapRecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    MapValue srcValue = record.getValue();
                    if (srcValue.getByte(tombstoneValueIndex) == 1) {
                        // Reclaim the tombstoned partition's primary ring slab
                        // (and, for frameLoBounded, the deque slab too) so a
                        // future isNew partition can reuse them instead of
                        // growing memory.
                        freeList.add((long) bufferSize, srcValue.getLong(1));
                        if (frameLoBounded) {
                            dequeFreeList.add((long) dequeBufferSize, srcValue.getLong(2));
                        }
                        continue;
                    }
                    long srcKeyHash = record.keyHashCode();
                    MapKey dstKey = scratch.withKey();
                    record.copyToKey(dstKey);
                    MapValue dstValue = dstKey.createValue(srcKeyHash);
                    record.copyValue(dstValue);
                }
                Misc.free(map);
                map = scratch;
                scratch = null;
                tombstoneCount = 0;
            } finally {
                Misc.free(scratch);
            }
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - (0-based) index of oldest value {0, bufferSize}
            // 1 - native array start offset (relative to memory address)
            // we keep nulls in window and reject them when computing max
            // 2 - max value if frameLoBounded is false
            // 2 - deque memory startOffset is frameLoBounded is true
            // 3 - deque memory startIndex is frameLoBounded is true
            // 4 - deque memory endIndex is frameLoBounded is true

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx; //current index of lo frame value ('oldest')
            long startOffset;
            double d = arg.getDouble(record);

            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
                loIdx = 0;
                final int freeN = freeList.size();
                if (freeN > 0) {
                    // Reuse a primary slab reclaimed from a tombstoned
                    // partition. Capacity is always bufferSize; only the
                    // startOffset matters.
                    startOffset = freeList.getQuick(freeN - 1);
                    freeList.setPos(freeN - 2);
                } else {
                    startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                }
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    this.maxMin = d;
                } else {
                    this.maxMin = Double.NaN;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
                if (frameLoBounded) {
                    final int dequeFreeN = dequeFreeList.size();
                    if (dequeFreeN > 0) {
                        // Reuse a deque slab reclaimed from a tombstoned
                        // partition. Capacity is always dequeBufferSize.
                        dequeStartOffset = dequeFreeList.getQuick(dequeFreeN - 1);
                        dequeFreeList.setPos(dequeFreeN - 2);
                    } else {
                        dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Double.BYTES) - dequeMemory.getPageAddress(0);
                    }
                    if (Numbers.isFinite(d) && frameIncludesCurrentValue) {
                        dequeMemory.putDouble(dequeStartOffset, d);
                        dequeEndIndex++;
                    }
                } else {
                    value.putDouble(2, this.maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                //compute value using top frame element (that could be current or previous row)
                double hiValue = frameIncludesCurrentValue ? d : memory.getDouble(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);

                    if (Numbers.isFinite(hiValue)) {
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.compare(hiValue, dequeMemory.getDouble(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Double.BYTES))) {
                            dequeEndIndex--;
                        }
                        dequeMemory.putDouble(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Double.BYTES, hiValue);
                        dequeEndIndex++;
                        this.maxMin = dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Double.BYTES);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            this.maxMin = dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Double.BYTES);
                        } else {
                            this.maxMin = Double.NaN;
                        }
                    }
                } else {
                    double max = value.getDouble(2);
                    if (Numbers.isFinite(hiValue)) {
                        if (Numbers.isNull(max) || comparator.compare(hiValue, max)) {
                            max = hiValue;
                            value.putDouble(2, max);
                        }
                    }
                    this.maxMin = max;
                }

                if (frameLoBounded) {
                    //remove the oldest element
                    double loValue = memory.getDouble(startOffset + loIdx * Double.BYTES);
                    if (Numbers.isFinite(loValue) && dequeStartIndex != dequeEndIndex && loValue == dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Double.BYTES)) {
                        dequeStartIndex++;
                    }
                }
            }

            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(1, startOffset);
            if (frameLoBounded) {
                value.putLong(2, dequeStartOffset);
                value.putLong(3, dequeStartIndex);
                value.putLong(4, dequeEndIndex);
            }
            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return maxMin;
        }

        @Override
        public String getName() {
            return name;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            dequeFreeList.clear();
            tombstoneCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            freeList.clear();
            dequeFreeList.clear();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                final long startOffset = value.getLong(1);
                value.putLong(0, 0L);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
                if (frameLoBounded) {
                    // slot 2 = dequeStartOffset stays allocated; reset indices.
                    value.putLong(3, 0L);
                    value.putLong(4, 0L);
                } else {
                    value.putDouble(2, Double.NaN);
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
            if (dequeMemory != null) {
                dequeMemory.truncate();
            }
            freeList.clear();
            dequeFreeList.clear();
            tombstoneCount = 0;
            long srcOffset = 0;
            final long partitionCount = source.getLong(srcOffset);
            srcOffset += Long.BYTES;
            final long ringBytes = (long) bufferSize * Double.BYTES;
            final long dequeBytes = (long) dequeBufferSize * Double.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                srcOffset = LiveViewSnapshotKeyCodec.readKey(key, source, srcOffset, keyColumnTypes);
                MapValue value = key.createValue();
                final long loIdx = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
                if (frameLoBounded) {
                    final long dequeStartIndex = source.getLong(srcOffset);
                    srcOffset += Long.BYTES;
                    final long dequeEndIndex = source.getLong(srcOffset);
                    srcOffset += Long.BYTES;
                    for (int i = 0; i < bufferSize; i++) {
                        memory.putDouble(newStartOffset + (long) i * Double.BYTES, source.getDouble(srcOffset));
                        srcOffset += Double.BYTES;
                    }
                    final long newDequeStartOffset = dequeMemory.appendAddressFor(dequeBytes) - dequeMemory.getPageAddress(0);
                    for (int i = 0; i < dequeBufferSize; i++) {
                        dequeMemory.putDouble(newDequeStartOffset + (long) i * Double.BYTES, source.getDouble(srcOffset));
                        srcOffset += Double.BYTES;
                    }
                    value.putLong(0, loIdx);
                    value.putLong(1, newStartOffset);
                    value.putLong(2, newDequeStartOffset);
                    value.putLong(3, dequeStartIndex);
                    value.putLong(4, dequeEndIndex);
                } else {
                    final double maxMinVal = source.getDouble(srcOffset);
                    srcOffset += Double.BYTES;
                    for (int i = 0; i < bufferSize; i++) {
                        memory.putDouble(newStartOffset + (long) i * Double.BYTES, source.getDouble(srcOffset));
                        srcOffset += Double.BYTES;
                    }
                    value.putLong(0, loIdx);
                    value.putLong(1, newStartOffset);
                    value.putDouble(2, maxMinVal);
                }
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
            final ArrayColumnTypes baseTypes = frameLoBounded
                    ? MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES
                    : MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : baseTypes.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                sink.putLong(value.getLong(0));
                final long startOffset = value.getLong(1);
                if (frameLoBounded) {
                    sink.putLong(value.getLong(3));
                    sink.putLong(value.getLong(4));
                    for (int i = 0; i < bufferSize; i++) {
                        sink.putDouble(memory.getDouble(startOffset + (long) i * Double.BYTES));
                    }
                    final long dequeStartOffset = value.getLong(2);
                    for (int i = 0; i < dequeBufferSize; i++) {
                        sink.putDouble(dequeMemory.getDouble(dequeStartOffset + (long) i * Double.BYTES));
                    }
                } else {
                    sink.putDouble(value.getDouble(2));
                    for (int i = 0; i < bufferSize; i++) {
                        sink.putDouble(memory.getDouble(startOffset + (long) i * Double.BYTES));
                    }
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
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());

            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            if (dequeMemory != null) {
                dequeMemory.truncate();
            }
            freeList.clear();
            dequeFreeList.clear();
            tombstoneCount = 0;
        }
    }

    // Handles max() over ({order by ts} range between {unbounded | x} preceding and { x preceding | current row } ); no partition by key
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value.
    // When the lower bound is unbounded, we only need to keep one maximum value(max) in history.
    // However, when the lower bound is not unbounded, we need a monotonically deque to maintain the history of records.
    public static class MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {

        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final DoubleComparator comparator;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        // holds resizable ring buffers
        // actual frame data - {timestamp, value} pairs - is stored in mem at { offset + first_idx*16, offset + last_idx*16}
        // note: we ignore nulls to reduce memory usage
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        // holds another resizable ring buffers as monotonically decreasing deque
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private double maxMin;
        private long size;
        private long startOffset;

        public MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                DoubleComparator comparator,
                String name
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE; // maxDiff must be used only if frameLoBounded
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;

            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin = Double.NaN;

            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Double.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        // if rangeHi < 0, some elements from the window can be not in the frame
                        if (frameSize > 0) {
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (Numbers.isFinite(val) && dequeStartIndex != dequeEndIndex && val ==
                                    dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Double.BYTES)) {
                                dequeStartIndex++;
                            }
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

            // add new element if not null
            if (Numbers.isFinite(d)) {
                if (size == capacity) { //buffer full
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    // call above can end up resizing and thus changing memory start address
                    long oldAddress = memory.getPageAddress(0) + startOffset;

                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        //we can't simply copy because that'd leave a gap in the middle
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
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

            // find new top border of range frame and add new elements
            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        double value = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);

                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.compare(value, dequeMemory.getDouble(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Double.BYTES))) {
                            dequeEndIndex--;
                        }

                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) { // deque full
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Double.BYTES);
                            // call above can end up resizing and thus changing deque memory start address
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;

                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Double.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                //we can't simply copy because that'd leave a gap in the middle
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Double.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Double.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Double.BYTES);
                                dequeStartIndex = 0;
                            }

                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }

                        dequeMemory.putDouble(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Double.BYTES, value);
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    this.maxMin = Double.NaN;
                } else {
                    this.maxMin = dequeMemory.getDouble(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Double.BYTES);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        if (Numbers.isNull(this.maxMin) || comparator.compare(val, this.maxMin)) {
                            this.maxMin = val;
                        }
                        frameSize++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return maxMin;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Double.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff);
            } else {
                sink.val("unbounded");
            }
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
            maxMin = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeMemory.truncate();
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Double.BYTES) - dequeMemory.getPageAddress(0);
                dequeEndIndex = 0;
                dequeStartIndex = 0;
            }
        }
    }

    // Handles max() over ({order by o} rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final DoubleComparator comparator;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final String name;
        private long dequeBufferSize;
        private long dequeEndIndex = 0;
        // holds another resizable ring buffers as monotonically decreasing deque
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private int loIdx = 0;
        private double maxMin = Double.NaN;

        public MaxMinOverRowsFrameFunction(Function arg,
                                           long rowsLo,
                                           long rowsHi,
                                           MemoryARW memory,
                                           MemoryARW dequeMemory,
                                           DoubleComparator comparator,
                                           String name) {
            super(arg);

            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use MaxOverUnboundedRowsFrameFunction in case of (Long.MIN_VALUE, 0) range
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
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }

            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeBufferSize = rowsHi - rowsLo + 1;
            }
            this.comparator = comparator;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);

            //compute value using top frame element (that could be current or previous row)
            double hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) (loIdx % bufferSize) * Double.BYTES);
            }
            if (Numbers.isFinite(hiValue)) {
                if (frameLoBounded) {
                    while (dequeStartIndex != dequeEndIndex &&
                            comparator.compare(hiValue, dequeMemory.getDouble(((dequeEndIndex - 1) % dequeBufferSize) * Double.BYTES))) {
                        dequeEndIndex--;
                    }
                    dequeMemory.putDouble(dequeEndIndex % dequeBufferSize * Double.BYTES, hiValue);
                    dequeEndIndex++;
                } else {
                    if (Numbers.isNull(maxMin) || comparator.compare(hiValue, maxMin)) {
                        maxMin = hiValue;
                    }
                }
            }

            if (frameLoBounded) {
                this.maxMin = dequeEndIndex == dequeStartIndex ? Double.NaN : dequeMemory.getDouble(dequeStartIndex % dequeBufferSize * Double.BYTES);
                //remove the oldest element with newest
                double loValue = buffer.getDouble((long) loIdx * Double.BYTES);
                if (Numbers.isFinite(loValue) && dequeStartIndex != dequeEndIndex && loValue ==
                        dequeMemory.getDouble(dequeStartIndex % dequeBufferSize * Double.BYTES)) {
                    dequeStartIndex++;
                }
            }

            //overwrite oldest element
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return maxMin;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Double.NaN;
            loIdx = 0;
            initBuffer();
            if (dequeMemory != null) {
                dequeEndIndex = 0;
                dequeStartIndex = 0;
            }
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
                dequeEndIndex = 0;
                dequeStartIndex = 0;
            }
            loIdx = 0;
            maxMin = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            maxMin = Double.NaN;
            loIdx = 0;
            initBuffer();
            if (dequeMemory != null) {
                dequeEndIndex = 0;
                dequeStartIndex = 0;
            }
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - max(a) over (partition by x rows between unbounded preceding and current row)
    // - max(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    public static class MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final DoubleComparator comparator;
        private final CairoConfiguration configuration;
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        // Full value layout (including tombstone slot) for the
        // compactPartitionMap scratch Map. Null outside live-view mode.
        private final ArrayColumnTypes mapValueTypes;
        private final String name;
        // Value-slot index of the per-partition tombstone byte; -1 outside LV.
        private double maxMin;
        // Single-writer (refresh worker), not volatile.

        public MaxMinOverUnboundedPartitionRowsFrameFunction(Map map,
                                                             VirtualRecord partitionByRecord,
                                                             RecordSink partitionBySink,
                                                             Function arg,
                                                             DoubleComparator comparator,
                                                             String name,
                                                             ColumnTypes partitionByKeyTypes,
                                                             boolean liveView,
                                                             CairoConfiguration configuration) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = MAX_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(MAX_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        public void compactPartitionMap() {
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                return;
            }
            Map scratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
            try {
                MapRecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    MapValue srcValue = record.getValue();
                    if (srcValue.getByte(tombstoneValueIndex) == 1) {
                        continue;
                    }
                    long srcKeyHash = record.keyHashCode();
                    MapKey dstKey = scratch.withKey();
                    record.copyToKey(dstKey);
                    MapValue dstValue = dstKey.createValue(srcKeyHash);
                    record.copyValue(dstValue);
                }
                Misc.free(map);
                map = scratch;
                scratch = null;
                tombstoneCount = 0;
            } finally {
                Misc.free(scratch);
            }
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            double d = arg.getDouble(record);

            if (Numbers.isFinite(d)) {
                MapValue value = key.createValue();
                if (value.isNew() && tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (value.isNew() || value.getByte(1) == 0) {
                    value.putDouble(0, d);
                    value.putByte(1, (byte) 1);
                    this.maxMin = d;
                } else {
                    double max = value.getDouble(0);
                    if (comparator.compare(d, max)) {
                        value.putDouble(0, d);
                        max = d;
                    }
                    this.maxMin = max;
                }
            } else {
                MapValue value = key.findValue();
                this.maxMin = value != null && value.getByte(1) == 1 ? value.getDouble(0) : Double.NaN;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return maxMin;
        }

        @Override
        public String getName() {
            return name;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            MapValue value = key.createValue();
            value.putByte(1, (byte) 0);
            if (value.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            } else if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                value.putByte(tombstoneValueIndex, (byte) 1);
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
                    : MAX_COLUMN_TYPES.getColumnCount();
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

    // Handles max() over (rows between unbounded preceding and current row); there's no partition by.
    public static class MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final DoubleComparator comparator;
        private final String name;
        private double maxMin = Double.NaN;

        public MaxMinOverUnboundedRowsFrameFunction(Function arg, DoubleComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d) && (Numbers.isNull(maxMin) || comparator.compare(d, maxMin))) {
                maxMin = d;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return maxMin;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reset() {
            super.reset();
            maxMin = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            maxMin = Double.NaN;
        }
    }

    // max() over () - empty clause, no partition by no order by, no frame == default frame
    static class MaxMinOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final DoubleComparator comparator;
        private final String name;
        private double maxMin = Double.NaN;

        public MaxMinOverWholeResultSetFunction(Function arg, DoubleComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d) && (Numbers.isNull(maxMin) || comparator.compare(d, maxMin))) {
                maxMin = d;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reset() {
            super.reset();
            maxMin = Double.NaN;
        }

        @Override
        public void toTop() {
            super.toTop();
            maxMin = Double.NaN;
        }
    }

    static {
        MAX_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_COLUMN_TYPES.add(ColumnType.DOUBLE); // max value
        // Live-view ANCHOR contract: an explicit "initialized" byte lets resetPartition
        // signal "no value yet for this partition" without deleting the map entry. The
        // MapValue's intrinsic isNew() flips to false on first access, which is too
        // coarse for repeated resets within the same partition.
        MAX_COLUMN_TYPES.add(ColumnType.BYTE); // initialized flag

        MAX_COLUMN_TYPES_LV = new ArrayColumnTypes();
        MAX_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // max value
        MAX_COLUMN_TYPES_LV.add(ColumnType.BYTE);   // initialized flag
        MAX_COLUMN_TYPES_LV.add(ColumnType.BYTE);   // tombstone (anchor-driven compaction)

        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // max value case when unbounded preceding

        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // max value case when unbounded preceding
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory capacity
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory endIndex

        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory capacity
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory endIndex
        MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE); // max value case when unbounded preceding

        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // max value case when unbounded preceding
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory endIndex

        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.LONG); // deque memory endIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)
    }
}
