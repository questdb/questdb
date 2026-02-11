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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class MaxDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final DoubleComparator GREATER_THAN = (a, b) -> Double.compare(a, b) > 0;
    public static final ArrayColumnTypes MAX_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;
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
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MAX_COLUMN_TYPES
                    );

                    return new MaxMinOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            GREATER_THAN,
                            NAME
                    );
                } // range between {unbounded | x} preceding and {x preceding | current row}, except unbounded preceding to current row
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_OVER_PARTITION_RANGE_COLUMN_TYPES : MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES
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
                                NAME
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
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MAX_COLUMN_TYPES
                    );

                    return new MaxMinOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            GREATER_THAN,
                            NAME
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
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_OVER_PARTITION_ROWS_COLUMN_TYPES : MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES
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
                                NAME
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
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

                if (!value.isNew()) {
                    if (comparator.compare(d, value.getDouble(0))) {
                        value.putDouble(0, d);
                    }
                } else {
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

    // Handles max() over (partition by x order by ts range between {unbounded | y} preceding and {z preceding | current row})
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When the lower bound is unbounded, we only need to keep one maximum value in history.
    // However, when the lower bound is not unbounded, we need a monotonically deque to maintain the history of records.
    public static class MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private static final int DEQUE_RECORD_SIZE = Double.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final DoubleComparator comparator;
        private final LongList dequeFreeList = new LongList();
        private final int dequeInitialBufferSize;
        // holds another resizable ring buffers as monotonically decreasing deque
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        // list of {size, startOffset} pairs marking free space within mem
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
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
                String name
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            maxMin = Double.NaN;
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
        }
    }

    // handles max() over (partition by x {order by o} rows between y and z)
    // removable cumulative aggregation
    public static class MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        private final DoubleComparator comparator;
        private final int dequeBufferSize;
        // holds another resizable ring buffers as monotonically decreasing deque
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
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
                String name
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
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    this.maxMin = d;
                } else {
                    this.maxMin = Double.NaN;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Double.BYTES) - dequeMemory.getPageAddress(0);
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
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
    static class MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final DoubleComparator comparator;
        private final String name;
        private double maxMin;

        public MaxMinOverUnboundedPartitionRowsFrameFunction(Map map,
                                                             VirtualRecord partitionByRecord,
                                                             RecordSink partitionBySink,
                                                             Function arg,
                                                             DoubleComparator comparator,
                                                             String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            double d = arg.getDouble(record);

            if (Numbers.isFinite(d)) {
                MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putDouble(0, d);
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
                this.maxMin = value != null ? value.getDouble(0) : Double.NaN;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), maxMin);
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

        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // max value case when unbounded preceding

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

        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE); // max value case when unbounded preceding

        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory endIndex
    }
}
