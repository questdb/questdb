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

/**
 * Type-agnostic building blocks shared by the max/min window functions over DATE and TIMESTAMP
 * arguments. Each window shape has one abstract base here (holding all the framing logic and the
 * native-long value buffers), and a thin DATE or TIMESTAMP subclass in
 * {@link MaxTimestampWindowFunctionFactory} / {@link MaxDateWindowFunctionFactory} that only adds
 * the per-type accessor. Min reuses Max's subclasses with a {@code LESS_THAN} comparator.
 * <p>
 * {@link #newInstance} is the single dispatcher: the caller passes a comparator, a name and the
 * per-type constructor references, and the dispatcher selects the shape from the window context.
 */
public class MaxMinWindowFunctionFactoryHelper {

    public static final ArrayColumnTypes MAX_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;

    /**
     * Selects and builds the concrete max/min window function for the current window context.
     * <p>
     * The dispatcher inspects framing mode, partitioning, ordering and frame bounds, allocates the
     * required maps and circular buffers, and delegates the actual instantiation to the per-type
     * constructor references so the same control flow serves both DATE and TIMESTAMP arguments and
     * both max ({@code GREATER_THAN}) and min ({@code LESS_THAN}).
     *
     * @param comparator decides which value wins (max vs. min)
     * @param name       function name used for plans and output
     * @return a window function computing max/min over the requested frame
     * @throws SqlException if window validation fails, RANGE is requested without ordering by the
     *                      designated timestamp, or the parameter combination is not implemented
     */
    static Function newInstance(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            boolean supportNullsDesc,
            TimestampComparator comparator,
            String name,
            CurrentRowConstructor currentRowConstructor,
            PartitionConstructor overPartitionConstructor,
            PartitionRangeConstructor overPartitionRangeConstructor,
            PartitionRowsConstructor overPartitionRowsConstructor,
            PartitionConstructor overUnboundedPartitionRowsConstructor,
            RangeConstructor overRangeConstructor,
            RowsConstructor overRowsConstructor,
            WholeResultSetConstructor overUnboundedRowsConstructor,
            WholeResultSetConstructor overWholeResultSetConstructor
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc);
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new AbstractWindowFunctionFactory.TimestampNullFunction(args.get(0),
                    name,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord,
                    Numbers.LONG_NULL);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving max over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MAX_COLUMN_TYPES
                        );

                        return overPartitionConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                comparator,
                                name
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MAX_COLUMN_TYPES
                        );

                        return overUnboundedPartitionRowsConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                comparator,
                                name
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
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
                        return overPartitionRangeConstructor.newFunction(
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
                                comparator,
                                name
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
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MAX_COLUMN_TYPES
                        );

                        return overUnboundedPartitionRowsConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                comparator,
                                name
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return currentRowConstructor.newFunction(args.get(0), name);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MAX_COLUMN_TYPES
                        );

                        return overPartitionConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                comparator,
                                name
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
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
                        return overPartitionRowsConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                dequeMem,
                                comparator,
                                name
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
                    return overWholeResultSetConstructor.newFunction(args.get(0), comparator, name);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return overUnboundedRowsConstructor.newFunction(args.get(0), comparator, name);
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
                        return overRangeConstructor.newFunction(
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                configuration,
                                mem,
                                dequeMem,
                                timestampIndex,
                                comparator,
                                name
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
                    return overUnboundedRowsConstructor.newFunction(args.get(0), comparator, name);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return currentRowConstructor.newFunction(args.get(0), name);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return overWholeResultSetConstructor.newFunction(args.get(0), comparator, name);
                } // between {unbounded | x} preceding and {x preceding | current row}
                else {
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
                        return overRowsConstructor.newFunction(
                                args.get(0),
                                rowsLo,
                                rowsHi,
                                mem,
                                dequeMem,
                                comparator,
                                name
                        );
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    @FunctionalInterface
    interface CurrentRowConstructor {
        WindowFunction newFunction(Function arg, String name);
    }

    @FunctionalInterface
    interface PartitionConstructor {
        WindowFunction newFunction(Map map,
                                   VirtualRecord partitionByRecord,
                                   RecordSink partitionBySink,
                                   Function arg,
                                   TimestampComparator comparator,
                                   String name);
    }

    @FunctionalInterface
    interface PartitionRangeConstructor {
        WindowFunction newFunction(Map map,
                                   VirtualRecord partitionByRecord,
                                   RecordSink partitionBySink,
                                   long rangeLo,
                                   long rangeHi,
                                   Function arg,
                                   MemoryARW memory,
                                   MemoryARW dequeMemory,
                                   int initialBufferSize,
                                   int timestampIdx,
                                   TimestampComparator comparator,
                                   String name);
    }

    @FunctionalInterface
    interface PartitionRowsConstructor {
        WindowFunction newFunction(Map map,
                                   VirtualRecord partitionByRecord,
                                   RecordSink partitionBySink,
                                   long rowsLo,
                                   long rowsHi,
                                   Function arg,
                                   MemoryARW memory,
                                   MemoryARW dequeMemory,
                                   TimestampComparator comparator,
                                   String name);
    }

    @FunctionalInterface
    interface RangeConstructor {
        WindowFunction newFunction(long rangeLo,
                                   long rangeHi,
                                   Function arg,
                                   CairoConfiguration configuration,
                                   MemoryARW memory,
                                   MemoryARW dequeMemory,
                                   int timestampIdx,
                                   TimestampComparator comparator,
                                   String name);
    }

    @FunctionalInterface
    interface RowsConstructor {
        WindowFunction newFunction(Function arg,
                                   long rowsLo,
                                   long rowsHi,
                                   MemoryARW memory,
                                   MemoryARW dequeMemory,
                                   TimestampComparator comparator,
                                   String name);
    }

    // Avoid autoboxing by not using the Comparator functional interface with generic parameters.
    @FunctionalInterface
    public interface TimestampComparator {
        /**
         * Compare two timestamp values.
         *
         * @param a first timestamp value
         * @param b second timestamp value
         * @return true if `a` is considered greater than `b`, false otherwise
         */
        boolean compare(long a, long b);
    }

    @FunctionalInterface
    interface WholeResultSetConstructor {
        WindowFunction newFunction(Function arg, TimestampComparator comparator, String name);
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    abstract static class MaxMinOverCurrentRowBase extends BaseWindowFunction {

        private final String name;
        protected long value;

        MaxMinOverCurrentRowBase(Function arg, String name) {
            super(arg);
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            value = readArgValue(record);
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles max() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    abstract static class MaxMinOverPartitionBase extends BasePartitionedWindowFunction {
        protected final TimestampComparator comparator;
        protected final String name;

        MaxMinOverPartitionBase(Map map,
                                VirtualRecord partitionByRecord,
                                RecordSink partitionBySink,
                                Function arg,
                                TimestampComparator comparator,
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
            long l = readArgValue(record);
            if (l != Numbers.LONG_NULL) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (!value.isNew()) {
                    if (comparator.compare(l, value.getTimestamp(0))) {
                        value.putTimestamp(0, l);
                    }
                } else {
                    value.putTimestamp(0, l);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            long val = value != null ? value.getTimestamp(0) : Numbers.LONG_NULL;

            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles max() over (partition by x order by ts range between {unbounded | y} preceding and {z preceding | current row})
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When the lower bound is unbounded, we only need to keep one maximum value in history.
    // However, when the lower bound is not unbounded, we need a monotonically deque to maintain the history of records.
    abstract static class MaxMinOverPartitionRangeFrameBase extends BasePartitionedWindowFunction {
        private static final int DEQUE_RECORD_SIZE = Long.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        protected final TimestampComparator comparator;
        protected final LongList dequeFreeList = new LongList();
        protected final int dequeInitialBufferSize;
        // holds another resizable ring buffers as monotonically decreasing deque
        protected final MemoryARW dequeMemory;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        // list of {size, startOffset} pairs marking free space within mem
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        // holds resizable ring buffers
        protected final MemoryARW memory;
        protected final AbstractWindowFunctionFactory.RingBufferDesc memoryDesc = new AbstractWindowFunctionFactory.RingBufferDesc();
        protected final long minDiff;
        protected final String name;
        protected final int timestampIndex;
        // current max value
        protected long maxMin;

        MaxMinOverPartitionRangeFrameBase(
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
                TimestampComparator comparator,
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
            long l = readArgValue(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }

                if (l != Numbers.LONG_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, l);

                    if (frameIncludesCurrentValue) {
                        this.maxMin = l;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        this.maxMin = Numbers.LONG_NULL;
                        frameSize = 0;
                        size = 1;
                    }

                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putLong(dequeStartOffset, l);
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    this.maxMin = Numbers.LONG_NULL;
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
                                        dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE) ==
                                                memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES)) {
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
                if (l != Numbers.LONG_NULL) {
                    if (size == capacity) { //buffer full
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        AbstractWindowFunctionFactory.expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }

                    // add element to buffer
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, l);
                    size++;
                }

                // find new top border of range frame and add new elements
                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            long value = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            while (dequeStartIndex != dequeEndIndex &&
                                    comparator.compare(value, dequeMemory.getLong(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE))) {
                                dequeEndIndex--;
                            }

                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) { // deque full
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                AbstractWindowFunctionFactory.expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }

                            dequeMemory.putLong(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, value);
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        this.maxMin = Numbers.LONG_NULL;
                    } else {
                        this.maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE);
                    }
                } else {
                    long oldMax = mapValue.getTimestamp(5);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (oldMax == Numbers.LONG_NULL || comparator.compare(val, oldMax)) {
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
                mapValue.putTimestamp(5, this.maxMin);
            }
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            maxMin = Numbers.LONG_NULL;
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
    abstract static class MaxMinOverPartitionRowsFrameBase extends BasePartitionedWindowFunction {
        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        protected final int bufferSize;
        protected final TimestampComparator comparator;
        protected final int dequeBufferSize;
        // holds another resizable ring buffers as monotonically decreasing deque
        protected final MemoryARW dequeMemory;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        // holds fixed-size ring buffers of long values
        protected final MemoryARW memory;
        protected final String name;
        protected long maxMin;

        MaxMinOverPartitionRowsFrameBase(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                TimestampComparator comparator,
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
            long l = readArgValue(record);

            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && l != Numbers.LONG_NULL) {
                    this.maxMin = l;
                } else {
                    this.maxMin = Numbers.LONG_NULL;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Numbers.LONG_NULL);
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Long.BYTES) - dequeMemory.getPageAddress(0);
                    if (l != Numbers.LONG_NULL && frameIncludesCurrentValue) {
                        dequeMemory.putLong(dequeStartOffset, l);
                        dequeEndIndex++;
                    }
                } else {
                    value.putTimestamp(2, this.maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                //compute value using top frame element (that could be current or previous row)
                long hiValue = frameIncludesCurrentValue ? l : memory.getLong(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);

                    if (hiValue != Numbers.LONG_NULL) {
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.compare(hiValue, dequeMemory.getLong(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Long.BYTES))) {
                            dequeEndIndex--;
                        }
                        dequeMemory.putLong(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Long.BYTES, hiValue);
                        dequeEndIndex++;
                        this.maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Long.BYTES);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            this.maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Long.BYTES);
                        } else {
                            this.maxMin = Numbers.LONG_NULL;
                        }
                    }
                } else {
                    long max = value.getTimestamp(2);
                    if (hiValue != Numbers.LONG_NULL) {
                        if (max == Numbers.LONG_NULL || comparator.compare(hiValue, max)) {
                            max = hiValue;
                            value.putTimestamp(2, max);
                        }
                    }
                    this.maxMin = max;
                }

                if (frameLoBounded) {
                    //remove the oldest element
                    long loValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    if (loValue != Numbers.LONG_NULL && dequeStartIndex != dequeEndIndex && loValue == dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Long.BYTES)) {
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
            memory.putLong(startOffset + loIdx * Long.BYTES, l);
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
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
    abstract static class MaxMinOverRangeFrameBase extends BaseWindowFunction implements Reopenable {
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        protected final TimestampComparator comparator;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        // holds resizable ring buffers
        // actual frame data - {timestamp, value} pairs - is stored in mem at { offset + first_idx*16, offset + last_idx*16}
        // note: we ignore nulls to reduce memory usage
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final String name;
        protected final int timestampIndex;
        protected long capacity;
        protected long dequeCapacity;
        protected long dequeEndIndex = 0;
        // holds another resizable ring buffers as monotonically decreasing deque
        protected MemoryARW dequeMemory;
        protected long dequeStartIndex = 0;
        protected long dequeStartOffset;
        protected long firstIdx;
        protected long frameSize;
        protected long maxMin;
        protected long size;
        protected long startOffset;

        MaxMinOverRangeFrameBase(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                TimestampComparator comparator,
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
            maxMin = Numbers.LONG_NULL;

            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Long.BYTES) - dequeMemory.getPageAddress(0);
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
            long l = readArgValue(record);

            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        // if rangeHi < 0, some elements from the window can be not in the frame
                        if (frameSize > 0) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (val != Numbers.LONG_NULL && dequeStartIndex != dequeEndIndex && val ==
                                    dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Long.BYTES)) {
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
            if (l != Numbers.LONG_NULL) {
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
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, l);
                size++;
            }

            // find new top border of range frame and add new elements
            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        long value = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);

                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.compare(value, dequeMemory.getLong(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Long.BYTES))) {
                            dequeEndIndex--;
                        }

                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) { // deque full
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Long.BYTES);
                            // call above can end up resizing and thus changing deque memory start address
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;

                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Long.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                //we can't simply copy because that'd leave a gap in the middle
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Long.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Long.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Long.BYTES);
                                dequeStartIndex = 0;
                            }

                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }

                        dequeMemory.putLong(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Long.BYTES, value);
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    this.maxMin = Numbers.LONG_NULL;
                } else {
                    this.maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Long.BYTES);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        if (this.maxMin == Numbers.LONG_NULL || comparator.compare(val, this.maxMin)) {
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Numbers.LONG_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Long.BYTES) - dequeMemory.getPageAddress(0);
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
            maxMin = Numbers.LONG_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeMemory.truncate();
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Long.BYTES) - dequeMemory.getPageAddress(0);
                dequeEndIndex = 0;
                dequeStartIndex = 0;
            }
        }
    }

    // Handles max() over ({order by o} rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    abstract static class MaxMinOverRowsFrameBase extends BaseWindowFunction implements Reopenable {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final TimestampComparator comparator;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final String name;
        protected long dequeBufferSize;
        protected long dequeEndIndex = 0;
        // holds another resizable ring buffers as monotonically decreasing deque
        protected MemoryARW dequeMemory;
        protected long dequeStartIndex = 0;
        protected int loIdx = 0;
        protected long maxMin = Numbers.LONG_NULL;

        MaxMinOverRowsFrameBase(Function arg,
                                long rowsLo,
                                long rowsHi,
                                MemoryARW memory,
                                MemoryARW dequeMemory,
                                TimestampComparator comparator,
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
            long l = readArgValue(record);

            //compute value using top frame element (that could be current or previous row)
            long hiValue = l;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) (loIdx % bufferSize) * Long.BYTES);
            }
            if (hiValue != Numbers.LONG_NULL) {
                if (frameLoBounded) {
                    while (dequeStartIndex != dequeEndIndex &&
                            comparator.compare(hiValue, dequeMemory.getLong(((dequeEndIndex - 1) % dequeBufferSize) * Long.BYTES))) {
                        dequeEndIndex--;
                    }
                    dequeMemory.putLong(dequeEndIndex % dequeBufferSize * Long.BYTES, hiValue);
                    dequeEndIndex++;
                } else {
                    if (maxMin == Numbers.LONG_NULL || comparator.compare(hiValue, maxMin)) {
                        maxMin = hiValue;
                    }
                }
            }

            if (frameLoBounded) {
                this.maxMin = dequeEndIndex == dequeStartIndex ? Numbers.LONG_NULL : dequeMemory.getLong(dequeStartIndex % dequeBufferSize * Long.BYTES);
                //remove the oldest element with newest
                long loValue = buffer.getLong((long) loIdx * Long.BYTES);
                if (loValue != Numbers.LONG_NULL && dequeStartIndex != dequeEndIndex && loValue ==
                        dequeMemory.getLong(dequeStartIndex % dequeBufferSize * Long.BYTES)) {
                    dequeStartIndex++;
                }
            }

            //overwrite oldest element
            buffer.putLong((long) loIdx * Long.BYTES, l);
            loIdx = (loIdx + 1) % bufferSize;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Numbers.LONG_NULL;
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
            maxMin = Numbers.LONG_NULL;
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
            maxMin = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
            if (dequeMemory != null) {
                dequeEndIndex = 0;
                dequeStartIndex = 0;
            }
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Numbers.LONG_NULL);
            }
        }
    }

    // Handles:
    // - max(a) over (partition by x rows between unbounded preceding and current row)
    // - max(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    abstract static class MaxMinOverUnboundedPartitionRowsFrameBase extends BasePartitionedWindowFunction {
        protected final TimestampComparator comparator;
        protected final String name;
        protected long maxMin;

        MaxMinOverUnboundedPartitionRowsFrameBase(Map map,
                                                  VirtualRecord partitionByRecord,
                                                  RecordSink partitionBySink,
                                                  Function arg,
                                                  TimestampComparator comparator,
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
            long l = readArgValue(record);

            if (l != Numbers.LONG_NULL) {
                MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putTimestamp(0, l);
                    this.maxMin = l;
                } else {
                    long max = value.getTimestamp(0);
                    if (comparator.compare(l, max)) {
                        value.putTimestamp(0, l);
                        max = l;
                    }
                    this.maxMin = max;
                }
            } else {
                MapValue value = key.findValue();
                this.maxMin = value != null ? value.getTimestamp(0) : Numbers.LONG_NULL;
            }
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
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
    abstract static class MaxMinOverUnboundedRowsFrameBase extends BaseWindowFunction {
        protected final TimestampComparator comparator;
        protected final String name;
        protected long maxMin = Numbers.LONG_NULL;

        MaxMinOverUnboundedRowsFrameBase(Function arg, TimestampComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            long l = readArgValue(record);
            if (l != Numbers.LONG_NULL && (maxMin == Numbers.LONG_NULL || comparator.compare(l, maxMin))) {
                maxMin = l;
            }
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reset() {
            super.reset();
            maxMin = Numbers.LONG_NULL;
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
            maxMin = Numbers.LONG_NULL;
        }
    }

    // max() over () - empty clause, no partition by no order by, no frame == default frame
    abstract static class MaxMinOverWholeResultSetBase extends BaseWindowFunction {
        protected final TimestampComparator comparator;
        protected final String name;
        protected long maxMin = Numbers.LONG_NULL;

        MaxMinOverWholeResultSetBase(Function arg, TimestampComparator comparator, String name) {
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
            long l = readArgValue(record);
            if (l != Numbers.LONG_NULL && (maxMin == Numbers.LONG_NULL || comparator.compare(l, maxMin))) {
                maxMin = l;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reset() {
            super.reset();
            maxMin = Numbers.LONG_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            maxMin = Numbers.LONG_NULL;
        }
    }

    static {
        MAX_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_COLUMN_TYPES.add(ColumnType.TIMESTAMP); // max value

        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.TIMESTAMP); // max value case when unbounded preceding

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
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.TIMESTAMP); // max value case when unbounded preceding

        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory endIndex
    }
}
