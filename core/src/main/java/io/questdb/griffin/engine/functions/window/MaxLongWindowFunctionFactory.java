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

public class MaxLongWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final LongComparator GREATER_THAN = (a, b) -> a > b;
    public static final ArrayColumnTypes MAX_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;
    public static final String NAME = "max";
    private static final String SIGNATURE = NAME + "(L)";

    /**
     * Returns the SQL function signature for this factory.
     *
     * @return the signature string ("max(N)")
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Create a window-function instance that computes the maximum long value for the current window
     * specification held in the provided SqlExecutionContext.
     *
     * <p>The factory inspects the WindowContext (framing mode, partitioning, ORDER BY, frame bounds)
     * and returns a specialized Function implementation optimized for that combination
     * (examples: per-partition aggregation, ROWS/RANGE bounded/unbounded frames, current-row,
     * whole-result-set two-pass, memory-backed ring buffers with optional deque for sliding frames,
     * or a LongNullFunction for invalid bounds).</p>
     *
     * @param position            parser/position index used to report errors
     * @param args                function argument list (expected to contain the long-valued expression)
     * @param argPositions        positions of the arguments (used for diagnostics)
     * @param configuration       execution configuration (used for allocating buffers) — not documented as a parameter per project convention
     * @param sqlExecutionContext execution context containing the WindowContext and runtime state — not documented as a parameter per project convention
     * @return a Function specialized for computing the MAX(long) over the configured window
     * @throws SqlException if the requested RANGE frame is used without ordering by the designated timestamp
     *                      or if the combination of window parameters is not implemented
     */
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
            return new LongNullFunction(args.get(0),
                    NAME,
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
    public interface LongComparator {
        /**
         * Compare two primitive long values.
         *
         * @param a first value
         * @param b second value
         * @return {@code true} if {@code a} is greater than {@code b}; {@code false} otherwise
         */
        boolean compare(long a, long b);
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class MaxMinOverCurrentRowFunction extends BaseWindowFunction implements WindowLongFunction {

        private final String name;
        private long value;

        /**
         * Creates a window function that returns the argument's value from the current row.
         * <p>
         * This implementation does not perform any windowing beyond reading the current record's value.
         *
         * @param arg  the input value expression whose current-row value will be returned
         * @param name the function name used in plan/output identification
         */
        MaxMinOverCurrentRowFunction(Function arg, String name) {
            super(arg);
            this.name = name;
        }

        /**
         * Reads the long value from the given record and stores it as the current value.
         *
         * @param record source record to read the value from
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getLong(record);
        }

        /**
         * Return the previously computed value for the current row.
         * <p>
         * The provided Record is ignored; this method returns the value stored by the function
         * (e.g., set during the preceding evaluation pass).
         *
         * @param rec ignored
         * @return the stored long value for the current row
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return ZERO_PASS indicating this function requires no processing passes.
         */
        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        /**
         * Computes the next value for the current input record and writes it as a long
         * into the function's output column for that row.
         * <p>
         * The method updates internal computation state by calling computeNext(record)
         * and then stores the resulting long value at the output address obtained from
         * the provided WindowSPI for the given record offset and this function's
         * columnIndex.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles max() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class MaxMinOverPartitionFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private final LongComparator comparator;
        private final String name;

        /**
         * Constructs a partition-based max/min window function.
         * <p>
         * This instance evaluates the aggregate (max or min) over each partition using the
         * provided comparator to determine the ordering and the supplied name for identification.
         *
         * @param comparator comparator used to choose the representative long (e.g., greater-than for max)
         * @param name       human-readable name for the function instance (used in plan/output)
         */
        public MaxMinOverPartitionFunction(Map map,
                                           VirtualRecord partitionByRecord,
                                           RecordSink partitionBySink,
                                           Function arg,
                                           LongComparator comparator,
                                           String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.name = name;
            this.comparator = comparator;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Indicates the function requires two processing passes.
         * <p>
         * The first pass aggregates values (e.g., computes a partition or global maximum)
         * and the second pass emits results for each row.
         *
         * @return WindowFunction.TWO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        /**
         * First pass: updates the per-partition maximum with the value from the current record.
         * <p>
         * If the argument value is not NULL, this method obtains the partition key for the record,
         * looks up or creates the per-partition MapValue and stores the larger of the existing
         * stored value and the current value using the configured comparator.
         * <p>
         * Side effects:
         * - Mutates the partition map backing store (inserting a new entry when needed or updating
         * an existing entry's stored maximum).
         * <p>
         * Nulls are ignored (no map access or modification when the argument is `Numbers.LONG_NULL`).
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long l = arg.getLong(record);
            if (l != Numbers.LONG_NULL) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (!value.isNew()) {
                    if (comparator.compare(l, value.getLong(0))) {
                        value.putLong(0, l);
                    }
                } else {
                    value.putLong(0, l);
                }
            }
        }

        /**
         * Writes the precomputed maximum value for the current partition to the output column for the given record.
         * <p>
         * Looks up the partition key derived from {@code record} in the per-partition map; if a value is present,
         * its stored long is written to the output address obtained from {@code spi} and {@code recordOffset},
         * otherwise {@link Numbers#LONG_NULL} is written.
         *
         * @param record       the input record used to derive the partition key
         * @param recordOffset the offset passed to WindowSPI used to obtain the output write address
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            long val = value != null ? value.getLong(0) : Numbers.LONG_NULL;

            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles max() over (partition by x order by ts range between {unbounded | y} preceding and {z preceding | current row})
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When the lower bound is unbounded, we only need to keep one maximum value in history.
    // However, when the lower bound is not unbounded, we need a monotonically deque to maintain the history of records.
    public static class MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private static final int DEQUE_RECORD_SIZE = Long.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final LongComparator comparator;
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
        private long maxMin;

        /**
         * Constructs a partitioned RANGE-frame window function instance that computes the maximum
         * over a timestamp-based frame for each partition.
         * <p>
         * The constructor initializes internal memory buffers, deque storage (optional), frame
         * boundary flags and comparison behavior used by the implementation.
         *
         * @param rangeLo           lower bound of the RANGE frame in timestamp units; Long.MIN_VALUE indicates unbounded preceding
         * @param rangeHi           upper bound of the RANGE frame in timestamp units; zero means the frame includes the current row's timestamp
         * @param arg               function that provides the value to be aggregated (input column)
         * @param memory            primary ring buffer storage for timestamp/value pairs for each partition
         * @param dequeMemory       optional deque storage used to maintain candidate maxima within the frame; may be null when not needed
         * @param initialBufferSize initial capacity (in rows) for the ring buffer(s)
         * @param timestampIdx      index of the designated timestamp column within the stored records
         * @param comparator        comparator used to determine the current maximum between two long values
         * @param name              human-readable name for the window function instance (used in planning/output)
         */
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
                LongComparator comparator,
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

        /**
         * Releases native resources and clears internal buffers used by the window function.
         * <p>
         * Closes the primary memory region, clears internal free-list trackers, and closes
         * the optional deque memory if it was allocated. Also invokes the superclass {@code close()}
         * to perform any additional cleanup.
         */
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

        /**
         * Advances the per-partition RANGE frame state with the given record and computes the current maximum value
         * for the frame.
         *
         * <p>This updates or creates the per-partition ring buffer and optional monotonic deque stored in the
         * factory's map, handles NULL inputs, grows underlying memory buffers when full, expires/retains rows
         * according to the configured RANGE bounds, and sets the instance's current max value (maxMin).
         *
         * @param record the input record whose timestamp and value are added to the partition frame;
         *               may be used to create or update the partition's buffer and to compute the frame's max
         */
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
            long l = arg.getLong(record);

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
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
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
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
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
                    long oldMax = mapValue.getLong(5);
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
                mapValue.putLong(5, this.maxMin);
            }
        }

        /**
         * Return the previously computed max value for the current row.
         * <p>
         * The provided Record parameter is ignored; this function returns the value
         * cached in the instance (set during pass1).
         *
         * @param rec ignored
         * @return the cached maximum long value (may be LONG_NULL if no non-null input was seen)
         */
        @Override
        public long getLong(Record rec) {
            return maxMin;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reopens the function instance for reuse.
         * <p>
         * Resets the cached maximum value to `LONG_NULL` and defers any memory
         * allocation until the value is first needed.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            maxMin = Numbers.LONG_NULL;
        }

        /**
         * Reset internal state and release allocated native resources used by the function.
         *
         * <p>Performs superclass reset, closes and releases the primary memory buffer,
         * clears internal free lists used for ring/deque element recycling, and closes
         * the optional deque memory if it was allocated.</p>
         */
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

        /**
         * Appends a textual plan fragment describing this window function to the provided PlanSink.
         * <p>
         * The produced fragment has the form:
         * `max({arg}) over (partition by {partitionFunctions} range between {lower} preceding and {upper})`
         * where `{lower}` is either the numeric `maxDiff` when the lower bound is bounded or `unbounded` otherwise,
         * and `{upper}` is `current row` when `minDiff == 0` or `{minDiff} preceding` otherwise.
         */
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

        /**
         * Reset this function instance to its initial, empty state for reuse.
         * <p>
         * Clears internal buffers and free lists and truncates associated off-heap memory
         * so the instance can be reused without retaining prior frame data.
         */
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
    public static class MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        private final LongComparator comparator;
        private final int dequeBufferSize;
        // holds another resizable ring buffers as monotonically decreasing deque
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // holds fixed-size ring buffers of long values
        private final MemoryARW memory;
        private final String name;
        private long maxMin;

        /**
         * Constructs a rows-framed, partition-aware max window function instance and initializes
         * internal frame and buffer sizes derived from the provided rows bounds.
         *
         * <p>Computations performed:
         * - If rowsLo is bounded (> Long.MIN_VALUE) the constructor treats the frame as having a
         * bounded number of preceding rows and sets frameSize, bufferSize and dequeBufferSize
         * accordingly.
         * - If rowsLo is unbounded (Long.MIN_VALUE) the constructor treats the frame as having an
         * unbounded preceding side and sets buffer sizing for a bounded following side.
         * - Determines whether the frame includes the current row (rowsHi == 0).
         * - Stores provided memory buffers and comparator for use by the frame implementation.</p>
         *
         * @param rowsLo      the lower rows bound (number of rows preceding current row). Use Long.MIN_VALUE
         *                    to indicate "UNBOUNDED PRECEDING".
         * @param rowsHi      the upper rows bound (number of rows following current row or 0 for current row).
         * @param arg         the input function that produces the long values to aggregate (the value source).
         * @param memory      a MemoryARW instance used as the primary ring buffer for timestamps/values.
         * @param dequeMemory optional MemoryARW used to store the monotonic deque when needed;
         *                    may be null if no deque is required.
         * @param comparator  comparator used to compare long values for determining the max.
         * @param name        descriptive name for the window function instance (used in plan/debug output).
         */
        public MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                LongComparator comparator,
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

        /**
         * Closes the window function and releases internal resources.
         * <p>
         * Calls the superclass close implementation then closes the memory buffer used by this instance.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
        }

        /**
         * Advance the window by one record: update per-partition ring buffer and deque state and compute the current max.
         *
         * <p>This method:
         * - Looks up or initializes per-partition state in the backing map.
         * - Maintains a circular buffer of recent values in native memory and an optional monotonic deque (for bounded lower frames)
         * to support O(1) retrieval of the frame maximum.
         * - Handles null input values by preserving nulls in the buffer but ignoring them when computing the max.
         * - Updates the partition map with the new buffer indices and deque metadata and writes the incoming value into memory.
         * - Sets the instance field `maxMin` to the current frame maximum (or `Numbers.LONG_NULL` if no non-null values are present).
         * <p>
         * The implementation assumes `bufferSize`, `dequeBufferSize`, `frameSize`, `frameIncludesCurrentValue`, `frameLoBounded`,
         * `memory`, `dequeMemory`, `map`, `partitionByRecord`, `partitionBySink`, `arg`, `comparator`, and related fields are valid
         * and correctly configured by the surrounding class.
         *
         * @param record the input record to advance the window with; its long value is read via `arg.getLong(record)`
         */
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
            long l = arg.getLong(record);

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
                    value.putLong(2, this.maxMin);
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
                    long max = value.getLong(2);
                    if (hiValue != Numbers.LONG_NULL) {
                        if (max == Numbers.LONG_NULL || comparator.compare(hiValue, max)) {
                            max = hiValue;
                            value.putLong(2, max);
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

        /**
         * Return the previously computed max value for the current row.
         * <p>
         * The provided Record parameter is ignored; this function returns the value
         * cached in the instance (set during pass1).
         *
         * @param rec ignored
         * @return the cached maximum long value (may be LONG_NULL if no non-null input was seen)
         */
        @Override
        public long getLong(Record rec) {
            return maxMin;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass processing for the window function: computes the next value for the current
         * input record and writes the current maximum value (as a long) to the output column
         * slot addressed by {@code spi.getAddress(recordOffset, columnIndex)}.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reopens resources for reuse by the planner.
         * <p>
         * Delegates to the superclass implementation. Called when the function instance is
         * reopened (for example when a plan is reused) to ensure internal state or resources
         * are reset or reinitialized as required by the parent class.
         */
        @Override
        public void reopen() {
            super.reopen();
        }

        /**
         * Reset internal state and release native memory resources held by this instance.
         *
         * <p>Calls {@code super.reset()}, closes the primary {@code memory} buffer and, if present,
         * the {@code dequeMemory} buffer. Safe to call multiple times; subsequent calls will attempt
         * to close already-closed resources.</p>
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        /**
         * Appends a textual plan representation of this window function to the provided PlanSink.
         * <p>
         * The representation has the form:
         * "{functionName}({arg}) over (partition by {partitionKeys} rows between {lower} preceding and {upper})"
         * where {lower} is either the numeric lower bound or "unbounded", and {upper} is either
         * "current row" or a numeric preceding bound computed from the frame size.
         */
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

        /**
         * Reset the function's transient state to the top of a new execution.
         *
         * <p>Performs superclass reset then truncates internal memory buffers used for
         * storing rows and the optional monotonic deque so both are empty and ready
         * for reuse within a new plan/scan. This does not close or free the backing
         * memory, only clears stored contents.</p>
         */
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
    public static class MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final LongComparator comparator;
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
        private long maxMin;
        private long size;
        private long startOffset;

        /**
         * Create a window function that computes the maximum long value over a RANGE frame.
         *
         * <p>Initializes in-memory structures and state needed to maintain a sliding RANGE window
         * of timestamps and values. The constructor:
         * - derives the initial buffer capacity from {@code configuration},
         * - records whether the lower frame bound is bounded,
         * - computes absolute distances used to evaluate frame membership,
         * - reserves space in the provided {@code memory} buffer for the ring of (timestamp,value)
         * records and, when the lower bound is bounded, reserves a parallel deque buffer in
         * {@code dequeMemory} for maintaining candidate maxima in monotonic order,
         * - initializes bookkeeping fields (start index/offset, current frame size and current max).
         * <p>
         * Note: If {@code rangeLo} is not Long.MIN_VALUE (bounded lower bound), a non-null
         * {@code dequeMemory} must be supplied so the deque structure can be allocated.
         *
         * @param rangeLo       lower bound of the RANGE frame in timestamp units, or {@link Long#MIN_VALUE}
         *                      to indicate an unbounded (unbounded preceding) lower bound
         * @param rangeHi       upper bound of the RANGE frame in timestamp units (absolute distance
         *                      used relative to the current row's timestamp)
         * @param arg           input value expression whose long values are aggregated
         * @param configuration used to determine initial buffer sizing (page-size)
         * @param memory        memory area used to store the ring buffer of records (timestamps + values)
         * @param dequeMemory   memory area used to store the deque when the lower bound is bounded;
         *                      may be null if {@code rangeLo} is {@link Long#MIN_VALUE}
         * @param timestampIdx  index of the designated timestamp column used for RANGE comparisons
         * @param comparator    comparator used to select the max between two long values
         * @param name          function instance name (used for diagnostics / plan text)
         */
        public MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                LongComparator comparator,
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

        /**
         * Releases resources held by this window function.
         *
         * <p>Delegates to {@code super.close()}, closes the primary memory buffer and, if present,
         * the auxiliary deque memory buffer. After this call the function's native memory resources
         * are released and should not be accessed.</p>
         */
        @Override
        public void close() {
            super.close();
            memory.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        /**
         * Advance the sliding RANGE frame with the given record, updating internal buffers and the current maximum.
         *
         * <p>Processes the input record's timestamp and long value, expires out-of-frame entries, appends the new
         * non-null value to the ring buffer, updates the frame's deque of candidate maxima (resizing ring or deque
         * memory if needed), and refreshes the current max value (stored in {@code maxMin}).</p>
         *
         * <p>Notes:
         * - Timestamps are read from the record at {@code timestampIndex}; the value is read via {@code arg.getLong(record)}.
         * - NULL long values are represented by {@link io.questdb.std.Numbers#LONG_NULL} and are not added to the deque.
         * - This method mutates internal state: ring buffer (startOffset, capacity, firstIdx, size), deque
         * (dequeStartOffset, dequeCapacity, dequeStartIndex, dequeEndIndex), frameSize and {@code maxMin}.
         * - Ring buffer and deque may be reallocated and moved; indices are adjusted accordingly.</p>
         *
         * @param record input record whose timestamp and value are used to advance the frame
         */
        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            long l = arg.getLong(record);

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

        /**
         * Return the previously computed max value for the current row.
         * <p>
         * The provided Record parameter is ignored; this function returns the value
         * cached in the instance (set during pass1).
         *
         * @param rec ignored
         * @return the cached maximum long value (may be LONG_NULL if no non-null input was seen)
         */
        @Override
        public long getLong(Record rec) {
            return maxMin;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reinitializes internal state and allocates buffers to their initial capacities for reuse.
         * <p>
         * Resets the running maximum to NULL, clears counters and indices (start index, first index,
         * frame size, and size), and (re)allocates the primary record buffer using the configured
         * initial capacity. If a deque buffer is used, it is likewise (re)allocated to the initial
         * capacity and its start/end indices are reset.
         */
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

        /**
         * Reset internal state and release native memory resources held by this instance.
         *
         * <p>Calls {@code super.reset()}, closes the primary {@code memory} buffer and, if present,
         * the {@code dequeMemory} buffer. Safe to call multiple times; subsequent calls will attempt
         * to close already-closed resources.</p>
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        /**
         * Appends a textual representation of this window function's plan to the provided sink.
         * <p>
         * The emitted format is:
         * `functionName(arg) over (range between {lower} preceding and {upper})`
         * where `{lower}` is either the numeric `maxDiff` or the literal `unbounded`,
         * and `{upper}` is either `current row` (when `minDiff == 0`) or the numeric `minDiff + " preceding"`.
         */
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

        /**
         * Reset this function's internal state and buffers to their initial "top" (clean) state.
         * <p>
         * This reinitializes the aggregated max value, frame/structure sizes and indices, and
         * truncates and re-allocates the underlying ring and deque memories to the configured
         * initial capacities so the instance can be reused for a new plan or partition.
         */
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
    public static class MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final LongComparator comparator;
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
        private long maxMin = Numbers.LONG_NULL;

        /**
         * Constructs a rows-frame max window function backed by native memory buffers.
         * <p>
         * The constructor configures internal ring buffer and optional monotonic deque sizes from the
         * rows-based frame bounds and wires provided memory regions and comparator. For frames with a
         * bounded lower bound a deque buffer is used; for unbounded lower bounds no deque is allocated.
         *
         * @param arg         input function producing long values for each row
         * @param rowsLo      lower bound of the ROWS frame (negative values indicate preceding;
         *                    Long.MIN_VALUE denotes unbounded preceding)
         * @param rowsHi      upper bound of the ROWS frame (0 typically indicates inclusion of the current row)
         * @param memory      native memory region used as the circular buffer for timestamps/values
         * @param dequeMemory native memory region used for the monotonic deque (only used when the lower bound is bounded)
         * @param comparator  comparator used to determine the window aggregate (e.g., GREATER_THAN for max)
         * @param name        name used for identification/plan output
         */
        public MaxMinOverRowsFrameFunction(Function arg,
                                           long rowsLo,
                                           long rowsHi,
                                           MemoryARW memory,
                                           MemoryARW dequeMemory,
                                           LongComparator comparator,
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

        /**
         * Release resources held by this instance.
         * <p>
         * Calls super.close(), closes the primary buffer, and closes the optional deque memory if it was allocated.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
        }

        /**
         * Advance the sliding-frame state by incorporating the given record.
         * <p>
         * Updates internal circular buffer, optional monotonic deque, and the current window maximum (maxMin)
         * according to the configured frame semantics (bounded or unbounded lower bound and whether the
         * frame includes the current row). NULL long values (Numbers.LONG_NULL) are ignored for aggregation;
         * non-NULL values are compared using the configured comparator.
         * <p>
         * Side effects:
         * - mutates the circular buffer storing recent row values,
         * - updates dequeStartIndex/dequeEndIndex and dequeMemory when a bounded lower frame is used,
         * - updates the running maxMin,
         * - advances loIdx.
         *
         * @param record the input record whose long value (arg.getLong(record)) is incorporated into the window
         */
        @Override
        public void computeNext(Record record) {
            long l = arg.getLong(record);

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

        /**
         * Return the previously computed max value for the current row.
         * <p>
         * The provided Record parameter is ignored; this function returns the value
         * cached in the instance (set during pass1).
         *
         * @param rec ignored
         * @return the cached maximum long value (may be LONG_NULL if no non-null input was seen)
         */
        @Override
        public long getLong(Record rec) {
            return maxMin;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass processing for the window function: computes the next value for the current
         * input record and writes the current maximum value (as a long) to the output column
         * slot addressed by {@code spi.getAddress(recordOffset, columnIndex)}.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reinitializes internal state so the function can be reused without reallocating.
         * <p>
         * Resets the cached max value to NULL, resets the low index pointer, reinitializes
         * the underlying circular buffer, and clears the monotonic deque indices if a
         * deque is in use.
         */
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

        /**
         * Reset the function's runtime state to its initial, unused condition.
         *
         * <p>Performs a superclass reset, closes and releases the backing buffer and optional deque memory,
         * and clears all internal indices and the cached maximum value so the instance can be reused.</p>
         */
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

        /**
         * Appends a textual plan fragment for this window function to the provided PlanSink.
         * <p>
         * The produced text has the form `max(arg) over ( rows between {lower} preceding and {upper} )`,
         * where `{lower}` is either the numeric lower bound (bufferSize) or `unbounded` when
         * `frameLoBounded` is false, and `{upper}` is `current row` when `frameIncludesCurrentValue`
         * is true or `N preceding` where `N = bufferSize - frameSize` otherwise.
         */
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

        /**
         * Reset internal state to the top (start) so the function can be reused.
         * <p>
         * Clears the currently tracked maximum, resets index counters, reinitializes
         * the circular buffer, and resets deque pointers when present.
         */
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

        /**
         * Initialize the memory buffer by writing the sentinel LONG_NULL value into each slot.
         * <p>
         * Each slot is a 64-bit long at offset i * Long.BYTES for i in [0, bufferSize).
         * This prepares the ring buffer/memory region so subsequent logic can treat empty slots as NULL.
         */
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
    static class MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private final LongComparator comparator;
        private final String name;
        private long maxMin;

        /**
         * Create a function that computes the maximum long value for an unbounded-rows frame within a partition.
         *
         * <p>This constructor initializes a per-partition unbounded-rows window function using the provided
         * partition map and expression that produces the values to aggregate.</p>
         *
         * @param arg        function that produces the long value evaluated for each row
         * @param comparator comparator used to compare two long values when determining the maximum
         * @param name       function name used in plan/output (e.g., "max")
         */
        public MaxMinOverUnboundedPartitionRowsFrameFunction(Map map,
                                                             VirtualRecord partitionByRecord,
                                                             RecordSink partitionBySink,
                                                             Function arg,
                                                             LongComparator comparator,
                                                             String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
        }

        /**
         * Advance aggregation for the current record: update and store the per-partition maximum.
         * <p>
         * If the input value is non-null, this method inserts or updates the partition map entry
         * with the greater of the existing stored value and the current value, and sets the
         * instance field `maxMin` to the partition's current maximum. If the input value is null,
         * it loads the existing partition maximum into `maxMin` (or LONG_NULL if none).
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            long l = arg.getLong(record);

            if (l != Numbers.LONG_NULL) {
                MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putLong(0, l);
                    this.maxMin = l;
                } else {
                    long max = value.getLong(0);
                    if (comparator.compare(l, max)) {
                        value.putLong(0, l);
                        max = l;
                    }
                    this.maxMin = max;
                }
            } else {
                MapValue value = key.findValue();
                this.maxMin = value != null ? value.getLong(0) : Numbers.LONG_NULL;
            }
        }

        /**
         * Return the previously computed max value for the current row.
         * <p>
         * The provided Record parameter is ignored; this function returns the value
         * cached in the instance (set during pass1).
         *
         * @param rec ignored
         * @return the cached maximum long value (may be LONG_NULL if no non-null input was seen)
         */
        @Override
        public long getLong(Record rec) {
            return maxMin;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass processing for the window function: computes the next value for the current
         * input record and writes the current maximum value (as a long) to the output column
         * slot addressed by {@code spi.getAddress(recordOffset, columnIndex)}.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Appends this window function's plan representation to the provided PlanSink.
         * <p>
         * The output format is:
         * "max(arg) over (partition by {partition-functions} rows between unbounded preceding and current row)"
         */
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
    public static class MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowLongFunction {

        private final LongComparator comparator;
        private final String name;
        private long maxMin = Numbers.LONG_NULL;

        /**
         * Create a MaxMinOverUnboundedRowsFrameFunction that computes a running extreme (max/min)
         * over an unbounded-preceding ROWS frame.
         *
         * @param arg        function that produces the long value for each row
         * @param comparator comparator used to compare two long values (defines max vs min behavior)
         * @param name       output column name used by this function instance
         */
        public MaxMinOverUnboundedRowsFrameFunction(Function arg, LongComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        /**
         * Processes the given record and updates the running maximum value.
         * <p>
         * Reads a long value from {@code arg} for the provided {@code record}. If the value is
         * not null and is greater (according to {@code comparator}) than the current
         * stored value, updates the internal {@code maxMin} to that value.
         *
         * @param record the record to read the value from
         */
        @Override
        public void computeNext(Record record) {
            long l = arg.getLong(record);
            if (l != Numbers.LONG_NULL && (maxMin == Numbers.LONG_NULL || comparator.compare(l, maxMin))) {
                maxMin = l;
            }
        }

        /**
         * Return the previously computed max value for the current row.
         * <p>
         * The provided Record parameter is ignored; this function returns the value
         * cached in the instance (set during pass1).
         *
         * @param rec ignored
         * @return the cached maximum long value (may be LONG_NULL if no non-null input was seen)
         */
        @Override
        public long getLong(Record rec) {
            return maxMin;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass processing for the window function: computes the next value for the current
         * input record and writes the current maximum value (as a long) to the output column
         * slot addressed by {@code spi.getAddress(recordOffset, columnIndex)}.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reset the function's internal state between uses.
         * <p>
         * Calls the superclass reset implementation and clears the current maximum
         * by setting {@code maxMin} to {@link Numbers#LONG_NULL}, so subsequent
         * passes start with no remembered value.
         */
        @Override
        public void reset() {
            super.reset();
            maxMin = Numbers.LONG_NULL;
        }

        /**
         * Appends this window function's plan representation to the provided sink.
         * <p>
         * The produced plan looks like: `max(arg) over (rows between unbounded preceding and current row)`.
         *
         * @param sink destination for the textual plan output
         */
        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        /**
         * Reset the function state for reuse at the top of a new processing cycle.
         *
         * <p>Calls {@code super.toTop()} and clears the running maximum by setting
         * {@code maxMin} to {@link io.questdb.std.Numbers#LONG_NULL}.</p>
         */
        @Override
        public void toTop() {
            super.toTop();
            maxMin = Numbers.LONG_NULL;
        }
    }

    // max() over () - empty clause, no partition by no order by, no frame == default frame
    static class MaxMinOverWholeResultSetFunction extends BaseWindowFunction implements WindowLongFunction {

        private final LongComparator comparator;
        private final String name;
        private long maxMin = Numbers.LONG_NULL;

        /**
         * Create a window function that computes an aggregate (max/min) for the whole result set.
         *
         * @param arg        the input function that produces long values to aggregate
         * @param comparator comparison used to decide the winning value (e.g. {@code GREATER_THAN} for max)
         * @param name       human-readable function name returned by getName()/toPlan()
         */
        public MaxMinOverWholeResultSetFunction(Function arg, LongComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        /**
         * Returns the function instance name used for identification in plans and logs.
         *
         * @return the instance name
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Indicates the function requires two processing passes.
         * <p>
         * The first pass aggregates values (e.g., computes a partition or global maximum)
         * and the second pass emits results for each row.
         *
         * @return WindowFunction.TWO_PASS
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        /**
         * First pass processing: update the running maximum with the current row's long value.
         * <p>
         * Examines the argument value from the provided record; if it is not LONG_NULL and
         * compares greater (per the configured comparator) than the stored `maxMin` (or if
         * `maxMin` is LONG_NULL), replaces `maxMin` with this value.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long l = arg.getLong(record);
            if (l != Numbers.LONG_NULL && (maxMin == Numbers.LONG_NULL || comparator.compare(l, maxMin))) {
                maxMin = l;
            }
        }

        /**
         * Write the current aggregated maximum value into the function's output column for the given row.
         * <p>
         * This implementation stores the precomputed `maxMin` long value at the memory address obtained
         * from {@code spi.getAddress(recordOffset, columnIndex)}.
         *
         * @param record       input record (not used by this implementation)
         * @param recordOffset byte offset identifying the target row in the SPI output memory
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reset the function's internal state between uses.
         * <p>
         * Calls the superclass reset implementation and clears the current maximum
         * by setting {@code maxMin} to {@link Numbers#LONG_NULL}, so subsequent
         * passes start with no remembered value.
         */
        @Override
        public void reset() {
            super.reset();
            maxMin = Numbers.LONG_NULL;
        }

        /**
         * Reset the function state for reuse at the top of a new processing cycle.
         *
         * <p>Calls {@code super.toTop()} and clears the running maximum by setting
         * {@code maxMin} to {@link io.questdb.std.Numbers#LONG_NULL}.</p>
         */
        @Override
        public void toTop() {
            super.toTop();
            maxMin = Numbers.LONG_NULL;
        }
    }

    static {
        MAX_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_COLUMN_TYPES.add(ColumnType.LONG); // max value

        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // frame size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory size
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory capacity
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // memory firstIdx
        MAX_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // max value case when unbounded preceding

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
        MAX_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // max value case when unbounded preceding

        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES = new ArrayColumnTypes();
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startOffset
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory startIndex
        MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES.add(ColumnType.LONG); // deque memory endIndex
    }
}