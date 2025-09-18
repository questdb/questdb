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
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class MaxTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final TimestampComparator GREATER_THAN = (a, b) -> a > b;
    public static final ArrayColumnTypes MAX_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES;
    public static final ArrayColumnTypes MAX_OVER_PARTITION_ROWS_COLUMN_TYPES;
    public static final String NAME = "max";
    private static final String SIGNATURE = NAME + "(N)";

    /**
     * Returns the function signature used for registration and lookups.
     *
     * @return the signature string (e.g. "max(N)")
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Create a MAX timestamp window function instance appropriate for the current
     * window context.
     * <p>
     * The factory inspects the SqlExecutionContext's WindowContext (framing mode,
     * partitioning, ordering, and frame bounds) and returns a concrete Function
     * implementation optimized for that scenario (e.g. whole-result-set, partitioned
     * two-pass, unbounded/rows/range frames, or memory-backed ring-buffer variants).
     *
     * @param position            parsing position used to report errors
     * @param args                function arguments (first argument is the timestamp expression)
     * @param argPositions        argument positions in the SQL text
     * @param configuration       runtime configuration (used for memory/page sizing) — omitted from @param per project conventions
     * @param sqlExecutionContext execution context providing WindowContext and other runtime services — omitted from @param per project conventions
     * @return a Function that computes the MAX over the requested window
     * @throws SqlException if window validation fails, if RANGE is requested for queries not ordered by the designated timestamp,
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
            return new TimestampNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowColumn.FRAMING_RANGE,
                    partitionByRecord,
                    Numbers.LONG_NULL);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowColumn.FRAMING_RANGE) {
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
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
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
            if (framingMode == WindowColumn.FRAMING_RANGE) {
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
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
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

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class MaxMinOverCurrentRowFunction extends BaseWindowFunction implements WindowTimestampFunction {

        private final String name;
        private long value;

        /**
         * Create a MAX-over-current-row window function.
         *
         * @param arg  function that produces the timestamp value for the current row
         * @param name display name used by the function (e.g., "max")
         */
        MaxMinOverCurrentRowFunction(Function arg, String name) {
            super(arg);
            this.name = name;
        }

        /**
         * Read the timestamp value for the current input record and store it as this function's current value.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getTimestamp(record);
        }

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return ZERO_PASS indicating the function produces results without additional passes
         */
        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        /**
         * Return the last-computed timestamp value for the current row.
         *
         * @param rec ignored; this implementation does not read from the provided record
         * @return the stored timestamp value produced by the most recent computeNext call
         */
        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Compute the next value for the current input record and write it to the window output slot.
         * <p>
         * This evaluates the function for the given input Record, then stores the resulting timestamp
         * (primitive long) into the output memory location identified by recordOffset and the
         * function's configured output column.
         *
         * @param record       the input record to evaluate
         * @param recordOffset the row-address/offset where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles max() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class MaxMinOverPartitionFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        private final TimestampComparator comparator;
        private final String name;

        /**
         * Creates a partitioned max/min window function that maintains per-partition aggregates.
         *
         * <p>Constructs a function which uses the provided map to store per-partition state and
         * the provided comparator to decide the aggregate (e.g., max vs min) when processing rows.
         *
         * @param map        a mutable Map used to store and retrieve per-partition aggregation state
         * @param comparator comparator used to compare two timestamp values to determine the current aggregate (returns true when the first argument should replace the second)
         * @param name       human-readable name for this function variant (used for planning/diagnostics)
         */
        public MaxMinOverPartitionFunction(Map map,
                                           VirtualRecord partitionByRecord,
                                           RecordSink partitionBySink,
                                           Function arg,
                                           TimestampComparator comparator,
                                           String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.name = name;
            this.comparator = comparator;
        }

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Indicates this window function requires two processing passes.
         *
         * @return {@link WindowFunction#TWO_PASS} signifying the function performs work in two passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * First-pass accumulator that updates the per-partition maximum timestamp.
         * <p>
         * For the given input record, reads the timestamp from the argument function and, if not null,
         * looks up the partition key in the map and updates the stored timestamp to the greater value
         * according to the provided comparator. New partition entries are initialized with the record's
         * timestamp. Null timestamps are ignored.
         *
         * @param record       input record to read the timestamp and partition key from
         * @param recordOffset unused here (present for API compatibility)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long l = arg.getTimestamp(record);
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

        /**
         * Second pass: materializes the precomputed per-partition maximum timestamp into the result.
         * <p>
         * Looks up the partition key for the current input record, reads the stored max timestamp
         * from the map (or uses SQL NULL when absent), and writes that long value to the output
         * address obtained from the WindowSPI.
         *
         * @param record       current input record used to derive the partition key
         * @param recordOffset offset passed to WindowSPI to obtain the output address where the timestamp is written
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            long val = value != null ? value.getTimestamp(0) : Numbers.LONG_NULL;

            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles max() over (partition by x order by ts range between {unbounded | y} preceding and {z preceding | current row})
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When the lower bound is unbounded, we only need to keep one maximum value in history.
    // However, when the lower bound is not unbounded, we need a monotonically deque to maintain the history of records.
    public static class MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        private static final int DEQUE_RECORD_SIZE = Long.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final TimestampComparator comparator;
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
         * Builds a window function that computes the maximum timestamp over a RANGE frame scoped per partition.
         * <p>
         * This constructor configures whether the lower bound is bounded, whether the frame includes the current row,
         * and stores the memory buffers and comparator used to maintain a per-partition ring buffer and optional deque
         * of candidate maxima.
         *
         * @param map               per-partition state map used to store frame metadata and offsets
         * @param partitionByRecord a virtual record representing the partition-by values
         * @param partitionBySink   sink used to serialize partition-by values into map keys
         * @param rangeLo           lower bound of the RANGE frame (Long.MIN_VALUE indicates unboundedpreceding)
         * @param rangeHi           upper bound of the RANGE frame (0 typically indicates "to current row")
         * @param arg               argument function that produces the input timestamps
         * @param memory            primary MemoryARW used as the per-partition ring buffer for frame rows
         * @param dequeMemory       optional MemoryARW used as a monotonic deque for candidate maxima (may be null for unbounded lower bound)
         * @param initialBufferSize initial capacity for per-partition buffers
         * @param timestampIdx      column index within the frame entries that holds the timestamp value
         * @param comparator        comparator used to choose the maximum timestamp (non-boxing)
         * @param name              function name used in plan/output
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

        /**
         * Release resources used by this window function instance.
         * <p>
         * Closes underlying memory buffers and the optional deque memory, clears internal free lists,
         * and invokes {@code super.close()} to perform superclass cleanup.
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
         * Updates per-partition frame state with the next input record and computes the current
         * partitioned maximum timestamp/value (stored in this.maxMin).
         *
         * <p>This method:
         * - Advances or initializes the ring-buffer frame and optional monotonic deque for the
         * partition identified by the record's partition key.
         * - Appends the incoming (timestamp, value) pair when the value is not null.
         * - Evicts elements that fall outside the configured range/rows frame.
         * - Maintains and expands underlying MemoryARW buffers when capacity is exceeded.
         * - Updates the partition map value with the new frame metadata (frameSize, buffer offsets,
         * sizes, indices) and, when the lower bound is unbounded, stores the current max directly
         * in the map value.
         * <p>
         * The behavior differs based on whether the partition entry is new and whether the frame's
         * lower bound is bounded:
         * - New partition: allocates initial buffers, writes the first element if present and
         * initializes deque or max accordingly.
         * - Existing partition: trims old elements outside the lower bound, appends the new element,
         * updates the deque (for bounded lower bound) or recomputes the max by scanning the frame
         * (for unbounded lower bound).
         *
         * @param record the input record whose partition key, ordering timestamp, and value are used
         *               to update per-partition window state
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
            long l = arg.getTimestamp(record);

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

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.ZERO_PASS} indicating the function produces results without multi-pass aggregation.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the current computed maximum timestamp for this window function.
         * <p>
         * The provided Record argument is not used by this implementation; the result is taken
         * from the function's internal state.
         *
         * @param rec ignored
         * @return the current max timestamp stored in this function's state
         */
        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reinitializes the function for a new execution cycle.
         * <p>
         * Calls the superclass reopen logic and resets the internal max/min accumulator
         * to a NULL sentinel so that any backing memory is allocated lazily on first use.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            maxMin = Numbers.LONG_NULL;
        }

        /**
         * Reset the function's internal state and release associated native resources.
         * <p>
         * Clears in-memory ring-buffer/free-list state so the function can be reused,
         * closes the primary memory region, clears internal free lists used for
         * element management, and closes the optional deque memory if present.
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
         * Append this window function's SQL-like plan representation to the given PlanSink.
         * <p>
         * The produced text follows the pattern:
         * "{name}({arg}) over (partition by {partition functions} range between {lower bound or 'unbounded'} preceding and {current row or '{minDiff} preceding'})".
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
         * Reset this function's internal state to the start of iteration.
         * <p>
         * Truncates the primary frame memory and optional deque memory, and clears
         * internal free lists so the function is ready to be reused from the top.
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
    public static class MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        private final TimestampComparator comparator;
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
         * Constructs a partitioned rows-frame max/min function and configures its internal ring buffer
         * and optional monotonic deque parameters according to the supplied row-frame bounds.
         * <p>
         * The constructor computes:
         * - frameSize: number of slots used to retain values inside the frame
         * - bufferSize: number of extra slots required to accommodate negative row offsets
         * - frameLoBounded: true when the frame lower bound is finite (requires a deque)
         * - dequeBufferSize: size of the deque buffer when the lower bound is bounded
         * - frameIncludesCurrentValue: true when the upper bound includes the current row (rowsHi == 0)
         * <p>
         * It then stores provided memory buffers, comparator and name for use during runtime.
         *
         * @param rowsLo      lower row bound for the frame (may be Long.MIN_VALUE to denote unbounded preceding)
         * @param rowsHi      upper row bound for the frame (relative to the current row)
         * @param arg         value-producing function for each row (timestamp source)
         * @param memory      ring-buffer memory used to store per-partition frame entries
         * @param dequeMemory optional memory for the monotonic deque (may be null when lower bound is unbounded)
         * @param comparator  comparator used to decide max/min between two timestamps
         * @param name        function instance name used for plan/output identification
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

        /**
         * Releases resources held by this window function.
         * <p>
         * Calls the superclass close implementation and closes the associated MemoryARW used for frame storage.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
        }

        /**
         * Advances the sliding-window state for the current input record and computes the windowed maximum timestamp.
         *
         * <p>This updates per-partition storage in the factory's backing map and memory buffers:
         * - When encountering a new partition key, it initializes the ring buffer and optional deque and sets the initial
         * max according to whether the frame includes the current value.
         * - For existing partitions it pushes the new timestamp into the ring buffer, updates or maintains the monotonic
         * deque when the lower bound is bounded, evicts the oldest value from the deque when necessary, and updates
         * the stored per-partition max for unbounded-lower-bound frames.
         *
         * <p>Behavior notes:
         * - Null timestamps (Numbers.LONG_NULL) are kept in the ring buffer but ignored for max computation.
         * - The method mutates: the partition map value, off-heap memory for the ring buffer (memory), optional dequeMemory,
         * and the instance field {@code this.maxMin} which receives the computed max for the current record.
         *
         * @param record the input record whose timestamp is pushed into the window and used to update per-partition state
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
            long l = arg.getTimestamp(record);

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

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.ZERO_PASS} indicating the function produces results without multi-pass aggregation.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the current computed maximum timestamp for this window function.
         * <p>
         * The provided Record argument is not used by this implementation; the result is taken
         * from the function's internal state.
         *
         * @param rec ignored
         * @return the current max timestamp stored in this function's state
         */
        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Compute the aggregate for the current input row and write the result to the output column.
         * <p>
         * Calls computeNext(record) to update the internal max/min state, then writes that
         * timestamp value into the output row at the address returned by spi.getAddress(recordOffset, columnIndex).
         *
         * @param record       current input record to process
         * @param recordOffset byte offset identifying the output row frame where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reopens and reinitializes this window function for a new execution.
         * <p>
         * Delegates to the superclass implementation to reset any internal state.
         */
        @Override
        public void reopen() {
            super.reopen();
        }

        /**
         * Reset the function state and release any allocated off-heap resources.
         * <p>
         * Calls the superclass reset behavior, closes the primary frame memory, and closes
         * the optional deque memory if it was allocated.
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
         * Appends a SQL-style plan fragment describing this window function to the provided PlanSink.
         * <p>
         * The produced fragment has the form:
         * "{name}({arg}) over (partition by {partitionFunctions} rows between {lower} preceding and {upper})"
         * where {lower} is either a numeric buffer size or "unbounded", and {upper} is either "current row"
         * or a numeric "N preceding" computed as `bufferSize - frameSize`.
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
         * Reset this function to its initial state for reuse.
         * <p>
         * Performs superclass reset behavior and releases any accumulated frame and deque
         * storage by truncating the associated MemoryARW buffers.
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
    public static class MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowTimestampFunction {
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final TimestampComparator comparator;
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
         * Constructs a range-frame max window function that maintains a sliding frame of timestamped
         * records in off-heap memory and an optional monotonic deque for efficient max computation.
         * <p>
         * The instance retains references to the provided memory regions and computes initial
         * buffer capacities from the SQL window store page size. If the lower range bound is
         * finite (`rangeLo != Long.MIN_VALUE`), a deque buffer is allocated using `dequeMemory`.
         * In that case `maxDiff` is set to `abs(rangeLo)` and must only be used when
         * `frameLoBounded` is true; otherwise `maxDiff` is Long.MAX_VALUE.
         * <p>
         * Parameters:
         * - rangeLo, rangeHi: inclusive offsets (relative to the row timestamp) that define the
         * window frame. `rangeLo == Long.MIN_VALUE` indicates an unbounded preceding lower bound.
         * - arg: the argument function that produces timestamps for each input row.
         * - configuration: provides SQL runtime configuration (used to derive the initial buffer size).
         * - memory: primary ring-buffer memory region for storing timestamp/value records.
         * - dequeMemory: optional memory region used to store deque indices when the lower bound is bounded.
         * - timestampIdx: index of the timestamp column within stored records.
         * - comparator: non-boxing comparator used to decide the max between two timestamps.
         * - name: function name for plan/debug output.
         */
        public MaxMinOverRangeFrameFunction(
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

        /**
         * Releases resources held by this function, including backing memory buffers.
         * <p>
         * Performs superclass cleanup, closes the primary frame memory, and closes
         * the optional deque memory if present.
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
         * Advance the sliding window state for the given input record and update the current maximum.
         *
         * <p>This method reads the record's timestamp and the argument function's timestamp value,
         * then updates in-memory ring buffers and an optional monotonic deque to maintain the max
         * value over the configured range/rows frame.</p>
         *
         * <p>Behavior summary:
         * - If the frame has a bounded lower bound, evict elements that fall outside the lower
         * bound, append the current element (if not null), and extend the top of the frame by
         * scanning forward, pushing qualifying values into the monotonic deque. The deque holds
         * candidate maxima; the current max is the deque head or `Numbers.LONG_NULL` when empty.
         * - If the frame lower bound is unbounded, append the current element (if not null) and
         * recompute the max by scanning buffered elements that meet the top bound criteria.</p>
         *
         * <p>Side effects:
         * - Mutates internal buffer state: startOffset, capacity, firstIdx, size, frameSize
         * and may grow `memory` and `dequeMemory` (reallocating and copying existing contents).
         * - Mutates deque indices (dequeStartIndex, dequeEndIndex) and writes deque entries.
         * - Updates `maxMin` to the current maximum or `Numbers.LONG_NULL` if no candidates exist.</p>
         *
         * <p>Notes:
         * - Null argument timestamps (equal to `Numbers.LONG_NULL`) are not added to the buffers.
         * - Uses the configured `comparator` to compare candidate values for deque maintenance.</p>
         */
        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            long l = arg.getTimestamp(record);

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
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.ZERO_PASS} indicating the function produces results without multi-pass aggregation.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the current computed maximum timestamp for this window function.
         * <p>
         * The provided Record argument is not used by this implementation; the result is taken
         * from the function's internal state.
         *
         * @param rec ignored
         * @return the current max timestamp stored in this function's state
         */
        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reset and initialize internal buffers and indices for a new execution/reopen.
         * <p>
         * Reinitializes the running maximum to null, resets ring buffer and deque sizes and indices,
         * and (re)allocates contiguous memory regions for the frame and optional deque using the
         * configured initial capacities. If `dequeMemory` is null, deque-related fields are left unset.
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
         * Reset the function state and release any allocated off-heap resources.
         * <p>
         * Calls the superclass reset behavior, closes the primary frame memory, and closes
         * the optional deque memory if it was allocated.
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
         * Writes the SQL plan fragment for this window function into the supplied PlanSink.
         * <p>
         * The emitted fragment has the form:
         * "{name}({arg}) over (range between {lower} preceding and {upper})", where
         * "{lower}" is either the numeric lower bound or "unbounded" when the lower
         * bound is not set, and "{upper}" is either "current row" for a zero upper
         * bound or the numeric upper bound followed by "preceding".
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
         * Reset this function's internal state and buffers to their initial empty configuration.
         *
         * <p>This clears the current maximum, resets capacities and indices to their initial values,
         * truncates and reallocates the main ring-buffer memory for the initial capacity, and
         * zeros the frame size and element count. If a deque buffer is present, it is also
         * truncated and reinitialized to the initial deque capacity and indices.</p>
         *
         * <p>Intended to prepare the function for a fresh execution pass without reallocating
         * the factory/struct that owns this instance.</p>
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
    public static class MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowTimestampFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final TimestampComparator comparator;
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
         * Create a max-over-rows-frame window function instance that maintains a ring buffer
         * (and an optional monotonic deque) to compute the maximum timestamp over a rows-based frame.
         *
         * <p>The constructor configures internal sizes and backing memory:
         * - Asserts that the frame is not the unbounded-previous-to-current special case (Long.MIN_VALUE, 0).
         * - If the lower bound is bounded (rowsLo > Long.MIN_VALUE) the instance keeps a bounded
         * sliding window: frameSize = rowsHi - rowsLo (+1 when rowsHi &lt; 0) and bufferSize = |rowsLo|.
         * - If the lower bound is unbounded, frameSize = |rowsHi| and bufferSize = frameSize.
         * - frameIncludesCurrentValue is set when rowsHi == 0.
         * - The provided MemoryARW `memory` is used as the ring buffer; when the lower bound is bounded
         * the provided `dequeMemory` is used for the monotonic deque and dequeBufferSize is computed.
         * <p>
         * The constructor initializes the buffers via initBuffer(); if buffer initialization throws,
         * resources are closed and the throwable is rethrown.
         *
         * @param arg         function producing the timestamp values to aggregate
         * @param rowsLo      lower bound of the rows frame (may be Long.MIN_VALUE for unbounded)
         * @param rowsHi      upper bound of the rows frame (relative to current row)
         * @param memory      pre-allocated memory to use for the ring buffer
         * @param dequeMemory pre-allocated memory to use for the deque; only used when lower bound is bounded
         * @param comparator  comparator used to decide the max between two timestamps
         * @param name        name used for function identification (e.g., "max")
         */
        public MaxMinOverRowsFrameFunction(Function arg,
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

        /**
         * Releases resources held by this function.
         * <p>
         * Closes the underlying buffer and, if present, the deque memory, and then invokes
         * superclass cleanup. Safe to call multiple times; null deque memory is ignored.
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
         * Advance the sliding window by one row: ingest the timestamp from the provided record,
         * update the circular frame buffer and (when configured) the monotonic deque used to
         * maintain the current maximum, evict old elements, and update the cached max value.
         *
         * <p>Behavior details:</p>
         * <ul>
         *   <li>Reads the timestamp via the wrapped argument function (arg.getTimestamp(record)).</li>
         *   <li>If the frame lower bound is bounded, maintains a monotonic deque in dequeMemory:
         *       new values are pushed while removing smaller elements according to the comparator;
         *       the deque front represents the current frame maximum.</li>
         *   <li>If the frame lower bound is unbounded, maintains a single accumulator (maxMin)
         *       that is updated when encountering larger timestamps.</li>
         *   <li>Handles frames that either include or exclude the current row when computing the
         *       top/frame element used for comparison.</li>
         *   <li>Ignores null timestamps (Numbers.LONG_NULL) when updating maxima or deque.</li>
         *   <li>Evicts the oldest element from the deque when it leaves the frame.</li>
         *   <li>Overwrites the oldest slot in the circular buffer with the new timestamp and
         *       advances loIdx.</li>
         * </ul>
         *
         * @param record the input record whose timestamp will be added to the window
         */
        @Override
        public void computeNext(Record record) {
            long l = arg.getTimestamp(record);

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
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.ZERO_PASS} indicating the function produces results without multi-pass aggregation.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the current computed maximum timestamp for this window function.
         * <p>
         * The provided Record argument is not used by this implementation; the result is taken
         * from the function's internal state.
         *
         * @param rec ignored
         * @return the current max timestamp stored in this function's state
         */
        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Compute the aggregate for the current input row and write the result to the output column.
         * <p>
         * Calls computeNext(record) to update the internal max/min state, then writes that
         * timestamp value into the output row at the address returned by spi.getAddress(recordOffset, columnIndex).
         *
         * @param record       current input record to process
         * @param recordOffset byte offset identifying the output row frame where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reset the function's internal state for a new execution pass.
         * <p>
         * Clears the current max/min accumulator, resets the frame start index, reinitializes the ring
         * buffer, and, if a deque buffer is present, resets deque start/end indices.
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
         * Reset internal state for reuse: close and clear the ring buffer and optional deque,
         * and reset indices and cached maximum timestamp to null sentinel.
         *
         * <p>This clears resources acquired by the function so it can be reopened or discarded.
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
         * Appends a human-readable plan fragment for this window function to the provided PlanSink.
         * <p>
         * The produced fragment has the form: `max(arg) over ( rows between {lower} preceding and {upper} )`,
         * where `{lower}` is either the numeric lower bound or "unbounded", and `{upper}` is either "current row"
         * or a numeric preceding offset computed from the buffer size and frame size.
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
         * Reset the function to its initial state before scanning from the start.
         * <p>
         * Calls the superclass reset, clears the current max/min sentinel, resets the frame index
         * and deque indices, and reinitializes the internal buffer(s) so the function can be reused
         * for a fresh pass over data.
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
         * Initialize the ring buffer by setting each slot to the sentinel null timestamp.
         * <p>
         * Writes Numbers.LONG_NULL at consecutive long-sized offsets for bufferSize entries
         * starting at offset 0.
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
    static class MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        private final TimestampComparator comparator;
        private final String name;
        private long maxMin;

        /**
         * Create a window function that computes the maximum timestamp for each partition with an unbounded
         * preceding rows frame (i.e., accumulates a running max per partition).
         *
         * @param map               map holding per-partition state (keys -> values)
         * @param partitionByRecord record wrapper used to read the partition key from the current row
         * @param partitionBySink   sink used to write partition key values into map keys
         * @param arg               function that produces the timestamp value for the current row
         * @param comparator        comparator used to decide which timestamp is greater
         * @param name              function name used in plan/output
         */
        public MaxMinOverUnboundedPartitionRowsFrameFunction(Map map,
                                                             VirtualRecord partitionByRecord,
                                                             RecordSink partitionBySink,
                                                             Function arg,
                                                             TimestampComparator comparator,
                                                             String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
        }

        /**
         * Process the given record and update the per-partition maximum timestamp.
         * <p>
         * Advances the partition key using the supplied record, reads the timestamp from
         * the argument function, and updates the partition's stored maximum if the
         * value is non-null and compares greater according to {@code comparator}.
         * Sets {@code this.maxMin} to the partition's current maximum timestamp after
         * processing the record, or {@link io.questdb.std.Numbers#LONG_NULL} if no value
         * exists for the partition.
         *
         * @param record current row being processed; used to derive the partition key
         *               and the timestamp to consider for the partition's maximum
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            long l = arg.getTimestamp(record);

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

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.ZERO_PASS} indicating the function produces results without multi-pass aggregation.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the current computed maximum timestamp for this window function.
         * <p>
         * The provided Record argument is not used by this implementation; the result is taken
         * from the function's internal state.
         *
         * @param rec ignored
         * @return the current max timestamp stored in this function's state
         */
        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Compute the aggregate for the current input row and write the result to the output column.
         * <p>
         * Calls computeNext(record) to update the internal max/min state, then writes that
         * timestamp value into the output row at the address returned by spi.getAddress(recordOffset, columnIndex).
         *
         * @param record       current input record to process
         * @param recordOffset byte offset identifying the output row frame where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Append a SQL-style plan representation of this window function to the provided sink.
         * <p>
         * The emitted text has the form:
         * `name(arg) over (partition by {partition expressions} rows between unbounded preceding and current row)`
         * and reflects that this function is applied with a partition clause and an unbounded-preceding-to-current-row rows frame.
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
    public static class MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowTimestampFunction {
        private final TimestampComparator comparator;
        private final String name;
        private long maxMin = Numbers.LONG_NULL;

        /**
         * Create a MAX-over-unbounded-rows window function instance.
         * <p>
         * This constructor builds a function that maintains the maximum timestamp seen so far
         * across all rows (no partitioning, unbounded preceding). The provided `arg` supplies
         * the timestamp value for each row; `comparator` is used to compare timestamps; `name`
         * is the function name returned by getName().
         *
         * @param arg        the input function that produces a timestamp value for each row
         * @param comparator comparator used to decide which timestamp is greater
         * @param name       the reported function name (e.g., "max")
         */
        public MaxMinOverUnboundedRowsFrameFunction(Function arg, TimestampComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        /**
         * Consume the next input record and update the running maximum timestamp.
         * <p>
         * Reads the timestamp from {@code arg} for the given {@code record}; if the value is not null
         * and is greater (per {@code comparator}) than the current stored value, replaces the stored
         * maximum with that timestamp.
         *
         * @param record input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            long l = arg.getTimestamp(record);
            if (l != Numbers.LONG_NULL && (maxMin == Numbers.LONG_NULL || comparator.compare(l, maxMin))) {
                maxMin = l;
            }
        }

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.ZERO_PASS} indicating the function produces results without multi-pass aggregation.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the current computed maximum timestamp for this window function.
         * <p>
         * The provided Record argument is not used by this implementation; the result is taken
         * from the function's internal state.
         *
         * @param rec ignored
         * @return the current max timestamp stored in this function's state
         */
        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Compute the aggregate for the current input row and write the result to the output column.
         * <p>
         * Calls computeNext(record) to update the internal max/min state, then writes that
         * timestamp value into the output row at the address returned by spi.getAddress(recordOffset, columnIndex).
         *
         * @param record       current input record to process
         * @param recordOffset byte offset identifying the output row frame where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reset the function's execution state for a new evaluation.
         * <p>
         * Clears the accumulated maximum timestamp by setting {@code maxMin} to the null timestamp marker
         * so subsequent processing starts with an empty accumulator.
         */
        @Override
        public void reset() {
            super.reset();
            maxMin = Numbers.LONG_NULL;
        }

        /**
         * Appends this function's textual plan representation to the provided PlanSink.
         * <p>
         * The produced plan has the form: "{functionName}({arg}) over (rows between unbounded preceding and current row)".
         */
        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        /**
         * Reset the function to its initial state for reuse.
         * <p>
         * Clears any accumulated maximum timestamp by setting {@code maxMin} to {@code Numbers.LONG_NULL}
         * and delegates common reset logic to the superclass via {@code super.toTop()}.
         */
        @Override
        public void toTop() {
            super.toTop();
            maxMin = Numbers.LONG_NULL;
        }
    }

    // max() over () - empty clause, no partition by no order by, no frame == default frame
    static class MaxMinOverWholeResultSetFunction extends BaseWindowFunction implements WindowTimestampFunction {
        private final TimestampComparator comparator;
        private final String name;
        private long maxMin = Numbers.LONG_NULL;

        /**
         * Constructs a window function that computes the maximum timestamp across the entire result set.
         *
         * @param arg        the input timestamp-producing function
         * @param comparator comparator used to determine which timestamp is greater
         * @param name       the function name used for labeling/plan output
         */
        public MaxMinOverWholeResultSetFunction(Function arg, TimestampComparator comparator, String name) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
        }

        /**
         * Returns the window function's name.
         *
         * @return the name used to identify this function instance
         */
        @Override
        public String getName() {
            return name;
        }

        /**
         * Indicates this window function requires two processing passes.
         *
         * @return {@link WindowFunction#TWO_PASS} signifying the function performs work in two passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Processes a single input row for the first pass: reads the timestamp from the argument
         * function and updates the running maximum (`maxMin`) if the timestamp is non-null and
         * greater according to the provided comparator.
         *
         * @param record       the input record to read the timestamp from
         * @param recordOffset offset associated with the record (not used by this implementation)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long l = arg.getTimestamp(record);
            if (l != Numbers.LONG_NULL && (maxMin == Numbers.LONG_NULL || comparator.compare(l, maxMin))) {
                maxMin = l;
            }
        }

        /**
         * Writes the computed maximum timestamp for the current partition into the output column for the specified row.
         *
         * @param record       unused input record for this pass
         * @param recordOffset row identifier used by WindowSPI to locate the output slot
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        /**
         * Reset the function's execution state for a new evaluation.
         * <p>
         * Clears the accumulated maximum timestamp by setting {@code maxMin} to the null timestamp marker
         * so subsequent processing starts with an empty accumulator.
         */
        @Override
        public void reset() {
            super.reset();
            maxMin = Numbers.LONG_NULL;
        }

        /**
         * Reset the function to its initial state for reuse.
         * <p>
         * Clears any accumulated maximum timestamp by setting {@code maxMin} to {@code Numbers.LONG_NULL}
         * and delegates common reset logic to the superclass via {@code super.toTop()}.
         */
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