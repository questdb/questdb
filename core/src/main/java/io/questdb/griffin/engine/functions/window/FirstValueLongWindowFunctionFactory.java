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
public class FirstValueLongWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "first_value";
    protected static final ArrayColumnTypes FIRST_VALUE_COLUMN_TYPES;
    private static final String SIGNATURE = NAME + "(L)";

    /**
     * Returns the function signature for this factory.
     * <p>
     * The signature identifies the function name and accepted argument types (e.g. "first_value(L)").
     *
     * @return the signature string
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Create a window-function instance for `first_value(L)` based on the current WindowContext.
     * <p>
     * If the frame bounds are invalid (rowsHi &lt; rowsLo) this returns a LongNullFunction that yields
     * SQL NULL for every row in the frame. Otherwise delegates to an implementation chosen for the
     * window's null-handling mode (ignore vs. respect nulls), framing mode, partitioning and ordering.
     *
     * @param position            SQL token position used for error reporting
     * @param args                function arguments (first argument is the input long value)
     * @param argPositions        positions of the arguments in the SQL for error reporting
     * @param configuration       Cairo configuration (passed through to generated functions)
     * @param sqlExecutionContext execution context supplying the WindowContext
     * @return a Function that implements the requested first_value behavior for LONG inputs
     * @throws SqlException if the WindowContext is invalid or the requested window configuration is unsupported
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
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new LongNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE,
                    windowContext.getPartitionByRecord(),
                    Numbers.LONG_NULL
            );
        }

        return windowContext.isIgnoreNulls() ?
                this.generateIgnoreNullsFunction(position, args, configuration, windowContext) :
                this.generateRespectNullsFunction(position, args, configuration, windowContext);
    }

    /**
     * Builds a specialized first_value(long) window Function implementation that ignores NULLs,
     * selecting an optimized strategy based on the provided WindowContext (partitioning, framing,
     * ordering and frame bounds).
     *
     * <p>The factory chooses among implementations that operate per-partition or globally and
     * that use either RANGE- or ROWS-based buffering strategies (ring buffers, per-partition maps,
     * or simple current-row implementations) to compute the first non-null LONG value within the
     * active frame.</p>
     *
     * @param position      parser/statement position used for error reporting when the window
     *                      configuration is unsupported
     * @param args          function argument list; this factory uses args.get(0) as the value expression
     * @param windowContext describes partitioning, ordering, framing mode and bounds that determine
     *                      which concrete implementation is returned
     * @return a Function instance implementing first_value(long) with ignore-null semantics
     * @throws SqlException if the provided window parameters are not supported (for example,
     *                      RANGE framing without a designated timestamp or other unimplemented
     *                      parameter combinations)
     */
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
                    return new FirstValueOverCurrentRowFunction(args.get(0), true);
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

    /**
     * Create a first_value window-function implementation that respects NULLs.
     * <p>
     * Chooses and constructs an optimized Function implementation based on the provided
     * WindowContext (partitioning, framing mode FRAMING_RANGE/FRAMING_ROWS, frame bounds,
     * ordering and designated timestamp) and the supplied argument list. Returned implementations
     * include specialized partitioned and non-partitioned variants that may allocate per-partition
     * maps or native circular buffers when required by the frame semantics.
     *
     * @param position      parser/code position used for SqlException diagnostics
     * @param args          function arguments (first entry is the value expression)
     * @param windowContext window framing/partitioning/ordering information that drives implementation selection
     * @return a Function instance implementing first_value with "respect nulls" semantics for the given window
     * @throws SqlException if RANGE framing is used with non-designated-timestamp ordering or if the
     *                      specific window parameter combination is not implemented
     */
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

    /**
     * Indicates that the function implementation supports NULLs-descending semantics for window ordering.
     *
     * @return true when NULLs descent (ignore/respect nulls ordering) is supported
     */
    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    // handles first_value() ignore nulls over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class FirstNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        /**
         * Creates a function that computes the first non-null long value for each partition.
         *
         * @param map               storage used to hold per-partition state
         * @param partitionByRecord provides the current row's partition key
         * @param partitionBySink   serializes the partition key into the map
         * @param arg               expression that produces the long value evaluated for each row
         */
        public FirstNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires two evaluation passes.
         *
         * @return WindowFunction.TWO_PASS to signal a two-pass (pass1 then pass2) execution model.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First pass: record the first non-null long value seen for the current partition.
         * <p>
         * For the given row's partition key, if the partition is not yet present in the map
         * and the argument value is not NULL, stores that value as the partition's first value.
         * This method updates the factory's partition map state; it does not emit results.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                long d = arg.getLong(record);
                if (d != Numbers.LONG_NULL) {
                    MapValue value = key.createValue();
                    value.putLong(0, d);
                }
            }
        }

        /**
         * Second pass: look up the partition's first-value (long) and write it to the output slot for the current row.
         * <p>
         * Looks up the value previously stored in the per-partition map; if no entry exists writes
         * SQL NULL for LONG. The result is written into the window output memory for the given
         * recordOffset and the function's configured column index.
         *
         * @param record       current input record used to derive the partition key
         * @param recordOffset byte offset for the current row in the WindowSPI output memory
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

    // Handles first_value() ignore nulls over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class FirstNotNullValueOverPartitionRangeFrameFunction extends FirstValueOverPartitionRangeFrameFunction {
        /**
         * Creates a partitioned RANGE-frame implementation of FIRST_VALUE that skips nulls.
         * <p>
         * This constructor builds a function instance that maintains a per-partition ring buffer
         * of (timestamp, value) pairs in native memory and computes the first non-null long value
         * within the inclusive range [rangeLo, rangeHi] relative to each row's timestamp.
         *
         * @param rangeLo           lower bound of the RANGE frame (relative offset, inclusive)
         * @param rangeHi           upper bound of the RANGE frame (relative offset, inclusive)
         * @param initialBufferSize initial capacity (in entries) for the native ring buffer used to store (timestamp, value) pairs
         * @param timestampIdx      column index of the timestamp value in the input record used for RANGE comparisons
         */
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

        /**
         * Processes the next input record and updates per-partition ring-buffer state to compute
         * the first_value over a RANGE frame of long values.
         *
         * <p>This method:
         * - Locates or creates the per-partition map entry that stores ring-buffer metadata
         * (start offset, size, capacity, index of the oldest element).
         * - For a new partition entry, initializes the buffer and sets the stored first value
         * when the current value is non-null and the frame includes the current row.
         * - For existing entries, evicts elements that fall outside the range frame defined by
         * timestamp difference bounds (minDiff/maxDiff), expands the ring buffer if full,
         * and appends the current (timestamp, value) pair when the value is non-null.
         * - Maintains the instance field `firstValue` to reflect the first element within the
         * current frame (or LONG_NULL when there is none).
         * <p>
         * The method persists updated buffer metadata back into the map entry (start offset,
         * size, capacity, first index) so subsequent calls and partitions can continue
         * incremental frame processing.
         *
         * @param record the input row whose timestamp and value are used to update partition state
         */
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
                long d = arg.getLong(record);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (d != Numbers.LONG_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    size = 1;
                    if (frameIncludesCurrentValue) {
                        this.firstValue = d;
                    } else {
                        this.firstValue = Numbers.LONG_NULL;
                    }
                } else {
                    size = 0;
                    this.firstValue = Numbers.LONG_NULL;
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
                            firstValue = memory.getLong(startOffset + Long.BYTES);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue = Numbers.LONG_NULL;
                        }
                    } else {
                        // first value always in first index case when frameLoBounded == false
                        firstValue = memory.getLong(startOffset + Long.BYTES);
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
                            this.firstValue = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                long d = arg.getLong(record);
                if (d != Numbers.LONG_NULL) {
                    if (size == capacity) { //buffer full
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }

                    // add element to buffer
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                if (!findNewFirstValue) {
                    this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // handles first_value() ignore nulls over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class FirstNotNullValueOverPartitionRowsFrameFunction extends FirstValueOverPartitionRowsFrameFunction {

        /**
         * Create a partitioned ROWS-frame implementation that returns the first non-null long
         * value within the frame for each partition.
         * <p>
         * The instance maintains per-partition state in `map` and uses `memory` as a native
         * ring buffer for storing recent rows required by the moving ROWS frame.
         *
         * @param map               per-partition map used to store frame metadata and indexes
         * @param partitionByRecord record holding the current partition key
         * @param partitionBySink   sink used to write partition keys into map records
         * @param rowsLo            lower bound of the ROWS frame (offset from current row)
         * @param rowsHi            upper bound of the ROWS frame (offset from current row)
         * @param arg               source function that produces the long value to evaluate
         * @param memory            native memory buffer used to store timestamp/value entries for each partition
         */
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

        /**
         * Compute the first non-null LONG value for the given input record and update per-partition frame state.
         *
         * <p>This method:
         * - Looks up or initializes per-partition state in {@code map} and a native ring buffer in {@code memory}.
         * - Maintains buffer metadata stored in the map value:
         * 0 = index of the oldest value (lo index),
         * 1 = start offset of the native buffer,
         * 2 = cached index of the first non-null value (or -1 if none cached),
         * 3 = total count of values appended (used for unbounded lower frames).
         * - For unbounded-lower frames, appends the current value and uses the cached first-non-null index when still valid;
         * otherwise updates the cache when a new non-null arrives and sets {@code firstValue} accordingly.
         * - For bounded (rows) frames, scans the in-buffer window (of size {@code frameSize}) beginning at the lo index
         * to find the first non-null value (falling back to the current row value only if the frame contains none),
         * advances the lo index, stores the new value into the ring buffer, and updates the cached index.
         * <p>
         * Effects:
         * - Updates the per-partition map entry and the native buffer in {@code memory}.
         * - Sets the instance field {@code firstValue} to the computed result or {@code Numbers.LONG_NULL} when no value is found
         * inside the frame (and the frame does not include the current row).
         *
         * @param record the input row for which to compute and materialize the window's first non-null LONG value
         */
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
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Numbers.LONG_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = memory.getLong(startOffset);
                    return;
                }

                long d = arg.getLong(record);
                if (firstNotNullIdx == -1 && d != Numbers.LONG_NULL) {
                    firstNotNullIdx = count;
                    memory.putLong(startOffset, d);
                    this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                } else {
                    this.firstValue = Numbers.LONG_NULL;
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                long d = arg.getLong(record);
                if (firstNotNullIdx != -1 && memory.getLong(startOffset + loIdx * Long.BYTES) != Numbers.LONG_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    this.firstValue = memory.getLong(startOffset + firstNotNullIdx * Long.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        long res = memory.getLong(startOffset + (loIdx + i) % bufferSize * Long.BYTES);
                        if (res != Numbers.LONG_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            this.firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                    }

                }

                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putLong(startOffset + loIdx * Long.BYTES, d);
            }
        }

        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles first_value() ignore nulls over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class FirstNotNullValueOverRangeFrameFunction extends FirstValueOverRangeFrameFunction implements Reopenable, WindowLongFunction {

        /**
         * Create a RANGE-framed first-value implementation that ignores nulls.
         * <p>
         * This constructor builds a window function that computes the first non-null
         * long value inside a RANGE frame defined by offsets [rangeLo, rangeHi]
         * relative to the designated ordering timestamp.
         *
         * @param rangeLo      lower bound of the RANGE frame (inclusive), expressed as an offset relative to the ordering timestamp
         * @param rangeHi      upper bound of the RANGE frame (inclusive), expressed as an offset relative to the ordering timestamp
         * @param arg          expression that produces the LONG values for which the first non-null is computed
         * @param timestampIdx index of the designated timestamp column used to evaluate RANGE frame bounds
         */
        public FirstNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        /**
         * Processes the next input record into the sliding range frame, updating internal ring-buffer
         * state and the currently reported first value.
         *
         * <p>This method:
         * - Computes the current row timestamp and, if the frame has an unbounded lower bound,
         * updates the cached first value quickly when possible.
         * - Evicts elements outside the allowed timestamp window (based on maxDiff) from the buffer.
         * - Locates the first non-null value inside the current frame bounds (based on minDiff).
         * - Appends the current row's value to the circular buffer if it is not `Numbers.LONG_NULL`,
         * expanding the underlying memory and re-linearizing the circular buffer when capacity is reached.
         * - Sets {@code firstValue} to the discovered first-in-frame value, or to `Numbers.LONG_NULL`
         * (or to the current value when the frame includes the current row) when no earlier value is found.
         * <p>
         * Side effects: mutates fields including {@code firstIdx}, {@code firstValue}, {@code size},
         * {@code capacity}, and {@code startOffset}, and may resize the backing memory.
         *
         * @param record the current input row whose timestamp and argument value are considered for
         *               frame eviction and insertion
         */
        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) { // use firstIdx as a flag firstValue has in frame.
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        firstValue = memory.getLong(startOffset + Long.BYTES);
                    } else {
                        firstValue = Numbers.LONG_NULL;
                    }
                } else {
                    // first value always in first index case when not frameLoBounded
                    firstValue = memory.getLong(startOffset + Long.BYTES);
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
                        this.firstValue = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    }
                    break;
                }
            }
            firstIdx = newFirstIdx;
            long d = arg.getLong(record);
            if (d != Numbers.LONG_NULL) {
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
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            if (!findNewFirstValue) {
                this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
            }
        }


        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles first_value() ignore nulls over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class FirstNotNullValueOverRowsFrameFunction extends FirstValueOverRowsFrameFunction implements Reopenable, WindowLongFunction {
        private long firstNotNullIdx = -1;

        /**
         * Create a rows-based "first value" window function that ignores nulls.
         * <p>
         * This constructor initializes a frame that scans a sliding window defined by
         * the row-offset bounds [rowsLo, rowsHi] and returns the first non-null long
         * value within that frame for each row.
         *
         * @param arg    the input long-valued function whose values are scanned for the first non-null entry
         * @param rowsLo lower bound of the rows frame (can be negative for preceding rows)
         * @param rowsHi upper bound of the rows frame (can be zero or positive)
         * @param memory native memory buffer used to store the frame's ring buffer state (must remain valid for the function's lifetime)
         */
        public FirstNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        /**
         * Advance the window computation by one input record, updating internal circular buffer
         * and the cached `firstValue` for the current frame.
         *
         * <p>Behavior differs by whether the window lower bound is bounded (sliding row frame)
         * or unbounded:
         * <ul>
         *   <li>Unbounded lower bound: track the first non-null value seen so far. If a first non-null
         *       value has been found and the buffer has advanced past it, expose that value; otherwise
         *       update the first-non-null tracking when a new non-null input is observed.</li>
         *   <li>Bounded lower bound: maintain a circular buffer of the last `frameSize` values,
         *       scan from the current lower-bound index for the first non-null entry, and update
         *       `firstValue` accordingly. If no non-null is found in the frame, `firstValue` is set
         *       to the current row's value only when the frame includes the current row; otherwise
         *       it becomes `Numbers.LONG_NULL`.</li>
         * </ul>
         * <p>
         * Side effects:
         * - mutates the circular `buffer` contents and `loIdx`,
         * - updates `firstNotNullIdx`, `count` (for unbounded case), and the exported `firstValue`.
         *
         * @param record the input row to process; its long argument value is read to update the buffer
         */
        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getLong(0);
                    return;
                }

                long d = arg.getLong(record);
                if (firstNotNullIdx == -1 && d != Numbers.LONG_NULL) {
                    firstNotNullIdx = count;
                    buffer.putLong(0, d);
                    this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                } else {
                    this.firstValue = Numbers.LONG_NULL;
                }
                count++;
            } else {
                long d = arg.getLong(record);
                if (firstNotNullIdx != -1 && buffer.getLong((long) loIdx * Long.BYTES) != Numbers.LONG_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    this.firstValue = buffer.getLong(firstNotNullIdx * Long.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        long res = buffer.getLong((long) (loIdx + i) % bufferSize * Long.BYTES);
                        if (res != Numbers.LONG_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            this.firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                    }
                }

                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putLong((long) loIdx * Long.BYTES, d);
                loIdx = (loIdx + 1) % bufferSize;
            }
        }


        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Reinitializes this function for a new processing pass.
         * <p>
         * Calls the superclass reopen logic and clears the cached index of the first non-null value
         * by setting {@code firstNotNullIdx} to {@code -1}.
         */
        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        /**
         * Reset internal state for reuse.
         * <p>
         * Clears the superclass state and resets the cached index of the first non-null value
         * to -1 (meaning "not set").
         */
        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        /**
         * Reset the function's iteration state to the start.
         * <p>
         * Calls the superclass reset and clears the cached index of the first non-null value by
         * setting {@code firstNotNullIdx} to {@code -1}.
         */
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
        /**
         * Create a per-partition, rows-framed implementation that returns the first non-null long
         * value for an unbounded-preceding frame within each partition.
         *
         * @param map               per-partition state map used to store the first value
         * @param partitionByRecord record describing the current partition key
         * @param partitionBySink   sink used to write/read partition key values into map keys
         * @param arg               argument function that produces the long values to inspect
         */
        public FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Advances computation for the given record: looks up the partition key and sets this.value
         * to the first seen non-null long for that partition.
         * <p>
         * If a map entry for the partition exists, this.value is loaded from the map. Otherwise the
         * function reads the long from the input argument; if that value is not `Numbers.LONG_NULL`
         * a new map entry is created and the value is stored and assigned to this.value. If the input
         * is null, this.value is set to `Numbers.LONG_NULL`.
         * <p>
         * Side effects: may create a new map value for the current partition and updates the instance
         * field `this.value`.
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.findValue();
            if (mapValue != null) {
                this.value = mapValue.getLong(0);
            } else {
                long d = arg.getLong(record);
                if (d != Numbers.LONG_NULL) {
                    mapValue = key.createValue();
                    mapValue.putLong(0, d);
                    this.value = d;
                } else {
                    this.value = Numbers.LONG_NULL;
                }
            }
        }

        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // handles:
    // first_value() ignore nulls over () - empty clause, no partition by no order by, no frame == default frame
    // first_value() ignore nulls over (rows between unbounded preceding and current row); there's no partition by.
    public static class FirstNotNullValueOverWholeResultSetFunction extends FirstValueOverWholeResultSetFunction {

        /**
         * Construct a first_value implementation that returns the first non-null LONG over the whole result set.
         *
         * @param arg source function that produces the LONG values to inspect; nulls produced by this function are ignored
         */
        public FirstNotNullValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        /**
         * Processes the next input record and captures the first non-null long value seen.
         * <p>
         * If a non-null value is found in the supplied record via {@code arg.getLong(record)},
         * it is stored into {@code this.value} and {@code this.found} is set to {@code true}.
         *
         * @param record the current input record to inspect for the first-value candidate
         */
        @Override
        public void computeNext(Record record) {
            if (!found) {
                long d = arg.getLong(record);
                if (d != Numbers.LONG_NULL) {
                    this.value = d;
                    this.found = true;
                }
            }
        }

        /**
         * Indicates this window function requires two evaluation passes.
         *
         * @return WindowFunction.TWO_PASS to signal a two-pass (pass1 then pass2) execution model.
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        /**
         * Indicates this window function operates in "IGNORE NULLS" mode.
         *
         * @return true when the function ignores NULL input values
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First pass consumer that captures the first non-null long value seen.
         * <p>
         * If a non-null long is read from the supplied record and no value has been
         * recorded yet, stores that value and marks the function as having found a
         * value. Subsequent calls are no-ops once a value has been found.
         * <p>
         * This method intentionally ignores records whose `arg.getLong(record)` returns
         * {@code Numbers.LONG_NULL}.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                long d = arg.getLong(record);
                if (d != Numbers.LONG_NULL) {
                    this.value = d;
                    this.found = true;
                }
            }
        }

        /**
         * Write the function's current long result into the output column for the given row.
         * <p>
         * This stores the internally held `value` at the memory location corresponding to
         * the row identified by {@code recordOffset} and the function's output column.
         *
         * @param recordOffset byte offset of the target row in the record memory
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function's transient state for reuse.
         * <p>
         * Clears the cached first-value flag and stored value, and invokes the superclass reset.
         */
        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Numbers.LONG_NULL;
        }

        /**
         * Resets the function's iteration state to the start.
         * <p>
         * Invokes the superclass reset, clears the "found" flag, and resets the cached
         * value to `Numbers.LONG_NULL` so subsequent computations start from a clean state.
         */
        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Numbers.LONG_NULL;
        }
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class FirstValueOverCurrentRowFunction extends BaseWindowFunction implements WindowLongFunction {

        private final boolean ignoreNulls;
        private long value;

        /**
         * Create a first_value implementation that returns the argument value of the current row.
         *
         * @param arg         the input value function whose current-row value will be returned
         * @param ignoreNulls if true, null values from {@code arg} are ignored (the function will behave
         *                    according to the "ignore nulls" semantics); if false, nulls are respected
         */
        FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
        }

        /**
         * Read the function argument from the given input record and store it in the function's internal `value` field.
         *
         * @param record input row to read the argument from
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getLong(record);
        }

        /**
         * Returns the function's current long result.
         * <p>
         * This implementation ignores the supplied Record and always returns the internally stored value.
         *
         * @param rec ignored
         * @return the stored long value
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of execution passes required for this window function.
         *
         * @return the pass count (ZERO_PASS)
         */
        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        /**
         * Indicates whether this window function is configured to ignore NULL input values.
         *
         * @return true if NULLs are ignored (rows with NULL inputs are skipped when computing the first value);
         * false if NULLs are respected (NULL may be returned as the first value)
         */
        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        /**
         * First pass for the window function: computes the next value for the given input record
         * and writes the resulting long into the WindowSPI's memory at the supplied record offset
         * and the instance's columnIndex.
         *
         * @param record       the input record to compute the value from
         * @param recordOffset the memory offset (as returned by WindowSPI.getAddress) where the
         *                     computed long should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles first_value() over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class FirstValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private long firstValue;

        /**
         * Constructs a partitioned first_value operator using the supplied per-partition state and
         * partitioning metadata.
         * <p>
         * The provided map stores per-partition results; partitionByRecord and partitionBySink describe
         * how partition keys are read and written; `arg` is the expression supplying values for
         * which the first value per partition is computed.
         */
        public FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Advance computation for the next input row: locate or create the per-partition slot,
         * and ensure `firstValue` contains the first-seen value for that partition.
         * <p>
         * If the partition key is new, this reads the argument value from the provided record,
         * stores it in the partition map, and sets the instance field `firstValue`. If the
         * partition already exists, `firstValue` is loaded from the stored map value.
         *
         * @param record the input row used to derive the partition key and (for new partitions)
         *               to read the candidate first_value
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                firstValue = arg.getLong(record);
                value.putLong(0, firstValue);
            } else {
                firstValue = value.getLong(0);
            }
        }

        /**
         * Return the computed first_value for the current window position.
         *
         * <p>The supplied Record parameter is ignored; the function returns the internally held
         * firstValue for the window.</p>
         *
         * @param rec ignored; present to satisfy the WindowLongFunction contract
         * @return the first_value long result for the current window
         */
        @Override
        public long getLong(Record rec) {
            return firstValue;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Computes the next first_value for the given input record and writes it to the output column.
         * <p>
         * computeNext(record) updates internal state, and the current `firstValue` is written as a 64-bit
         * long into the column memory for the row at `recordOffset`.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    // Handles first_value() over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
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
        protected long firstValue;

        /**
         * Create a partitioned, RANGE-framed implementation of FIRST_VALUE for LONG values.
         * <p>
         * Constructs an instance configured for a RANGE frame defined by {@code rangeLo} and {@code rangeHi}
         * and initialized with per-partition state and an underlying ring-buffer memory region.
         *
         * @param map               per-partition map used to store frame metadata and state
         * @param partitionByRecord record view used to read the partition key for the current row
         * @param partitionBySink   sink used to write the partition key into map keys
         * @param rangeLo           lower bound of the RANGE frame (use {@code Long.MIN_VALUE} for unbounded)
         * @param rangeHi           upper bound of the RANGE frame (typically relative to the ordering timestamp)
         * @param arg               input function providing the LONG values for FIRST_VALUE
         * @param memory            native memory region used for the ring buffer that stores (timestamp, value) pairs
         * @param initialBufferSize initial size (in bytes/slots) to allocate for the ring buffer
         * @param timestampIdx      record column index that provides the ordering/timestamp value used by RANGE
         */
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

        /**
         * Release resources held by this function.
         * <p>
         * Calls the superclass close, closes the associated native memory buffer, and clears the free-list
         * used for buffer management.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        /**
         * Computes the next value for the first_value window function, handling range-based and rows-based framing.
         * <p>
         * The function maintains a per-partition ring buffer of (timestamp, value) pairs to track the first value within the current frame.
         * It dynamically expands the buffer as needed and updates the frame boundaries as new rows are processed.
         *
         * @param record the current input record
         */
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
            long d = arg.getLong(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                memory.putLong(startOffset, timestamp);
                memory.putLong(startOffset + Long.BYTES, d);
                size = 1;

                if (frameIncludesCurrentValue) {
                    firstValue = d;
                    frameSize = 1;
                } else {
                    firstValue = Numbers.LONG_NULL;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize > 0) {
                    firstValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
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
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
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
                    firstValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    firstValue = Numbers.LONG_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        /**
         * Return the computed first_value for the current window position.
         *
         * <p>The supplied Record parameter is ignored; the function returns the internally held
         * firstValue for the window.</p>
         *
         * @param rec ignored; present to satisfy the WindowLongFunction contract
         * @return the first_value long result for the current window
         */
        @Override
        public long getLong(Record rec) {
            return firstValue;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reinitializes state for a reopened window.
         * <p>
         * Calls the superclass reopen logic and resets the cached first value to
         * the LONG null sentinel (Numbers.LONG_NULL). Memory for buffers is not
         * allocated here and will be deferred until first use.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            firstValue = Numbers.LONG_NULL;
        }

        /**
         * Reset the function's internal state and release per-frame native resources.
         * <p>
         * Performs superclass reset logic, closes the associated MemoryARW buffer, and clears
         * the free-list used for recycled buffer slots.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        /**
         * Append this window function's textual plan to the provided sink.
         * <p>
         * Writes `name(arg)` followed by an optional `ignore nulls` hint and an
         * `OVER (partition by ... range between {maxDiff} preceding and {minDiff} preceding|current row)` clause.
         */
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

        /**
         * Reset this object's internal state to its initial "top" position.
         * <p>
         * Truncates the associated memory storage and clears the free-list so the
         * buffer is empty and ready for reuse. Also calls the superclass toTop()
         * to perform any base-class reset behavior.
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    // handles first_value() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        // holds fixed-size ring buffers of long values
        protected final MemoryARW memory;
        protected long firstValue;

        /**
         * Constructs a rows-frame, partitioned FirstValue implementation and initializes
         * frame bookkeeping (frame size, circular buffer size and bounds) based on the
         * frame's lower/upper offsets.
         * <p>
         * The constructor computes:
         * - frameSize: number of rows in the frame window (or 1 when lower bound is unbounded),
         * - bufferSize: capacity of the circular buffer used to track preceding rows,
         * - frameLoBounded: true when the frame has a bounded lower offset,
         * - frameIncludesCurrentValue: true when the upper bound includes the current row (rowsHi == 0).
         *
         * @param rowsLo lower bound offset for the ROWS frame (can be Long.MIN_VALUE to indicate unbounded preceding)
         * @param rowsHi upper bound offset for the ROWS frame
         */
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

        /**
         * Advances the window computation for the given input record and sets the current
         * firstValue for its partition according to the rows-based frame logic.
         *
         * <p>This method:
         * - Retrieves or creates per-partition state from a Map. The MapValue layout is:
         * 0: 0-based index of the oldest slot in the ring buffer (lo index),
         * 1: native memory start offset for the partition's ring buffer,
         * 2: current count of values stored in the buffer.
         * - Maintains a fixed-size circular buffer stored in native memory where each slot
         * holds a long (initialized to LONG_NULL for new partitions).
         * - Determines the first value for the partition's current frame using the buffer
         * contents, the current input value, and frame bounds (bounded/unbounded,
         * whether the frame includes the current row).
         * - Updates the per-partition state in the map (lo index and count) and writes the
         * current row's value into the buffer slot previously identified as the oldest.
         * <p>
         * Side effects:
         * - Mutates the per-partition MapValue and native memory buffer.
         * - Sets the instance field `firstValue` to the computed result (or LONG_NULL).
         */
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
            long d = arg.getLong(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Numbers.LONG_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);

                if (!frameLoBounded && count == bufferSize) {
                    // loIdx already points at the 'oldest' element because frame is 1-el. big and buffer is full
                    firstValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = memory.getLong(startOffset + (loIdx + bufferSize - count) % bufferSize * Long.BYTES);
            } else {
                firstValue = Numbers.LONG_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);

            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        /**
         * Return the computed first_value for the current window position.
         *
         * <p>The supplied Record parameter is ignored; the function returns the internally held
         * firstValue for the window.</p>
         *
         * @param rec ignored; present to satisfy the WindowLongFunction contract
         * @return the first_value long result for the current window
         */
        @Override
        public long getLong(Record rec) {
            return firstValue;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Computes the next first_value for the given input record and writes it to the output column.
         * <p>
         * computeNext(record) updates internal state, and the current `firstValue` is written as a 64-bit
         * long into the column memory for the row at `recordOffset`.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reinitializes the function for reuse across reopen calls.
         * <p>
         * Calls the superclass reopen logic; memory-backed buffers are not allocated here and
         * will be allocated lazily on first use.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
        }

        /**
         * Reset the function to its initial state and release owned native memory.
         * <p>
         * Calls super.reset() to clear base-class state, then closes the associated
         * memory buffer to free native resources.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Appends a textual "plan" representation of this window function into the given PlanSink.
         * <p>
         * The output format is `{name}({arg}) [ignore nulls] over (partition by {partitionFuncs}
         * rows between {bufferSize} preceding and {N} preceding|current row)`, where the trailing
         * bound is rendered as "current row" when the frame includes the current value, otherwise
         * as an explicit preceding offset computed from bufferSize and frameSize.
         *
         * @param sink destination to receive the rendered plan
         */
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

        /**
         * Reset iteration state to the top and clear any stored frame data.
         * <p>
         * Invokes the superclass reset behavior and truncates the backing memory buffer so previously
         * accumulated frame entries are discarded.
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    // Handles first_value() over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        protected final int RECORD_SIZE = Long.BYTES + Long.BYTES;
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
        protected long firstValue;
        protected long frameSize;
        protected long size;
        protected long startOffset;

        /**
         * Constructs a range-framed first_value function for LONG values.
         * <p>
         * Initializes frame bounds, buffer sizing, and a native circular memory buffer used to
         * store (timestamp, value) records for computing the first value within a sliding
         * RANGE window.
         *
         * @param rangeLo       lower bound of the RANGE frame (inclusive offset relative to row timestamp);
         *                      Long.MIN_VALUE denotes unbounded preceding.
         * @param rangeHi       upper bound of the RANGE frame (inclusive offset relative to row timestamp);
         *                      typically 0 when the frame includes the current row.
         * @param arg           the value argument function (LONG) whose first value is computed over the frame
         * @param configuration runtime configuration used to size and allocate the native circular buffer
         * @param timestampIdx  column index of the designated timestamp used for RANGE comparisons
         */
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

        /**
         * Closes the function and frees associated resources.
         * <p>
         * This releases superclass resources and closes the backing memory region. After calling
         * this method the instance must not be used.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
        }

        /**
         * Advances the window with the given input record and updates the internal ring buffer and current first value.
         *
         * <p>The method:
         * - Removes expired entries when the lower bound of a RANGE frame is bounded.
         * - Appends the current record (timestamp and value) into the circular buffer, growing and rebase-copying the buffer if needed.
         * - Recomputes the frame size and the index of the first element in the frame.
         * - Updates {@code firstValue} to the first element's value in the current frame or to {@link io.questdb.std.Numbers#LONG_NULL} when the frame is empty.</p>
         *
         * <p>Side effects: mutates buffer state fields such as {@code firstIdx}, {@code size}, {@code capacity},
         * {@code startOffset}, {@code frameSize}, and {@code firstValue}. The underlying memory may be resized,
         * which can change base addresses during execution.</p>
         *
         * @param record the input record whose timestamp and value are used to advance the window and update the frame
         */
        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize > 0) {
                firstValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getLong(record);

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
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
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
                firstValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                firstValue = Numbers.LONG_NULL;
            }
        }

        /**
         * Return the computed first_value for the current window position.
         *
         * <p>The supplied Record parameter is ignored; the function returns the internally held
         * firstValue for the window.</p>
         *
         * @param rec ignored; present to satisfy the WindowLongFunction contract
         * @return the first_value long result for the current window
         */
        @Override
        public long getLong(Record rec) {
            return firstValue;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reset the function's internal state and buffers to their initial configuration.
         * <p>
         * Clears any stored first-value marker, restores the ring-buffer capacity to the
         * initial capacity, and resets offsets, indices, and size counters so the
         * function behaves as if newly constructed.
         */
        @Override
        public void reopen() {
            firstValue = Numbers.LONG_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }

        /**
         * Reset the function to its initial state and release owned native memory.
         * <p>
         * Calls super.reset() to clear base-class state, then closes the associated
         * memory buffer to free native resources.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Append a textual plan representation of this window function to the given PlanSink.
         * <p>
         * The output format is: `{name}({arg}) [ignore nulls] over (range between {maxDiff} preceding and {minDiff or "current row"})`.
         * This includes the function name, argument, optional "ignore nulls" hint, and RANGE frame bounds using
         * `maxDiff` as the preceding upper bound and `minDiff` (or "current row" when zero) as the lower bound.
         *
         * @param sink the PlanSink to receive the plan text
         */
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

        /**
         * Reset the function's internal state and backing memory to its initial empty configuration.
         * <p>
         * This clears any stored values, truncates the allocated memory buffer, restores the
         * initial capacity, sets the first-value marker to NULL, and resets indices and sizes
         * so the instance behaves as if just constructed.
         */
        @Override
        public void toTop() {
            super.toTop();
            firstValue = Numbers.LONG_NULL;
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
    public static class FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected long count = 0;
        protected long firstValue;
        protected int loIdx = 0;

        /**
         * Creates a rows-based FIRST_VALUE window function instance configured for the given frame.
         * <p>
         * The constructor interprets rowsLo and rowsHi as the lower and upper frame bounds
         * relative to the current row (e.g., -N for N preceding, 0 for current row, N for N following).
         * It computes internal buffer and frame sizes and flags used by the implementation:
         * - frameSize: number of rows in the frame used to determine the first value.
         * - bufferSize: number of previous rows that must be retained to evaluate the frame.
         * - frameLoBounded: true when the lower bound is bounded (rowsLo > Long.MIN_VALUE).
         * - frameIncludesCurrentValue: true when the frame includes the current row (rowsHi == 0).
         * <p>
         * Special conditions:
         * - The pair (Long.MIN_VALUE, 0) is not allowed here; that case should use FirstValueOverWholeResultSetFunction.
         *
         * @param arg    the input value function whose long values are evaluated by this window function
         * @param rowsLo lower frame bound relative to the current row (use Long.MIN_VALUE for unbounded preceding)
         * @param rowsHi upper frame bound relative to the current row
         */
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

        /**
         * Closes the function and releases associated resources.
         * <p>
         * Calls {@code super.close()} and then closes the internal ring buffer to free its memory.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        /**
         * Advance the rows-based sliding window by one input record and compute the current first_value.
         * <p>
         * This reads the current long value from the provided record, appends it into the internal circular
         * buffer (overwriting the oldest entry when full), advances the buffer index and element count, and
         * updates the cached {@code firstValue} according to the configured frame:
         * - If the frame is effectively unbounded on the low side and the buffer contains more elements
         * than the frame can hold, the first value is taken from the oldest element within the active
         * frame window.
         * - If the frame currently includes only the current row and no prior rows are present, the
         * current record's value becomes {@code firstValue}.
         * - Otherwise, {@code firstValue} is set to the LONG null sentinel (Numbers.LONG_NULL).
         * <p>
         * The method mutates internal state: the circular buffer contents, {@code loIdx}, {@code count},
         * and {@code firstValue}.
         *
         * @param record the input record for the current row from which the value is read
         */
        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && count > (bufferSize - frameSize)) {
                firstValue = buffer.getLong((loIdx + bufferSize - count) % bufferSize * Long.BYTES);
                return;
            }

            long d = arg.getLong(record);

            if (count > bufferSize - frameSize) {//we've some elements in the frame
                firstValue = buffer.getLong((loIdx + bufferSize - count) % bufferSize * Long.BYTES);
            } else if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else {
                firstValue = Numbers.LONG_NULL;
            }

            count = Math.min(count + 1, bufferSize);

            //overwrite oldest element
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        /**
         * Return the computed first_value for the current window position.
         *
         * <p>The supplied Record parameter is ignored; the function returns the internally held
         * firstValue for the window.</p>
         *
         * @param rec ignored; present to satisfy the WindowLongFunction contract
         * @return the first_value long result for the current window
         */
        @Override
        public long getLong(Record rec) {
            return firstValue;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Computes the next first_value for the given input record and writes it to the output column.
         * <p>
         * computeNext(record) updates internal state, and the current `firstValue` is written as a 64-bit
         * long into the column memory for the row at `recordOffset`.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reset the function's internal state and buffers so the instance can be reused.
         * <p>
         * Sets the current first-value marker to NULL, resets the low index and count,
         * and reinitializes the internal ring buffer.
         */
        @Override
        public void reopen() {
            firstValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        /**
         * Reset this function's running state to its initial, empty condition.
         * <p>
         * Calls {@code super.reset()}, closes the underlying buffer (releasing its resources),
         * and clears internal state used to track the frame: sets {@code firstValue} to NULL,
         * and zeroes {@code loIdx} and {@code count}.
         */
        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue = Numbers.LONG_NULL;
            loIdx = 0;
            count = 0;
        }

        /**
         * Renders a textual plan representation of this window function into the provided PlanSink.
         * <p>
         * The output format is:
         * {functionName}({arg})[ ignore nulls] over ( rows between {bufferSize} preceding and
         * {current row|(bufferSize + 1 - frameSize) preceding} )
         * <p>
         * Uses the instance's name, argument, nulls-handling flag, bufferSize, and frameSize/frameIncludesCurrentValue
         * to produce the formatted window clause.
         */
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

        /**
         * Reset the function to its initial state so it can be reused from the start.
         * <p>
         * Clears the stored first value, resets the low index and row counter, and
         * reinitializes the internal buffer.
         */
        @Override
        public void toTop() {
            super.toTop();
            firstValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        /**
         * Initialize the native buffer by writing the sentinel value `Numbers.LONG_NULL` into
         * each long-sized slot.
         * <p>
         * This fills `bufferSize` consecutive long positions at offsets `i * Long.BYTES` with
         * the null sentinel to mark them as empty/uninitialized.
         */
        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Numbers.LONG_NULL);
            }
        }
    }

    // Handles:
    // - first_value(a) over (partition by x rows between unbounded preceding and [current row | x preceding ])
    // - first_value(a) over (partition by x order by ts range between unbounded preceding and [current row | x preceding])
    static class FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        protected long value;

        /**
         * Create a FirstValueOverUnboundedPartitionRowsFrameFunction bound to a per-partition state map.
         *
         * @param arg the input value expression whose first value (within the partition's unbounded-rows frame)
         *            this function will produce
         */
        public FirstValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Processes the next input record by locating or creating the partition entry and producing the partition's first value.
         * <p>
         * If the partition key does not exist yet, reads the long argument from the supplied record, stores it as the partition's value,
         * and updates the instance field `value`. If the partition entry already exists, loads that stored value into `value`.
         *
         * @param record the input row used to derive the partition key and, for new partitions, the first_value to store
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                long d = arg.getLong(record);
                mapValue.putLong(0, d);
                value = d;
            } else {
                value = mapValue.getLong(0);
            }
        }

        /**
         * Returns the function's current long result.
         * <p>
         * This implementation ignores the supplied Record and always returns the internally stored value.
         *
         * @param rec ignored
         * @return the stored long value
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First pass for the window function: computes the next value for the given input record
         * and writes the resulting long into the WindowSPI's memory at the supplied record offset
         * and the instance's columnIndex.
         *
         * @param record       the input record to compute the value from
         * @param recordOffset the memory offset (as returned by WindowSPI.getAddress) where the
         *                     computed long should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Append a textual plan representation of this window function to the provided sink.
         * <p>
         * The rendered form is: `name(arg)` optionally followed by ` ignore nulls`, then
         * ` over (partition by <partition expressions> rows between unbounded preceding and current row)`.
         */
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
    public static class FirstValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowLongFunction {
        protected boolean found;
        protected long value = Numbers.LONG_NULL;

        /**
         * Construct a function that computes FIRST_VALUE over the entire result set using the given argument as the source value.
         *
         * @param arg expression that produces the LONG value to be used as the first_value source
         */
        public FirstValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        /**
         * Consume a single input record and capture the first encountered long value.
         * <p>
         * If a value has not yet been captured for this window frame, reads the long
         * from the provided record and stores it in the instance state (`value`),
         * marking the value as found. Subsequent calls have no effect.
         *
         * @param record the input record to read the long value from
         */
        @Override
        public void computeNext(Record record) {
            if (!found) {
                this.value = arg.getLong(record);
                this.found = true;
            }
        }

        /**
         * Return the current stored long result for the window function.
         * <p>
         * The provided record parameter is ignored; this implementation always
         * returns the internally held value.
         *
         * @param rec ignored input record
         * @return the stored long value
         */
        @Override
        public long getLong(Record rec) {
            return this.value;
        }

        /**
         * Returns the function name exposed by this factory.
         *
         * @return the function name ("first_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Number of processing passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require any window-processing passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First pass for the window function: computes the next value for the given input record
         * and writes the resulting long into the WindowSPI's memory at the supplied record offset
         * and the instance's columnIndex.
         *
         * @param record       the input record to compute the value from
         * @param recordOffset the memory offset (as returned by WindowSPI.getAddress) where the
         *                     computed long should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function's transient state for reuse.
         * <p>
         * Clears the cached first-value flag and stored value, and invokes the superclass reset.
         */
        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Numbers.LONG_NULL;
        }

        /**
         * Resets the function's iteration state to the start.
         * <p>
         * Invokes the superclass reset, clears the "found" flag, and resets the cached
         * value to `Numbers.LONG_NULL` so subsequent computations start from a clean state.
         */
        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Numbers.LONG_NULL;
        }
    }

    static {
        FIRST_VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_COLUMN_TYPES.add(ColumnType.LONG);
    }
}
