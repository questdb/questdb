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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

// Returns value evaluated at the row that is the first row of the window frame.
public class FirstValueTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "first_value";
    protected static final ArrayColumnTypes FIRST_VALUE_COLUMN_TYPES;
    private static final String SIGNATURE = NAME + "(N)";

    /**
     * Returns the function signature string for this factory.
     *
     * @return the signature "first_value(N)"
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Create a timestamp "first_value" window function instance appropriate for the current window context.
     *
     * <p>Validates the WindowContext, handles the special case where the frame is empty
     * (rowsHi &lt; rowsLo) by returning a TimestampNullFunction, and otherwise dispatches
     * to either the "ignore nulls" or "respect nulls" implementation generator based on
     * windowContext.isIgnoreNulls().</p>
     *
     * @param position parser/bytecode position used for error reporting
     * @param args     function argument list (first argument is the value expression)
     * @return a Function implementation tailored to the window framing, partitioning,
     * and nulls handling of the current WindowContext
     * @throws SqlException if the WindowContext is invalid or the requested combination
     *                      of framing/ordering/partitioning is not supported
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
            return new TimestampNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    windowContext.getFramingMode() == WindowColumn.FRAMING_RANGE,
                    windowContext.getPartitionByRecord(),
                    Numbers.LONG_NULL
            );
        }

        return windowContext.isIgnoreNulls() ?
                this.generateIgnoreNullsFunction(position, args, configuration, windowContext) :
                this.generateRespectNullsFunction(position, args, configuration, windowContext);
    }

    /**
     * Selects and constructs the appropriate "first_value" window-function implementation for the
     * "IGNORE NULLS" mode based on window framing (RANGE or ROWS), presence of a partition key,
     * ordering, and frame bounds.
     * <p>
     * The method returns specialized Function instances optimized for:
     * - partitioned vs. non-partitioned windows,
     * - range-based or row-based frames,
     * - unbounded/whole-partition cases,
     * - moving frames backed by in-memory circular buffers or partition-scoped maps,
     * - single-row (current row) frames.
     * <p>
     * Behavior notes:
     * - When RANGE framing is used with ordering, the method requires ordering by the designated
     * timestamp; otherwise it throws a SqlException.
     * - For unsupported combinations of framing/partitioning/bounds the method throws a SqlException.
     *
     * @param position source position used for SqlException error reporting when parameters are unsupported
     * @param args     function argument list; the first element (args.get(0)) is the value expression
     * @return a Function implementing first_value with IGNORE NULLS for the given window context
     * @throws SqlException if RANGE is requested but ordering is not by the designated timestamp, or
     *                      if the provided window parameters have no supported implementation
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
            if (framingMode == WindowColumn.FRAMING_RANGE) {
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
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
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
            if (framingMode == WindowColumn.FRAMING_RANGE) {
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
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
                // between unbounded preceding and [current row | unbounded following]
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new FirstNotNullValueOverWholeResultSetFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new FirstValueTimestampWindowFunctionFactory.FirstValueOverCurrentRowFunction(args.get(0), true);
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
     * Creates a window function implementation for FIRST_VALUE(...) when NULLs are respected.
     * <p>
     * The returned implementation is chosen based on the WindowContext: partitioning presence,
     * framing mode (RANGE or ROWS), ordering, and frame bounds (rowsLo/rowsHi). Supported targets
     * include whole-result-set, per-partition, range-frame and row-frame variants; implementations
     * may allocate per-partition maps or native circular buffers as required.
     *
     * @param position      parser/token position used when constructing SqlException for unsupported combinations
     * @param args          function arguments (first argument is the value expression for FIRST_VALUE)
     * @param configuration Cairo configuration used for creating maps and native memory (omitted from @param of services is intentional)
     * @param windowContext describes partitioning, ordering, framing mode and frame bounds used to select the implementation
     * @return a Function implementing FIRST_VALUE for timestamp values that respects NULLs
     * @throws SqlException if the requested combination is not implemented or if RANGE framing is used with a non-designated-timestamp ordering
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
            if (framingMode == WindowColumn.FRAMING_RANGE) {
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
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
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
            if (framingMode == WindowColumn.FRAMING_RANGE) {
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
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
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
     * Indicates that the factory includes NULL-handling information in generated plans.
     *
     * @return true if the function's plan should describe NULL-handling (e.g., IGNORE/RESPECT NULLS)
     */
    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    // handles first_value() ignore nulls over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class FirstNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        /**
         * Construct a function that computes the first non-null timestamp value for each partition.
         * <p>
         * The instance stores per-partition state in the provided map and uses the given record/sink
         * to identify partitions. The supplied argument function is used to read the timestamp value
         * for each input record; the function emits the first non-null value observed for a partition.
         *
         * @param arg function that produces the timestamp value for the current record
         */
        public FirstNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires two passes over the data.
         *
         * @return {@link WindowFunction#TWO_PASS}
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
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First pass: record the first non-null timestamp observed for the current partition.
         * <p>
         * If the partition has no stored value and the argument's timestamp for this record is not
         * NULL, stores that timestamp into the partition map. Existing per-partition values are never
         * overwritten by this method.
         *
         * @param record       current input record
         * @param recordOffset offset of the record in the input (unused)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                long d = arg.getTimestamp(record);
                if (d != Numbers.LONG_NULL) {
                    MapValue value = key.createValue();
                    value.putTimestamp(0, d);
                }
            }
        }

        /**
         * Emit the stored first-value timestamp for the record's partition into the output column during pass 2.
         * <p>
         * Looks up the partition key in the per-partition map (populated during pass1) and writes the persisted
         * timestamp or SQL NULL (Numbers.LONG_NULL) into the SPI-backed row at the given recordOffset.
         *
         * @param record       the current record used to derive the partition key
         * @param recordOffset byte offset within the SPI row where the timestamp should be written
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

    // Handles first_value() ignore nulls over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class FirstNotNullValueOverPartitionRangeFrameFunction extends FirstValueOverPartitionRangeFrameFunction {
        /**
         * Creates a partitioned RANGE-frame implementation that tracks the first non-null timestamp value
         * within a sliding time range for each partition using an in-memory ring buffer.
         * <p>
         * The instance maintains per-partition buffers of (timestamp, value) pairs and updates the
         * first-non-null value as the frame moves. This constructor wires the partition map and
         * partition keys/sink together with framing bounds and buffer configuration.
         *
         * @param rangeLo           lower bound of the RANGE frame (inclusive) expressed as timestamp offset
         * @param rangeHi           upper bound of the RANGE frame (inclusive) expressed as timestamp offset
         * @param arg               the value-producing function (the column/expression whose timestamp values are tracked)
         * @param initialBufferSize initial capacity (number of entries) for the per-partition ring buffer
         * @param timestampIdx      index of the designated timestamp column within buffered records
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
         * Advance function state for the given input record by updating the per-partition ring buffer
         * that maintains [timestamp, value] pairs for a RANGE window and computing the current
         * first-value for that partition.
         *
         * <p>Behavior summary:
         * - Locates or creates a partition entry in the backing map and reads/writes four metadata
         * fields (stored at positions 0..3): startOffset, size, capacity, firstIdx.
         * - For a new partition, allocates an initial in-memory buffer and inserts the current row
         * if its argument value is non-null.
         * - For an existing partition, evicts elements that fall outside the current range frame
         * (based on maxDiff/minDiff relative to the current row's timestamp), optionally expands
         * the ring buffer when full, and appends the current row's [timestamp, value] pair if
         * non-null.
         * - Updates this.firstValue to the first (oldest) value that belongs to the active frame
         * according to frameIncludesCurrentValue and frameLoBounded flags.
         *
         * <p>State layout in the map value:
         * - index 0: startOffset (relative to memory page address)
         * - index 1: size (number of elements currently in the ring buffer)
         * - index 2: capacity (ring buffer capacity)
         * - index 3: firstIdx (index of the oldest valid element in the buffer)
         *
         * <p>Side effects:
         * - Mutates the provided map (creates entries on demand).
         * - Reads and writes to the shared MemoryARW buffer and may call expandRingBuffer which
         * modifies that buffer and updates memoryDesc.
         * - Updates the instance field this.firstValue.
         *
         * @param record the current input record to process (its timestamp and argument value are used
         *               to update the partition ring buffer and compute the first-value)
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
                long d = arg.getTimestamp(record);
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
                long d = arg.getTimestamp(record);
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
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
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
         * Constructs a FirstNotNullValueOverPartitionRowsFrameFunction for a partitioned, row-based window frame.
         * <p>
         * This function computes the first non-null timestamp value within a sliding row-based frame for each partition.
         *
         * @param rowsLo number of rows preceding the current row that define the lower bound of the frame (may be 0)
         * @param rowsHi number of rows following the current row that define the upper bound of the frame (may be 0)
         * @param arg    the input timestamp argument function supplying values to evaluate for the first non-null entry
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
         * Advance the window computation for the provided record and update the per-partition
         * ring-buffer state to compute the current first (non-null) timestamp value.
         *
         * <p>This method:
         * - Locates or creates the partition map entry for the record.
         * - Initializes a fixed-size circular buffer in MemoryARW when a partition is new.
         * - Maintains per-partition metadata stored in the map:
         * index of the oldest element, native buffer start offset, cached first-not-null index,
         * and a count of appended values (used for unbounded-low frames).
         * - Updates the buffer with the current row's timestamp argument and computes {@code firstValue}
         * according to whether the window frame's lower bound is bounded or unbounded and whether
         * the current row is included in the frame (controlled by {@code frameIncludesCurrentValue}).
         * <p>
         * Behavior details:
         * - For unbounded-low frames: the implementation appends values to the buffer (tracking count),
         * caches the index of the first non-null value, and sets {@code firstValue} to that value
         * only when it falls within the active window; otherwise it emits null (or the current value
         * when applicable).
         * - For bounded-low (row-based) frames: the method scans the active frame region in the circular
         * buffer to find the first non-null timestamp; if none found it may use the current row's value
         * depending on {@code frameIncludesCurrentValue}. The oldest index is advanced and the buffer
         * slot for the evicted position is overwritten with the current timestamp.
         * <p>
         * Side effects:
         * - Mutates map-backed per-partition state (stored longs at specified offsets).
         * - Writes to the MemoryARW buffer for the partition.
         * - Sets the instance field {@code firstValue} to the first timestamp to be emitted for this row
         * (or {@code Numbers.LONG_NULL} when no suitable value exists).
         *
         * @param record the input record for which to advance the window and compute the next value
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

                long d = arg.getTimestamp(record);
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
                long d = arg.getTimestamp(record);
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
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles first_value() ignore nulls over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class FirstNotNullValueOverRangeFrameFunction extends FirstValueOverRangeFrameFunction implements Reopenable, WindowTimestampFunction {
        /**
         * Constructs a FirstNotNullValueOverRangeFrameFunction for a RANGE-based, non-partitioned
         * window that returns the first non-null timestamp within the moving time window.
         * <p>
         * This function maintains an in-memory ring buffer of (timestamp, value) entries bounded by
         * the provided range offsets and uses the specified argument function to read values.
         *
         * @param rangeLo      lower bound of the range frame (inclusive offset, in the same units as timestamps)
         * @param rangeHi      upper bound of the range frame (inclusive offset)
         * @param arg          function that produces the timestamp values to be considered
         * @param timestampIdx index of the designated timestamp column used for ordering
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
         * Advance the sliding range frame with the given input record and update the cached first value.
         *
         * <p>Updates internal ring-buffer state stored in {@code memory} (at {@code startOffset}):
         * it removes entries that fall outside the [timestamp - maxDiff, timestamp + maxDiff] range,
         * appends the current row's value when non-null, resizes the buffer when full, and updates
         * {@code size}, {@code capacity}, {@code firstIdx}, and {@code firstValue} accordingly.</p>
         *
         * <p>Behavior details:
         * - Reads the ordering timestamp from the input {@code record} using {@code timestampIndex}.
         * - If the frame is unbounded on the low side ({@code !frameLoBounded}) and the buffer is non-empty,
         * uses the value at the buffer's first slot as the candidate first value and returns early.
         * - Otherwise, scans the buffer from {@code firstIdx} to drop elements older than {@code maxDiff}
         * relative to the current timestamp and locates the first element that satisfies {@code |timestamp - ts| >= minDiff}
         * to set {@code firstValue}.
         * - Retrieves the current row value via {@code arg.getTimestamp(record)}; if non-null, appends it to the ring buffer,
         * expanding and realigning the underlying memory when capacity is reached.
         * - If no qualifying first value was found during the scan, sets {@code firstValue} to the current row's value
         * when the frame includes the current row, otherwise to {@code Numbers.LONG_NULL}.</p>
         *
         * <p>Side effects:
         * - Mutates the ring-buffer memory region, {@code size}, {@code capacity}, {@code startOffset}, {@code firstIdx},
         * and {@code firstValue}.
         * - May call {@code memory.appendAddressFor(...)} which can reallocate and change the base page address.</p>
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
            long d = arg.getTimestamp(record);
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

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles first_value() ignore nulls over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class FirstNotNullValueOverRowsFrameFunction extends FirstValueOverRowsFrameFunction implements Reopenable, WindowTimestampFunction {
        private long firstNotNullIdx = -1;

        /**
         * Construct a row-based frame implementation that returns the first non-null timestamp value in the frame.
         *
         * @param arg    function that produces the timestamp value to consider
         * @param rowsLo lower bound of the row frame (e.g., preceding offset or unbounded)
         * @param rowsHi upper bound of the row frame (e.g., following offset, current row, or unbounded)
         * @param memory memory buffer used by the frame to retain values for window computation
         */
        public FirstNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        /**
         * Advance the window computation for the given record and update the cached first non-null timestamp.
         *
         * <p>Depending on whether the frame has a bounded lower bound, this method either:
         * <ul>
         *   <li>For unbounded-lower frames: track the first non-null timestamp seen across rows (using
         *       `firstNotNullIdx`, `count` and `buffer`) and set `firstValue` to that timestamp when the
         *       first non-null falls inside the frame; otherwise `firstValue` is set to NULL.</li>
         *   <li>For bounded-lower (row-based) frames: scan the current ring buffer window for the first
         *       non-null timestamp, update `firstNotNullIdx` and `firstValue` accordingly, then write the
         *       current row's timestamp into the ring buffer and advance `loIdx`.</li>
         * </ul>
         * <p>
         * Side effects: updates internal state used by the window function including `firstValue`,
         * `firstNotNullIdx`, `buffer`, `loIdx`, and `count`.</p>
         *
         * @param record current input record used to read the timestamp argument
         */
        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getLong(0);
                    return;
                }

                long d = arg.getTimestamp(record);
                if (firstNotNullIdx == -1 && d != Numbers.LONG_NULL) {
                    firstNotNullIdx = count;
                    buffer.putLong(0, d);
                    this.firstValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                } else {
                    this.firstValue = Numbers.LONG_NULL;
                }
                count++;
            } else {
                long d = arg.getTimestamp(record);
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

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Reinitializes internal state for a new scan/pass.
         * <p>
         * Calls the superclass reopen() and resets the index that tracks the first non-null
         * value in the current frame so the frame search starts from scratch.
         */
        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        /**
         * Reset internal state for reuse.
         * <p>
         * Resets the superclass state and clears the index tracking the first non-null
         * value within the current frame by setting {@code firstNotNullIdx} to -1.
         */
        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        /**
         * Reset the function's read state to the start of processing.
         * <p>
         * Calls the superclass toTop() to reset inherited state and clears the cached
         * index of the first non-null value so the function can be reused from the
         * beginning.
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
         * Create a function that computes the first non-NULL timestamp over an unbounded-rows frame
         * scoped to each partition.
         * <p>
         * This constructor builds a partition-scoped implementation used when the window frame is
         * "UNBOUNDED PRECEDING ... CURRENT ROW" (rows framing) and NULLs are ignored. The instance
         * maintains per-partition state in the provided map.
         *
         * @param map               storage for per-partition state (keys -> first non-NULL value)
         * @param partitionByRecord a record representing the partition key for the current row
         * @param partitionBySink   serializes the partition key into map key form
         * @param arg               the argument function that produces the timestamp value for the current row
         */
        public FirstNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Advances computation for the current input row and ensures the partition's first timestamp is recorded.
         * <p>
         * If a first value for the record's partition already exists in the map, this sets the function's current
         * value to that stored timestamp. If no value exists, it reads the timestamp from the provided record;
         * if that timestamp is non-null it is stored as the partition's first value and becomes the current value;
         * otherwise the current value is set to SQL NULL (Numbers.LONG_NULL).
         *
         * @param record the input record for the current row; used to determine the partition key and to read the timestamp argument
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.findValue();
            if (mapValue != null) {
                this.value = mapValue.getTimestamp(0);
            } else {
                long d = arg.getTimestamp(record);
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
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
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
         * Create a window function that computes the first non-null timestamp value over the entire result set.
         *
         * @param arg the argument expression whose first non-null value will be returned
         */
        public FirstNotNullValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        /**
         * Inspect the given record and, if this function has not yet produced a value,
         * capture the record's timestamp as the first value (if it is not SQL NULL).
         *
         * <p>This is a no-op if a value has already been found. If the record's timestamp
         * equals Numbers.LONG_NULL it is treated as NULL and ignored.</p>
         *
         * @param record the current row record to inspect for a timestamp value
         */
        @Override
        public void computeNext(Record record) {
            if (!found) {
                long d = arg.getTimestamp(record);
                if (d != Numbers.LONG_NULL) {
                    this.value = d;
                    this.found = true;
                }
            }
        }

        /**
         * Indicates this window function requires two passes over the data.
         *
         * @return {@link WindowFunction#TWO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true — this implementation operates in "ignore nulls" mode
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Pass 1: inspect the current record and capture the first non-null timestamp from the argument.
         * <p>
         * If a non-null timestamp is found and no value has been recorded yet, stores it in {@code this.value}
         * and marks {@code this.found} true. Null timestamps (Numbers.LONG_NULL) are ignored.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                long d = arg.getTimestamp(record);
                if (d != Numbers.LONG_NULL) {
                    this.value = d;
                    this.found = true;
                }
            }
        }

        /**
         * Write the stored timestamp value into the output column for the current row during pass 2.
         * <p>
         * This writes the long `value` directly into the memory address returned by the WindowSPI for
         * the given record offset and the function's output column index.
         *
         * @param recordOffset byte offset of the current row's output record in the WindowSPI memory
         * @param record       current input record (not inspected by this implementation)
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function to its initial state for reuse.
         * <p>
         * Calls the superclass reset, clears the 'found' flag, and sets the stored
         * timestamp value to the sentinel LONG_NULL.
         */
        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Numbers.LONG_NULL;
        }

        /**
         * Reset the function to the initial state for a new scan.
         * <p>
         * Calls {@code super.toTop()} and clears internal state by marking no value as found
         * and setting the cached timestamp to {@link Numbers#LONG_NULL}.
         */
        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Numbers.LONG_NULL;
        }
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class FirstValueOverCurrentRowFunction extends BaseWindowFunction implements WindowTimestampFunction {
        private final boolean ignoreNulls;
        private long value;

        /**
         * Constructs a first_value implementation that returns the argument's timestamp for the current row.
         * <p>
         * The instance implements a single-row frame: it emits the argument's value from the current record,
         * honoring the ignoreNulls flag to determine whether NULL values should be treated as absent.
         *
         * @param arg         the input timestamp expression evaluated for the current row
         * @param ignoreNulls if true, NULL argument values are treated as absent (the function will skip/emit null accordingly); if false, NULLs are returned as-is
         */
        FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
        }

        /**
         * Reads the timestamp from the provided record's argument and stores it as the current value.
         *
         * @param record the input record (current row) from which the argument timestamp is retrieved
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getTimestamp(record);
        }

        /**
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of passes this window function requires.
         *
         * @return the pass count; ZERO_PASS indicates no extra passes are required
         */
        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        /**
         * Return the cached timestamp value for this function.
         * <p>
         * The input record is ignored; this function always returns the stored first-value
         * timestamp for the current window state.
         *
         * @param rec ignored
         * @return the stored timestamp value
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
         * Returns whether this function is configured to ignore NULL input values.
         *
         * @return true if NULLs are ignored, false if NULLs are respected
         */
        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        /**
         * Advances the window computation for the given input record and writes the current
         * computed timestamp value into the SPI output column for that record.
         * <p>
         * This method updates internal state by invoking {@code computeNext(record)} and
         * then stores the resulting long timestamp value into the SPI memory at
         * {@code spi.getAddress(recordOffset, columnIndex)}.
         *
         * @param record       the input record to process
         * @param recordOffset the SPI record offset (used to locate the output slot in SPI memory)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles first_value() over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class FirstValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        private long firstValue;

        /**
         * Create a partitioned FirstValue window function instance.
         * <p>
         * The function records and returns the first (earliest) timestamp value seen for each partition.
         *
         * @param map               partition-scoped state map used to store the first value per partition
         * @param partitionByRecord a record object used to extract the partition key from incoming rows
         * @param partitionBySink   a RecordSink used to serialize the partition key into the map
         * @param arg               the argument function that produces the timestamp value to be tracked
         */
        public FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Advance computation for the given record by updating or retrieving the first-value
         * timestamp for the record's partition.
         * <p>
         * If the partition is seen for the first time, reads the timestamp from the argument
         * function and stores it in the partition map; otherwise loads the previously stored
         * partition first-value into the instance field `firstValue`.
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                firstValue = arg.getTimestamp(record);
                value.putLong(0, firstValue);
            } else {
                firstValue = value.getTimestamp(0);
            }
        }

        /**
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Return the cached first value's timestamp for the current window frame.
         * <p>
         * The supplied Record parameter is not consulted; this method always returns
         * the stored `firstValue`.
         *
         * @param rec ignored
         * @return the first-value timestamp as a long
         */
        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Advance internal state for the current input record and write the current first-value
         * timestamp into the window SPI output column.
         * <p>
         * This method updates the function's state for `record` (via {@code computeNext})
         * and stores the resulting timestamp value into the SPI at the provided record slot
         * and the function's output column index.
         *
         * @param record       input record to process
         * @param recordOffset slot/address offset in the WindowSPI where the output should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    // Handles first_value() over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
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
         * Create a partitioned RANGE-frame implementation that maintains a timestamp/value ring buffer
         * for computing the first_value over a moving time window.
         * <p>
         * The constructor configures whether the lower bound is bounded (Long.MIN_VALUE denotes unbounded),
         * precomputes absolute bounds used to limit the ring buffer window, and records whether the frame
         * includes the current row's timestamp (when rangeHi == 0).
         *
         * @param rangeLo           lower bound of the range frame; use Long.MIN_VALUE to indicate unbounded preceding
         * @param rangeHi           upper bound of the range frame; an upper bound of 0 means the frame includes the current row
         * @param memory            MemoryARW instance used to back the ring buffer storage for the frame
         * @param initialBufferSize initial capacity to allocate for the ring buffer
         * @param timestampIdx      column index of the ordering timestamp within the input record (used to compare/expire entries)
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
         * Calls {@code super.close()}, closes the associated memory buffer, and clears the internal free-list.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        /**
         * Advances the window computation for the given input record and updates per-partition
         * in-memory frame state kept in the backing map.
         *
         * <p>Behaviors:
         * - Initializes per-partition ring buffer and first-value state for new partitions.
         * - Evicts elements outside the configured range bounds when the frame is range-bounded.
         * - Appends the current record's timestamp/value pair to the ring buffer, expanding
         * the buffer in memory when full.
         * - Recomputes the frame size and the index of the first (oldest) element in the frame,
         * then updates the cached firstValue (set to Numbers.LONG_NULL when the frame is empty).
         * - Persists updated frame metadata back into the MapValue at indices:
         * 0: frameSize, 1: startOffset, 2: size, 3: capacity, 4: firstIdx.
         * <p>
         * This method mutates shared memory buffers and the provided map value. It may return
         * early when the frame is not lower-bounded and the first value is already known.
         *
         * @param record current input record used to advance the window state
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
            long d = arg.getTimestamp(record);

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
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Return the cached first value's timestamp for the current window frame.
         * <p>
         * The supplied Record parameter is not consulted; this method always returns
         * the stored `firstValue`.
         *
         * @param rec ignored
         * @return the first-value timestamp as a long
         */
        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reinitializes the function for a new execution pass.
         * <p>
         * Resets internal state so memory is allocated lazily on first use and clears the cached
         * first-value sentinel by setting {@code firstValue} to {@link Numbers#LONG_NULL}.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            firstValue = Numbers.LONG_NULL;
        }

        /**
         * Reset the function's internal state and release allocated resources.
         *
         * <p>Performs the superclass reset behavior, closes the associated {@code memory}
         * buffer, and clears the {@code freeList} used for pooled buffer indices.</p>
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        /**
         * Appends a textual plan representation of this window function to the given PlanSink.
         * <p>
         * The produced plan includes the function name and argument, an optional "ignore nulls"
         * clause, and a "partition by ... range between {maxDiff} preceding and {minDiff} preceding|current row"
         * framing description.
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
         * Reset the function's internal state to the beginning/top.
         * <p>
         * Truncates the associated memory buffer and clears the free-list allocator,
         * in addition to performing superclass reset actions.
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
    public static class FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
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
         * Constructs a rows-based, partitioned first_value window function that maintains a sliding
         * row-frame per partition using the provided memory buffer.
         * <p>
         * The constructor computes internal buffering and frame metadata from the row-frame bounds:
         * - If `rowsLo` is bounded (greater than Long.MIN_VALUE) the frame lower bound is considered
         * bounded and `frameSize` is derived from `rowsHi - rowsLo` (adjusting for negative high bounds),
         * while `bufferSize` is set to |rowsLo|.
         * - If `rowsLo` is unbounded (Long.MIN_VALUE) the frame lower bound is unbounded; `frameSize` is
         * set to 1 (the first element entering the frame determines the first value) and `bufferSize`
         * is set to |rowsHi|.
         * - `frameIncludesCurrentValue` is true when `rowsHi == 0`.
         * <p>
         * The instance uses the supplied MemoryARW as the backing storage for the per-partition ring buffer.
         *
         * @param rowsLo lower row offset of the frame (use Long.MIN_VALUE to indicate UNBOUNDED PRECEDING)
         * @param rowsHi upper row offset of the frame (e.g. 0 means current row)
         * @param memory backing MemoryARW used for the frame's ring buffer
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
         * Advances window state for the given record by updating the partition-scoped ring buffer
         * and computing the current first value for the row's frame.
         *
         * <p>The method:
         * <ul>
         *   <li>Looks up or creates a MapValue for the record's partition.</li>
         *   <li>If the partition is new, allocates and initializes a contiguous memory region to
         *       hold up to {@code bufferSize} timestamp slots (initialized to NULL).</li>
         *   <li>Maintains three metadata slots in the MapValue:
         *       index 0 = 0-based index of the oldest buffer slot (loIdx),
         *       index 1 = start offset of the native memory array,
         *       index 2 = current count of values in the buffer.</li>
         *   <li>Computes the first value visible in the current row's frame using the ring buffer
         *       state, {@code frameSize}, {@code frameIncludesCurrentValue}, and {@code frameLoBounded}.</li>
         *   <li>Advances buffer metadata, writes the current row's timestamp into the ring buffer,
         *       and updates the instance field {@code firstValue} with the computed first-timestamp
         *       (or {@code Numbers.LONG_NULL} if none).</li>
         * </ul>
         * <p>
         * Side effects:
         * - Mutates the partition MapValue (metadata slots 0/1/2).
         * - Writes into the underlying memory region for the partition's ring buffer.
         * - Updates the instance field {@code firstValue} visible to callers.
         *
         * @param record current input record used to derive the partition key and timestamp value
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
            long d = arg.getTimestamp(record);

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
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Return the cached first value's timestamp for the current window frame.
         * <p>
         * The supplied Record parameter is not consulted; this method always returns
         * the stored `firstValue`.
         *
         * @param rec ignored
         * @return the first-value timestamp as a long
         */
        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Advance internal state for the current input record and write the current first-value
         * timestamp into the window SPI output column.
         * <p>
         * This method updates the function's state for `record` (via {@code computeNext})
         * and stores the resulting timestamp value into the SPI at the provided record slot
         * and the function's output column index.
         *
         * @param record       input record to process
         * @param recordOffset slot/address offset in the WindowSPI where the output should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Prepare the function for reuse by reopening its resources.
         * <p>
         * Calls the superclass reopen implementation and leaves any large memory
         * buffers unallocated — allocation is deferred until first actual use.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
        }

        /**
         * Reset internal state and release the backing memory buffer.
         *
         * <p>Calls the superclass reset logic, then closes the {@code memory} resource
         * used by this instance so the associated off-heap/storage is freed.</p>
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Write a textual plan description of this window function to the provided PlanSink.
         * <p>
         * The produced plan has the form:
         * "{name}({arg})[ ignore nulls] over (partition by {partition functions} rows between {bufferSize} preceding and {X preceding|current row})"
         * <p>
         * Notes:
         * - Uses the instance's `arg`, `partitionByRecord`, `bufferSize`, `frameSize`, and
         * `frameIncludesCurrentValue` to determine the exact framing clause.
         * - Includes "ignore nulls" when the function is configured to ignore NULL values.
         *
         * @param sink target PlanSink to receive the plan text
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
         * Reset the function's internal state to the start and clear any buffered frame data.
         * <p>
         * Calls the superclass toTop() then truncates the backing memory buffer to release stored frame contents.
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    // Handles first_value() over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowTimestampFunction {
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
         * Constructs a range-framed first_value implementation for timestamp windows.
         * <p>
         * Initializes internal ring-buffer memory and framing parameters derived from the provided
         * inclusive range bounds and configuration. The instance will maintain a circular buffer
         * sized from configuration.getSqlWindowStorePageSize(), track the current frame size and
         * first-element index, and set whether the frame includes the current row when `rangeHi == 0`.
         *
         * @param rangeLo       inclusive lower bound of the time range (may be Long.MIN_VALUE to indicate unbounded)
         * @param rangeHi       inclusive upper bound of the time range (typically 0 to include current row)
         * @param arg           the argument function that produces the timestamp value for each record
         * @param configuration runtime configuration used to size the buffer (not documented as a service)
         * @param timestampIdx  index of the timestamp column within stored records used for range comparisons
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
         * Releases resources held by this function.
         * <p>
         * Delegates to the superclass close implementation and closes the associated
         * MemoryARW buffer.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
        }

        /**
         * Advance the sliding range frame with the current row and update the cached first value.
         *
         * <p>Processes the provided record's timestamp to:
         * - Early-return with the cached first value when the frame is unbounded below and already non-empty.
         * - Evict out-of-range elements from the ring buffer when the lower bound is bounded.
         * - Append the current (timestamp, value) pair into the ring buffer (the value may be null).
         * - Grow and realign the underlying memory buffer when capacity is reached (note: resizing can change
         * the memory base address).
         * - Recompute the frameSize, firstIdx, and firstValue according to minDiff/maxDiff range bounds.
         *
         * <p>Side effects: mutates the instance's ring-buffer-backed state (memory, startOffset, capacity,
         * size, frameSize, firstIdx) and writes the new firstValue (or Numbers.LONG_NULL when the frame
         * contains no qualifying elements).
         *
         * @param record the current input row used to advance the window frame; its timestamp is read
         *               using the function's configured timestampIndex and the associated argument
         *               function (arg.getTimestamp(record))
         */
        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize > 0) {
                firstValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getTimestamp(record);

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
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Return the cached first value's timestamp for the current window frame.
         * <p>
         * The supplied Record parameter is not consulted; this method always returns
         * the stored `firstValue`.
         *
         * @param rec ignored
         * @return the first-value timestamp as a long
         */
        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reinitializes the function's internal state and buffer for a fresh processing pass.
         *
         * <p>Clears the current first-value marker and resets ring-buffer bookkeeping (capacity,
         * start offset, first index, frame size, and element count). Also (re)allocates the backing
         * memory region used for the frame by appending pages for the initial capacity.</p>
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
         * Reset internal state and release the backing memory buffer.
         *
         * <p>Calls the superclass reset logic, then closes the {@code memory} resource
         * used by this instance so the associated off-heap/storage is freed.</p>
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Appends this function's textual plan representation to the given PlanSink.
         * <p>
         * The produced plan fragment has the form:
         * "first_value({arg})[ ignore nulls] over (range between {maxDiff} preceding and {minDiff} preceding|current row)".
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
         * Reset the function's internal state and memory buffer so the instance can be reused
         * from the beginning of a new evaluation.
         * <p>
         * This clears the current first value, restores capacity to the initial capacity,
         * truncates and reinitializes the backing memory region, and resets frame bookkeeping
         * (start offset, indices, and sizes) to their empty defaults.
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
    public static class FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowTimestampFunction {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected long count = 0;
        protected long firstValue;
        protected int loIdx = 0;

        /**
         * Creates a row-based first_value window function configured for a rows-frame.
         * <p>
         * The constructor interprets the frame bounds (rowsLo, rowsHi) and initializes
         * internal buffer sizing and flags used to maintain a sliding row window:
         * - asserts that the special pair (Long.MIN_VALUE, 0) is not used here (use
         * FirstValueOverWholeResultSetFunction for that case).
         * - when rowsLo is bounded (> Long.MIN_VALUE) the buffer keeps values equal to
         * abs(rowsLo) and the logical frame size is computed from rowsLo..rowsHi;
         * otherwise the frame is treated as unbounded below and the buffer size is
         * set to abs(rowsHi).
         * - frameIncludesCurrentValue is set when rowsHi == 0.
         *
         * @param arg    the value expression whose first value inside the frame is computed
         * @param rowsLo lower bound of the row frame (can be Long.MIN_VALUE to indicate unbounded preceding)
         * @param rowsHi upper bound of the row frame (relative to the current row; 0 means current row is included)
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
         * Closes this function, releasing any resources held by the superclass and the internal buffer.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        /**
         * Advance the sliding row-based frame with the given record and update the cached first value.
         *
         * <p>The method reads the record's timestamp and appends it into an internal circular buffer,
         * incrementing the element count (capped at bufferSize) and advancing the buffer start index
         * (loIdx). It also updates the cached {@code firstValue} according to the current frame state:
         * - If the frame is unbounded on the low side and the total count exceeds the capacity available
         * for the frame, the first value is taken from the appropriate position in the circular buffer.
         * - If there are already elements that belong to the frame, the first value is read from the
         * buffer at the computed index.
         * - If the buffer is empty and the frame definition includes the current row, the current record's
         * timestamp becomes the first value.
         * - Otherwise the cached first value is set to {@code LONG_NULL}.
         *
         * <p>Side effects: writes the timestamp into {@code buffer}, advances {@code loIdx}, updates
         * {@code count}, and updates {@code firstValue}.
         */
        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && count > (bufferSize - frameSize)) {
                firstValue = buffer.getLong((loIdx + bufferSize - count) % bufferSize * Long.BYTES);
                return;
            }

            long d = arg.getTimestamp(record);

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
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Return the cached first value's timestamp for the current window frame.
         * <p>
         * The supplied Record parameter is not consulted; this method always returns
         * the stored `firstValue`.
         *
         * @param rec ignored
         * @return the first-value timestamp as a long
         */
        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Advance internal state for the current input record and write the current first-value
         * timestamp into the window SPI output column.
         * <p>
         * This method updates the function's state for `record` (via {@code computeNext})
         * and stores the resulting timestamp value into the SPI at the provided record slot
         * and the function's output column index.
         *
         * @param record       input record to process
         * @param recordOffset slot/address offset in the WindowSPI where the output should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        /**
         * Reset the function's internal state and buffers so it can be reused.
         * <p>
         * Clears the cached first value, resets the buffer start index and element count,
         * and reinitializes the underlying buffer storage via initBuffer().
         */
        @Override
        public void reopen() {
            firstValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        /**
         * Reset the function to its initial state and release per-instance buffer resources.
         *
         * <p>Calls {@code super.reset()}, closes the internal buffer, clears the cached
         * first value (sets it to {@code Numbers.LONG_NULL}), and resets the frame
         * indices/counters ({@code loIdx} and {@code count}) to zero so the instance
         * is ready for reuse.</p>
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
         * Append a textual plan fragment for this window function to the provided sink.
         * <p>
         * The emitted text has the form:
         * `name(arg) [ignore nulls] over ( rows between {bufferSize} preceding and {frame-end} )`
         * where `{frame-end}` is either `current row` when the frame includes the current value,
         * or `{bufferSize + 1 - frameSize} preceding` otherwise.
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
         * Reset the function to its initial state so it can be replayed from the start.
         *
         * <p>This restores superclass state via {@code super.toTop()}, clears the cached
         * first value (sets it to NULL), resets the lowest buffer index, reinitializes
         * the ring buffer, and zeroes the element count.</p>
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
         * Fill the internal buffer with the sentinel long value (Numbers.LONG_NULL) for each slot.
         * <p>
         * The method writes Numbers.LONG_NULL at offsets 0, 8, 16, ... up to (bufferSize-1)*Long.BYTES,
         * effectively marking all buffer slots as empty/null.
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
    static class FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowTimestampFunction {
        protected long value;

        /**
         * Constructs a first_value window function for partitioned, row-based frames that are unbounded
         * in the preceding direction (per-partition unbounded preceding to current row). The function
         * produces the first (earliest) timestamp value per partition, respecting NULL values.
         *
         * @param map               map used to store per-partition state
         * @param partitionByRecord record representing the partition key for the current row
         * @param partitionBySink   sink used to serialize the partition key into the map
         * @param arg               argument function that produces the timestamp value for each row
         */
        public FirstValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Advance state using the supplied input record: determine the partition key for the record,
         * look up (or create) the per-partition map entry, and ensure the partition's first timestamp
         * value is recorded.
         * <p>
         * If the partition entry is new, the method reads the timestamp from `arg` for the current
         * record, stores it into the map entry and updates the instance `value` field. If the entry
         * already exists, it loads the stored timestamp into `value` without modifying the map.
         *
         * @param record the input record whose partition key and candidate timestamp are processed
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                long d = arg.getTimestamp(record);
                mapValue.putLong(0, d);
                value = d;
            } else {
                value = mapValue.getTimestamp(0);
            }
        }

        /**
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Return the cached timestamp value for this function.
         * <p>
         * The input record is ignored; this function always returns the stored first-value
         * timestamp for the current window state.
         *
         * @param rec ignored
         * @return the stored timestamp value
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
         * Advances the window computation for the given input record and writes the current
         * computed timestamp value into the SPI output column for that record.
         * <p>
         * This method updates internal state by invoking {@code computeNext(record)} and
         * then stores the resulting long timestamp value into the SPI memory at
         * {@code spi.getAddress(recordOffset, columnIndex)}.
         *
         * @param record       the input record to process
         * @param recordOffset the SPI record offset (used to locate the output slot in SPI memory)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Writes a textual plan representation of this window function into the given PlanSink.
         * <p>
         * The produced plan has the form:
         * `functionName(arg)[ ignore nulls] over (partition by <partition-exprs> rows between unbounded preceding and current row)`
         * <p>
         * This method emits the function name and argument, appends " ignore nulls" when configured,
         * and includes the partition expressions taken from {@code partitionByRecord} followed by the
         * fixed rows frame "rows between unbounded preceding and current row".
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
    public static class FirstValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowTimestampFunction {
        protected boolean found;
        protected long value = Numbers.LONG_NULL;

        /**
         * Construct a FirstValueOverWholeResultSetFunction that computes the FIRST_VALUE over the entire result set.
         *
         * @param arg function that produces the timestamp values to evaluate
         */
        public FirstValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        /****
         * Capture the first timestamp value from the supplied record.
         *
         * If a value has not yet been recorded for this function instance, reads the timestamp
         * from the provided record using the configured argument function and stores it;
         * subsequent calls do nothing.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            if (!found) {
                this.value = arg.getTimestamp(record);
                this.found = true;
            }
        }

        /**
         * Returns the window function name.
         *
         * @return the function name (\"first_value\")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of evaluation passes required by this window function.
         *
         * @return WindowFunction.ZERO_PASS indicating the function does not require additional evaluation passes
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Returns the cached timestamp value representing the first value for the current context.
         * <p>
         * The provided Record parameter is ignored; the method always returns the stored timestamp.
         *
         * @param rec (ignored) record passed by the caller
         * @return the stored timestamp value (milliseconds since epoch)
         */
        @Override
        public long getTimestamp(Record rec) {
            return this.value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        /**
         * Advances the window computation for the given input record and writes the current
         * computed timestamp value into the SPI output column for that record.
         * <p>
         * This method updates internal state by invoking {@code computeNext(record)} and
         * then stores the resulting long timestamp value into the SPI memory at
         * {@code spi.getAddress(recordOffset, columnIndex)}.
         *
         * @param record       the input record to process
         * @param recordOffset the SPI record offset (used to locate the output slot in SPI memory)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function to its initial state for reuse.
         * <p>
         * Calls the superclass reset, clears the 'found' flag, and sets the stored
         * timestamp value to the sentinel LONG_NULL.
         */
        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Numbers.LONG_NULL;
        }

        /**
         * Reset the function to the initial state for a new scan.
         * <p>
         * Calls {@code super.toTop()} and clears internal state by marking no value as found
         * and setting the cached timestamp to {@link Numbers#LONG_NULL}.
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
        FIRST_VALUE_COLUMN_TYPES.add(ColumnType.TIMESTAMP);
    }
}
