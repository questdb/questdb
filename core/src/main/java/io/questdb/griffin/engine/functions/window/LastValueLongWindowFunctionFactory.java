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
public class LastValueLongWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES;
    public static final String NAME = "last_value";
    private static final String SIGNATURE = NAME + "(L)";

    /**
     * Returns the function signature identifying this window function.
     *
     * @return the signature string "last_value(L)"
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Create a window-function instance for LAST_VALUE(L) based on the current window context.
     *
     * <p>If the resolved frame is empty (rowsHi &lt; rowsLo) this returns a LongNullFunction that
     * always yields NULL for the frame bounds. Otherwise the method delegates to the factory
     * helpers to produce an implementation that either ignores NULLs or respects NULLs depending
     * on the window context.</p>
     *
     * @param position     parse position used for error reporting
     * @param args         function argument list (first argument is the LONG input)
     * @param argPositions source positions of each argument (used for error reporting)
     * @return a Function implementing last_value over LONG according to the window frame and context
     * @throws SqlException if window context validation fails or configuration is unsupported
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
     * Selects and constructs a Window Function implementation for the `last_value` aggregation
     * when NULLs are ignored, based on the window context (partitioning, framing mode, bounds,
     * ordering) and provided arguments.
     * <p>
     * The method inspects windowContext to determine the exact variant required (partitioned vs.
     * global, RANGE vs. ROWS, unbounded vs. bounded, current-row vs. whole-partition/result-set)
     * and returns a specialized Function instance that implements the semantics of
     * `last_value(... ) IGNORE NULLS` for long inputs.
     *
     * @param position parser/source position used for error reporting when a configuration is not supported
     * @param args     list of function argument expressions (first element is the input value function)
     * @return a Function implementing `last_value(... )` with ignore-null semantics appropriate for the window parameters
     * @throws SqlException if the window parameters request an unsupported configuration (for example RANGE with non-designated timestamp ordering) or if no matching implementation exists
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

    /**
     * Selects and constructs a concrete Window Function implementation for LAST_VALUE(L)
     * that respects NULLs, based on the provided WindowContext (framing mode, partitioning,
     * ordering, and frame bounds).
     * <p>
     * The method returns a specialized Function instance for combinations such as:
     * - partitioned vs. non-partitioned
     * - RANGE vs. ROWS framing
     * - whole-partition / whole-result-set, include-current-row, preceding-N-rows, and range-window variants
     * <p>
     * Implementations created here may allocate per-partition maps or memory-backed circular buffers as needed.
     *
     * @throws SqlException if RANGE framing is used with ordering that is not by the designated timestamp,
     *                      or if no implementation exists for the given window parameters.
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

    /**
     * Returns whether this factory supports NULL ordering directives (e.g. NULLS FIRST / NULLS LAST).
     *
     * @return true if NULL ordering is supported (always true for this factory)
     */
    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    // Handles last_value() ignore nulls over (rows between unbounded preceding and current row); there's no partition by.
    public static class LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowLongFunction {

        private long lastValue = Numbers.LONG_NULL;

        /**
         * Construct a last_value function (ignore NULLs) for an unbounded ROWS frame.
         * <p>
         * The created function returns the most recent non-null long value seen in the
         * partition from the start up to the current row.
         *
         * @param arg input value function that yields long values to evaluate
         */
        public LastNotNullOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        /**
         * Inspect the given record and, if its timestamp is not SQL NULL, update the cached
         * lastValue to that timestamp.
         *
         * @param record the input record whose timestamp is considered
         */
        @Override
        public void computeNext(Record record) {
            long d = arg.getLong(record);
            if (d != Numbers.LONG_NULL) {
                lastValue = d;
            }
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First-pass handler: updates the internal `lastValue` with the given input record
         * and writes that value to the function output column at the provided record offset.
         * <p>
         * The method calls {@code computeNext(record)} to advance internal state (which may
         * change `lastValue`) and then stores `lastValue` into the SPI output slot for this
         * row using the configured `columnIndex`.
         *
         * @param record       the input record used to update the function state
         * @param recordOffset the memory offset/row index supplied by the WindowSPI where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reset the function's runtime state to its initial condition.
         * <p>
         * Clears any cached last value by setting {@code lastValue} to {@link Numbers#LONG_NULL}
         * and invokes superclass reset behavior.
         */
        @Override
        public void reset() {
            super.reset();
            lastValue = Numbers.LONG_NULL;
        }

        /**
         * Appends this window function's plan representation to the provided PlanSink.
         * <p>
         * The emitted plan describes a `last_value` variant that ignores nulls and uses
         * the framing "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", formatted as:
         * `{functionName}({arg}) ignore nulls over (rows between unbounded preceding and current row)`.
         */
        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        /**
         * Reset the function's internal state to the initial position.
         * <p>
         * Clears the cached last value (sets it to Numbers.LONG_NULL) and delegates other reset work to the superclass.
         */
        @Override
        public void toTop() {
            super.toTop();
            lastValue = Numbers.LONG_NULL;
        }
    }

    // handle last_value() ignore nulls (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction implements WindowLongFunction {

        private long value = Numbers.LONG_NULL;

        /**
         * Create a LastNotNullValueOverCurrentRowFunction.
         *
         * @param arg the input function that produces the long value for the current row; this
         *            constructor builds an implementation that returns that value only when it is non-null
         *            (ignore-nulls semantics for the current row).
         */
        LastNotNullValueOverCurrentRowFunction(Function arg) {
            super(arg);
        }

        /**
         * Reads the timestamp value from the given record and stores it in the function's
         * internal `value` field for later use.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getLong(record);
        }

        /**
         * Returns the cached last value for the current row.
         * <p>
         * The supplied Record parameter is not used by this implementation; the method
         * simply returns the stored long value.
         *
         * @param rec ignored
         * @return the cached last long value
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes required by this window function.
         *
         * @return {@code ZERO_PASS} indicating the function requires no separate processing passes
         */
        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First-pass handler that computes and writes the last_value for the current input record.
         * <p>
         * Computes the next aggregated value from the supplied record (updating this function's internal
         * state) and writes the resulting long to the output slot identified by the given recordOffset
         * and this function's columnIndex.
         *
         * @param record       input record used to update the aggregation state
         * @param recordOffset byte offset (address) of the output row where the long result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles last_value() ignore nulls over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    static class LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        /**
         * Creates a partition-aware last_value function that ignores NULLs.
         * <p>
         * This constructor builds an instance that maintains per-partition state in the supplied
         * map and uses the provided partition key record/sink to identify partitions. The function
         * computes the last non-null long value for each partition.
         *
         * @param map               storage map for per-partition state (keys produced by partitionByRecord/sink)
         * @param partitionByRecord record that provides partition key values
         * @param partitionBySink   sink that writes partition key values into map keys
         * @param arg               argument function that produces the input long values
         */
        public LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that pass1 should scan rows in the backward direction.
         *
         * @return {@link Pass1ScanDirection#BACKWARD} to request a backward pass1 scan.
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * The number of execution passes this window function requires.
         * <p>
         * This implementation requires two-pass execution (build intermediate state in pass1, produce output in pass2).
         *
         * @return {@link WindowFunction#TWO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First pass: ensure each partition has an initial entry storing the first non-null timestamp seen.
         * <p>
         * For the given input record this method locates the partition map entry by the partition key.
         * If no entry exists and the record's timestamp (from the function argument) is not NULL,
         * it creates a new map value and stores that timestamp at index 0.
         *
         * @param record       the input record to inspect for partition key and timestamp
         * @param recordOffset unused in this implementation
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
                    value.putTimestamp(0, d);
                }
            }
        }

        /**
         * Emits the stored last-value timestamp for the partition of the given record into the
         * window output column at the provided recordOffset.
         * <p>
         * Looks up the partition key derived from {@code record} in the internal map; if an entry
         * exists, its timestamp is written, otherwise {@code Numbers.LONG_NULL} is written.
         *
         * @param record       the input record whose partition is used to lookup the stored value
         * @param recordOffset the output row offset (used together with {@code columnIndex}) where the value is written
         * @param spi          window SPI used to obtain the output memory address
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

    // Handles last_value() ignore nulls over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class LastNotNullValueOverPartitionRangeFrameFunction extends LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        /**
         * Constructs a partitioned RANGE-frame implementation of LAST_VALUE that ignores NULLs.
         * <p>
         * This constructor creates a function that maintains a per-partition sliding window of
         * (timestamp, value) pairs in off-heap memory and computes the last non-null value
         * within the frame defined by the inclusive bounds [rangeLo, rangeHi] relative to the
         * current row's timestamp.
         *
         * @param rangeLo           lower bound of the RANGE frame (inclusive offset from the current row's timestamp)
         * @param rangeHi           upper bound of the RANGE frame (inclusive offset from the current row's timestamp);
         *                          if {@code rangeHi == 0} the frame includes the current row's value
         * @param arg               the value expression whose long values are being aggregated
         * @param initialBufferSize initial capacity for the per-partition ring buffer (in entries)
         * @param timestampIdx      record column index used as the ordering timestamp for RANGE calculations
         */
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

        /**
         * Processes the next input record and updates the per-partition ring buffer and
         * computed `lastValue` for a RANGE frame (long values).
         *
         * <p>This method:
         * - Looks up or creates per-partition state in the partition map.
         * - Maintains an in-memory ring buffer of [timestamp, value] pairs for the partition,
         * allocating or expanding the buffer in persistent memory as needed.
         * - Evicts entries that fall outside the frame's maxDiff (when the lower bound is
         * bounded), appends the current record's value when non-null, and advances the
         * buffer start so that `lastValue` reflects the newest value inside the frame
         * according to the configured minDiff/maxDiff rules and whether the frame
         * includes the current row.
         * - Writes the updated per-partition metadata (start offset, size, capacity, first index)
         * back to the map value.
         *
         * @param record the input record to process; its timestamp is used for frame arithmetic
         *               and `arg.getTimestamp(record)` provides the long value appended to the buffer
         */
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
            long d = arg.getLong(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Numbers.LONG_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    size = 1;
                    lastValue = frameIncludesCurrentValue ? d : Numbers.LONG_NULL;
                } else {
                    lastValue = Numbers.LONG_NULL;
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
                    lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Numbers.LONG_NULL;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // handles last_value() ignore nulls over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        //number of values we need to keep to compute over frame
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // holds fixed-size ring buffers of timestamp values
        private final MemoryARW memory;
        private long lastValue = Numbers.LONG_NULL;

        /**
         * Constructs a partitioned rows-framed last_value implementation that ignores NULLs.
         *
         * <p>Initializes internal ring-buffer sizing and frame flags based on the ROWS frame bounds.
         * If rowsLo > Long.MIN_VALUE the constructor treats the frame as bounded on the lower side:
         * - frameSize is computed from rowsHi - rowsLo (adjusted when rowsHi is negative).
         * - bufferSize is set to |rowsLo|.
         * Otherwise the frame is treated as unbounded below:
         * - frameSize is set to 1.
         * - bufferSize is set to |rowsHi| (rowsHi == 0 is handled elsewhere).
         * The boolean frameIncludesCurrentValue is true when rowsHi == 0.</p>
         *
         * @param rowsLo lower bound of the ROWS frame (can be Long.MIN_VALUE to indicate unbounded PRECEDING)
         * @param rowsHi upper bound of the ROWS frame
         * @param arg    function providing input values for the window
         * @param memory ring-buffer memory used to store per-partition (timestamp, value) entries
         */
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

        /**
         * Process the next input row for a partitioned ROWS-frame variant, updating per-partition ring-buffer
         * state and the cached last value.
         *
         * <p>Examines or creates the per-partition MapValue, reads the current input timestamp from
         * {@code arg}, and maintains a fixed-size circular buffer in {@code memory} that stores recent
         * timestamps for this partition. The method updates:
         * <ul>
         *   <li>the partition map value with the next low-index (oldest) position and the stored last value
         *       (or {@code Numbers.LONG_NULL} when the last value becomes unavailable),</li>
         *   <li>the ring buffer slot for the current row with the input timestamp, and</li>
         *   <li>the instance field {@code lastValue} to reflect the most-recent visible value according to
         *       frame boundaries and the {@code frameIncludesCurrentValue} flag.</li>
         * </ul>
         *
         * <p>Behavior highlights:
         * <ul>
         *   <li>If the partition entry is new, the buffer region is initialized with {@code LONG_NULL}
         *       and {@code lastValue} is set to the current timestamp only when {@code frameIncludesCurrentValue}
         *       is true and the input timestamp is non-null.</li>
         *   <li>If the partition entry exists, the method prefers the current row's timestamp when
         *       {@code frameIncludesCurrentValue} is true and non-null; otherwise it derives the visible
         *       last value from buffer positions determined by {@code loIdx}, {@code frameSize}, and
         *       {@code frameLoBounded} (scanning backward within the frame when necessary).</li>
         *   <li>When the outgoing oldest buffer element equals the currently visible last value and the
         *       lower bound is bounded, the stored last value is cleared (set to {@code LONG_NULL}) to
         *       indicate it must be recomputed later.</li>
         * </ul>
         *
         * <p>No exceptions are thrown by this method; it has side effects on the partition map, the native
         * buffer memory, and the object's {@code lastValue} field.
         *
         * @param record the current input row to process (provides partition keys and the input timestamp)
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            long d = arg.getLong(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && d != Numbers.LONG_NULL) {
                    this.lastValue = d;
                } else {
                    this.lastValue = Numbers.LONG_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Numbers.LONG_NULL);
                }
            } else {
                this.lastValue = value.getLong(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (d != Numbers.LONG_NULL && frameIncludesCurrentValue) {
                    this.lastValue = d;
                } else if (frameLoBounded) {
                    long last = memory.getLong(startOffset + (loIdx + frameSize - 1) % bufferSize * Long.BYTES);
                    if (last != Numbers.LONG_NULL) {
                        this.lastValue = last;
                    } else if (this.lastValue == Numbers.LONG_NULL) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            long v = memory.getLong(startOffset + (loIdx + i) % bufferSize * Long.BYTES);
                            if (v != Numbers.LONG_NULL) {
                                this.lastValue = v;
                                break;
                            }
                        }
                    } // else keep lastValue
                } else {
                    long last = memory.getLong(startOffset + loIdx % bufferSize * Long.BYTES);
                    if (last != Numbers.LONG_NULL) {
                        this.lastValue = last;
                    } else {
                        this.lastValue = value.getLong(0);
                    }
                }
            }

            long nextLastValue = this.lastValue;
            // set lastValue as invalid
            if (frameLoBounded && memory.getLong(startOffset + loIdx % bufferSize * Long.BYTES) == this.lastValue) {
                nextLastValue = Numbers.LONG_NULL;
            }
            value.putTimestamp(0, nextLastValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);//not necessary because it doesn't change
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First-pass handler: updates the internal `lastValue` with the given input record
         * and writes that value to the function output column at the provided record offset.
         * <p>
         * The method calls {@code computeNext(record)} to advance internal state (which may
         * change `lastValue`) and then stores `lastValue` into the SPI output slot for this
         * row using the configured `columnIndex`.
         *
         * @param record       the input record used to update the function state
         * @param recordOffset the memory offset/row index supplied by the WindowSPI where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reopens the function for reuse, invoking superclass reopen behavior.
         * <p>
         * Any memory-backed resources are not allocated here and will be lazily
         * allocated on first use.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
        }

        /**
         * Resets the function's state and releases its memory buffer.
         * <p>
         * This calls the superclass reset logic and closes the associated memory resource to free any
         * allocated native/storage buffers used by this instance.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Appends this window function's textual plan representation to the given PlanSink.
         * <p>
         * The plan includes the function name and argument, the "ignore nulls" marker,
         * the partition-by expressions, and the ROWS framing clause in the form:
         * "rows between {bufferSize} preceding and {current row|N preceding}".
         */
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

        /**
         * Reset the function to its initial state before reuse.
         *
         * <p>Performs superclass reset, releases/clears any allocated ring-buffer memory, and
         * clears the cached last value by setting it to `Numbers.LONG_NULL`.</p>
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Numbers.LONG_NULL;
        }
    }

    // Handles last_value() ignore nulls over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class LastNotNullValueOverRangeFrameFunction extends LastValueOverRangeFrameFunction {

        /**
         * Create a last_value window function variant that ignores NULLs and evaluates over a RANGE frame.
         *
         * @param rangeLo      lower bound of the RANGE frame (as provided by the window frame specification)
         * @param rangeHi      upper bound of the RANGE frame (as provided by the window frame specification)
         * @param arg          the input value expression (long) whose last non-null value is produced
         * @param timestampIdx index of the timestamp column used to evaluate RANGE boundaries
         */
        public LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        /**
         * Processes a single input record, updating the internal sliding window buffer and computing the current last_value.
         *
         * <p>This method:
         * - Removes entries outside the allowed range based on `maxDiff` (if `frameLoBounded`).
         * - Appends the current record's argument timestamp/value to the ring buffer when the argument is non-null.
         * - Trims older entries that do not satisfy `minDiff` so only the candidate for last_value is kept.
         * - Updates internal buffer state (`firstIdx`, `size`, `capacity`, `startOffset`) and sets `lastValue`
         * to the most recent value within the frame or to `Numbers.LONG_NULL` when none exists.</p>
         *
         * @param record input record; must contain the row timestamp at `timestampIndex` and the argument value retrievable via `arg.getTimestamp(record)`.
         */
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
            long d = arg.getLong(record);
            // add new element
            if (d != Numbers.LONG_NULL) {
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
                this.lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                this.lastValue = Numbers.LONG_NULL;
            }
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles last_value() ignore nulls over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        // holds fixed-size ring buffers of timestamp values
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private long cacheValue = Numbers.LONG_NULL;
        private long lastValue = Numbers.LONG_NULL;
        private int loIdx = 0;

        /**
         * Constructs a rows-framed last_value implementation that ignores NULLs, using a circular memory buffer.
         *
         * <p>rowsLo and rowsHi define the ROWS frame bounds relative to the current row (can be negative
         * for following rows). The constructor computes the window's frame size and the backing buffer
         * capacity based on those bounds and initializes the provided MemoryARW buffer for use.</p>
         *
         * @param arg    argument expression that produces input long values
         * @param rowsLo lower frame bound (relative to current row)
         * @param rowsHi upper frame bound (relative to current row)
         * @param memory preallocated MemoryARW used as the function's ring buffer (must be initialized
         *               by caller and will be prepared by this constructor)
         */
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

        /**
         * Release resources held by this function.
         * <p>
         * Calls the superclass close handler and closes the internal buffer used for window state.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        /**
         * Advances the sliding-row-frame computation by consuming the next input record and updating
         * internal state for the last_value window calculation.
         *
         * <p>The method reads the current row's timestamp from {@code arg} and updates {@code lastValue}
         * and {@code cacheValue} according to the active frame semantics:
         * - If {@code frameIncludesCurrentValue} and the current timestamp is not {@code Numbers.LONG_NULL},
         * the current timestamp becomes the new last value.
         * - If the frame has a lower bound ({@code frameLoBounded}), the value is selected from the
         * frame buffer at the logical end of the frame; if that slot is null, the buffer is scanned
         * backwards within the frame to find the most recent non-null value. If none is found the
         * previous {@code lastValue} is preserved.
         * - If the frame is unbounded below, the method reads the value at the buffer's lowest index
         * and falls back to {@code cacheValue} when that slot is null.
         *
         * <p>After selecting the appropriate last value, the method writes the current timestamp into
         * the ring buffer at the current {@code loIdx} position, advances {@code loIdx}, and updates
         * {@code cacheValue}. If the value at the buffer's lower bound equals the chosen last value,
         * {@code cacheValue} is set to {@code Numbers.LONG_NULL} to indicate that the cached value is
         * no longer valid once that slot is evicted.
         *
         * @param record the input record whose timestamp is used to update the window state
         */
        @Override
        public void computeNext(Record record) {
            long d = arg.getLong(record);
            this.lastValue = this.cacheValue;
            if (d != Numbers.LONG_NULL && frameIncludesCurrentValue) {
                this.lastValue = d;
            } else if (frameLoBounded) {
                long last = buffer.getLong((long) (loIdx + frameSize - 1) % bufferSize * Long.BYTES);
                if (last != Numbers.LONG_NULL) {
                    this.lastValue = last;
                } else if (this.lastValue == Numbers.LONG_NULL) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        long v = buffer.getLong((long) (loIdx + i) % bufferSize * Long.BYTES);
                        if (v != Numbers.LONG_NULL) {
                            this.lastValue = v;
                            break;
                        }
                    }
                } // else keep lastValue
            } else {
                long last = buffer.getLong((long) loIdx % bufferSize * Long.BYTES);
                if (last != Numbers.LONG_NULL) {
                    this.lastValue = last;
                } else {
                    this.lastValue = cacheValue;
                }
            }

            this.cacheValue = this.lastValue;
            // set lastValue as invalid
            if (frameLoBounded && buffer.getLong((long) loIdx % bufferSize * Long.BYTES) == this.lastValue) {
                this.cacheValue = Numbers.LONG_NULL;
            }
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First-pass handler: updates the internal `lastValue` with the given input record
         * and writes that value to the function output column at the provided record offset.
         * <p>
         * The method calls {@code computeNext(record)} to advance internal state (which may
         * change `lastValue`) and then stores `lastValue` into the SPI output slot for this
         * row using the configured `columnIndex`.
         *
         * @param record       the input record used to update the function state
         * @param recordOffset the memory offset/row index supplied by the WindowSPI where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reinitializes the function's mutable state so it can be reused.
         * <p>
         * Resets the cached last value to SQL NULL, resets the low index used by the
         * ring buffer, and (re)initializes the underlying buffer structure.
         */
        @Override
        public void reopen() {
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
        }

        /**
         * Reset the function to its initial state for reuse.
         * <p>
         * Calls super.reset(), closes the internal buffer, clears cached values by
         * setting them to NULL, and resets the low index pointer to 0.
         */
        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Numbers.LONG_NULL;
            cacheValue = Numbers.LONG_NULL;
            loIdx = 0;
        }

        /**
         * Appends a human-readable plan fragment for this window function to the provided PlanSink.
         * <p>
         * The produced fragment has the form:
         * "functionName(arg) ignore nulls over ( rows between {bufferSize} preceding and {bound} )"
         * where {bound} is either "current row" when the frame includes the current row, or
         * "{bufferSize + 1 - frameSize} preceding" otherwise.
         * <p>
         * This method directly writes to the given PlanSink; it does not return a value.
         */
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

        /**
         * Reset this function's runtime state to the initial/top position.
         * <p>
         * Clears the tracked last and cached long values, resets the low index used
         * for the ring buffer, and reinitializes the internal buffer so the function
         * can be reused from the beginning of its input stream.
         */
        @Override
        public void toTop() {
            super.toTop();
            lastValue = Numbers.LONG_NULL;
            cacheValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
        }

        /**
         * Fill the internal buffer with sentinel null values.
         * <p>
         * Initializes each long slot in the backing buffer (at offsets 0, 8, 16, ...) up to
         * bufferSize entries to Numbers.LONG_NULL, marking them as empty/unset.
         */
        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Numbers.LONG_NULL);
            }
        }
    }

    // Handles:
    // - last_value(a) ignore nulls over (partition by x rows between unbounded preceding and [current row | x preceding ])
    // - last_value(a) ignore nulls over (partition by x order by ts range between unbounded preceding and [current row | x preceding])
    public static class LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private long value = Numbers.LONG_NULL;

        /**
         * Construct a function that computes LAST_VALUE for long inputs while ignoring NULLs,
         * over a partitioned ROWS frame that is unbounded preceding and ends at the current row.
         *
         * @param map               per-partition state map used to store and retrieve partition-specific state
         * @param partitionByRecord a record representing the current partition key values
         * @param partitionBySink   a sink that writes partition key values into the map key
         * @param arg               the input function that produces the long value for each row
         */
        public LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Process a single input row: update per-partition state with the row's timestamp
         * and set the instance's current `value` to the latest known timestamp for the
         * partition (ignoring null timestamps).
         * <p>
         * This method:
         * - Looks up or creates the partition entry in `map` using `partitionByRecord`.
         * - Reads the row timestamp via `arg.getTimestamp(record)`.
         * - If the partition entry is new, stores the timestamp and sets `value` to it.
         * - If the entry exists, updates the stored timestamp and `value` when the row's
         * timestamp is not null; otherwise leaves the stored timestamp and `value`
         * unchanged and uses the stored timestamp for `value`.
         *
         * @param record the input row to process (current record for which to compute the window value)
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long d = arg.getLong(record);
            if (mapValue.isNew()) {
                mapValue.putTimestamp(0, d);
                value = d;
            } else {
                if (d != Numbers.LONG_NULL) {
                    mapValue.putTimestamp(0, d);
                    value = d;
                } else {
                    value = mapValue.getLong(0);
                }
            }
        }

        /**
         * Returns the cached last value for the current row.
         * <p>
         * The supplied Record parameter is not used by this implementation; the method
         * simply returns the stored long value.
         *
         * @param rec ignored
         * @return the cached last long value
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First-pass handler that computes and writes the last_value for the current input record.
         * <p>
         * Computes the next aggregated value from the supplied record (updating this function's internal
         * state) and writes the resulting long to the output slot identified by the given recordOffset
         * and this function's columnIndex.
         *
         * @param record       input record used to update the aggregation state
         * @param recordOffset byte offset (address) of the output row where the long result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Appends a textual plan description for this window function to the provided PlanSink.
         * <p>
         * The produced plan has the form:
         * "name(arg) ignore nulls over (partition by {partition-functions} rows between unbounded preceding and current row)".
         * <p>
         * This describes an IGNORE NULLS variant of LAST_VALUE operating over a partitioned ROWS frame
         * that spans from the partition start (UNBOUNDED PRECEDING) to the current row.
         */
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
    public static class LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowLongFunction {
        private boolean found;
        private long value = Numbers.LONG_NULL;

        /**
         * Create a last_value implementation that ignores NULLs and computes the last non-null long
         * across the whole result set (no PARTITION BY / ORDER BY).
         *
         * @param arg expression producing the input long values to consider for the last non-null result
         */
        public LastNotNullValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that pass1 should scan rows in the backward direction.
         *
         * @return {@link Pass1ScanDirection#BACKWARD} to request a backward pass1 scan.
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Indicates this window function requires two processing passes.
         *
         * <p>Two-pass functions perform an initial scan to collect state followed by a second pass
         * that produces the final outputs.</p>
         *
         * @return the pass count constant {@code TWO_PASS}
         */
        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        /**
         * Indicates this window function ignores NULL input values.
         *
         * @return true because this implementation uses the "IGNORE NULLS" semantics
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First pass — capture the first non-null timestamp from the input stream.
         * <p>
         * Reads the timestamp from the provided record and, if a non-null value is encountered
         * and no value has been captured yet, stores it in {@code this.value} and marks {@code found}.
         *
         * @param record       current input record
         * @param recordOffset offset of the current record (unused by this implementation)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                long d = arg.getLong(record);
                if (d != Numbers.LONG_NULL) {
                    found = true;
                    this.value = d;
                }
            }
        }

        /**
         * Write the computed long result into the window output for the given row.
         * <p>
         * Writes the function's current `value` into the output column at the memory address
         * returned by {@code spi.getAddress(recordOffset, columnIndex)} for the provided row.
         *
         * @param record       the input record (unused by this implementation)
         * @param recordOffset offset of the output row within the SPI's address space
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function's internal state to its initial values.
         * <p>
         * Clears any stored last value and marks that no value has been found yet.
         */
        @Override
        public void reset() {
            super.reset();
            value = Numbers.LONG_NULL;
            found = false;
        }

        /**
         * Reset internal state to its initial "no value" condition.
         * <p>
         * Sets the stored long value to NULL and clears the found flag. Called when the function's
         * cursor is repositioned to the start (top) of the input so subsequent processing begins
         * with a clean state.
         */
        @Override
        public void toTop() {
            super.toTop();
            value = Numbers.LONG_NULL;
            found = false;
        }
    }

    // Handles last_value() over (rows/range between [unbounded preceding x preceding] and current row); there's no partition by.
    public static class LastValueIncludeCurrentFrameFunction extends BaseWindowFunction implements WindowLongFunction {

        private final boolean isRange;
        private final long rowsLo;
        private long value = Numbers.LONG_NULL;

        /**
         * Create a last_value implementation that includes the current row in the frame.
         *
         * @param rowsLo  number of preceding rows (offset) that define the frame's lower bound
         * @param isRange true if the frame is RANGE-based, false if ROWS-based
         * @param arg     the input value expression to evaluate for last_value
         */
        public LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
        }

        /**
         * Reads the timestamp value from the given record and stores it in the function's
         * internal `value` field for later use.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getLong(record);
        }

        /**
         * Returns the cached last value for the current row.
         * <p>
         * The supplied Record parameter is not used by this implementation; the method
         * simply returns the stored long value.
         *
         * @param rec ignored
         * @return the cached last long value
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass handler that computes and writes the last_value for the current input record.
         * <p>
         * Computes the next aggregated value from the supplied record (updating this function's internal
         * state) and writes the resulting long to the output slot identified by the given recordOffset
         * and this function's columnIndex.
         *
         * @param record       input record used to update the aggregation state
         * @param recordOffset byte offset (address) of the output row where the long result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset function state for reuse.
         * <p>
         * Calls the superclass reset logic and clears the cached value by setting it to the long NULL sentinel (Numbers.LONG_NULL).
         */
        @Override
        public void reset() {
            super.reset();
            value = Numbers.LONG_NULL;
        }

        /**
         * Appends a concise textual plan representation of this window function to the provided PlanSink.
         * <p>
         * The produced text has the form:
         * {name}({arg})[ ignore nulls] over (range|rows between {unbounded|N} preceding and current row)
         * <p>
         * If {@code rowsLo} equals {@link Long#MIN_VALUE} the lower bound is printed as "unbounded"; otherwise
         * the absolute value of {@code rowsLo} is printed. The method writes directly to the given PlanSink.
         */
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

        /**
         * Reset this function to its initial state for reuse.
         * <p>
         * Calls the superclass reset logic and clears the stored last-value by setting it to
         * Numbers.LONG_NULL.
         */
        @Override
        public void toTop() {
            super.toTop();
            value = Numbers.LONG_NULL;
        }
    }

    // Handles:
    // - last_value(a) over (partition by x rows between [unbounded preceding | x preceding] and current row)
    // - last_value(a) over (partition by x order by ts range between  [unbounded preceding | x preceding] and current row)
    public static class LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private final boolean isRange;
        private final long rowsLo;
        private long value = Numbers.LONG_NULL;

        /**
         * Create a window function that computes LAST_VALUE (including the current row) for a partitioned
         * frame expressed as ROWS or RANGE.
         * <p>
         * This constructor configures a partition-aware implementation that includes the current row
         * in the frame. The function uses the provided partition key record/sink to group rows by
         * partition and evaluates `arg` to obtain the long value for each row.
         *
         * @param rowsLo  lower bound of the frame relative to the current row (e.g., 0 for CURRENT ROW,
         *                N for N PRECEDING); interpretation differs when `isRange` is true (then it
         *                represents a timestamp/range offset)
         * @param isRange true if the frame is RANGE-based (timestamp/difference semantics); false if ROWS-based
         * @param arg     expression that produces the long value to be considered by LAST_VALUE
         */
        public LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
        }

        /**
         * Reads the timestamp value from the given record and stores it in the function's
         * internal `value` field for later use.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = arg.getLong(record);
        }

        /**
         * Returns the cached last value for the current row.
         * <p>
         * The supplied Record parameter is not used by this implementation; the method
         * simply returns the stored long value.
         *
         * @param rec ignored
         * @return the cached last long value
         */
        @Override
        public long getLong(Record rec) {
            return value;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass handler that computes and writes the last_value for the current input record.
         * <p>
         * Computes the next aggregated value from the supplied record (updating this function's internal
         * state) and writes the resulting long to the output slot identified by the given recordOffset
         * and this function's columnIndex.
         *
         * @param record       input record used to update the aggregation state
         * @param recordOffset byte offset (address) of the output row where the long result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Appends a textual representation of this window function to the given PlanSink.
         * <p>
         * The output includes the function name and argument, optional "ignore nulls" marker,
         * the PARTITION BY clause (derived from partitionByRecord), and a ROWS/RANGE frame
         * described as "{n} preceding and current row" (or "unbounded" when rowsLo is Long.MIN_VALUE).
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
    static class LastValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        /**
         * Constructs a partitioned last_value function that tracks the last LONG value per partition.
         * <p>
         * This constructor initializes the function with the per-partition state map, the current
         * partition key record and its sink, and the input argument function whose values are used
         * to compute the last value.
         *
         * @param map               per-partition state map used to store the last-seen long value for each partition
         * @param partitionByRecord record providing the current row's partition key
         * @param partitionBySink   sink used to write/compare partition key values into the map
         * @param arg               input function producing the LONG values to track
         */
        public LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that pass1 should scan rows in the backward direction.
         *
         * @return {@link Pass1ScanDirection#BACKWARD} to request a backward pass1 scan.
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Records the partition's timestamp into the window state buffer for the current row.
         * <p>
         * Performs a per-partition lookup using the current record's partition key; if the
         * partition is new, stores the row's timestamp in the partition map. The chosen
         * timestamp (existing or newly stored) is written as a long into the SPI output
         * buffer at the provided recordOffset and the function's columnIndex.
         *
         * @param record       current input record
         * @param recordOffset offset within the SPI output buffer where the long value will be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long val;
            if (value.isNew()) {
                long d = arg.getLong(record);
                value.putLong(0, d);
                val = d;
            } else {
                val = value.getLong(0);
            }
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles last_value() over (partition by x order by ts range between y preceding and z preceding)
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
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
        protected long lastValue = Numbers.LONG_NULL;

        /**
         * Creates a partitioned RANGE-frame implementation of LAST_VALUE for long values using
         * a memory-backed ring buffer to maintain [timestamp, value] pairs.
         * <p>
         * The constructor configures whether the lower bound is bounded and computes the
         * absolute minimum and maximum allowed timestamp differences (used to evict entries
         * outside the range frame).
         *
         * @param rangeLo           lower bound of the RANGE frame (relative to current row); use Long.MIN_VALUE for unbounded
         * @param rangeHi           upper bound of the RANGE frame (relative to current row)
         * @param memory            memory-backed ring buffer storage for timestamp/value pairs
         * @param initialBufferSize initial capacity for the per-partition ring buffer
         * @param timestampIdx      index within the stored tuple that contains the designated timestamp used for range comparisons
         */
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

        /**
         * Release resources held by this window function.
         * <p>
         * Calls the superclass cleanup, closes the underlying memory buffer, and clears the internal free list.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        /**
         * Processes the next input record for a partitioned RANGE-framed window and updates
         * the per-partition ring-buffer state used to compute the last_value.
         *
         * <p>For the partition of the provided record this method:
         * - looks up or creates the per-partition buffer metadata (start offset, size, capacity, first index);
         * - removes entries outside the frame according to configured minDiff/maxDiff bounds;
         * - computes and updates the partition's current lastValue (or sets it to NULL if none);
         * - appends the current record's [timestamp, value] pair to the ring buffer, expanding the buffer if needed;
         * - persists the updated buffer metadata back into the partition map.</p>
         *
         * @param record the input record whose partition and timestamp/value are used to update the buffer state
         */
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
            long d = arg.getLong(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putLong(startOffset + Long.BYTES, d);
                size = 1;
                lastValue = Numbers.LONG_NULL;
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
                    lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Numbers.LONG_NULL;
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
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reinitializes the function for a new execution pass.
         * <p>
         * Calls the superclass reopen() and resets the cached lastValue to the long null sentinel.
         */
        @Override
        public void reopen() {
            super.reopen();
            lastValue = Numbers.LONG_NULL;
        }

        /**
         * Reset the function's runtime state to its initial (closed) state.
         *
         * <p>This performs superclass reset actions, closes the associated memory buffer,
         * and clears the internal free-list used for buffer slot management.</p>
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        /**
         * Appends this window function's textual plan representation to the provided PlanSink.
         * <p>
         * The emitted plan includes the function name and argument, optional "ignore nulls" marker,
         * the partition-by expression(s), and the RANGE frame specification (preceding bounds).
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
            if (!frameLoBounded) {
                sink.val("unbounded");
            } else {
                sink.val(maxDiff);
            }
            sink.val(" preceding and ");
            sink.val(minDiff).val(" preceding");
            sink.val(')');
        }

        /**
         * Reset the function's runtime state to its initial/top position.
         *
         * <p>Performs superclass reset, truncates the backing memory buffer, clears
         * any cached free-list entries, and resets the cached last value to SQL NULL
         * (Numbers.LONG_NULL).</p>
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Numbers.LONG_NULL;
        }
    }

    // handles last_value() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        // holds fixed-size ring buffers of timestamp values
        private final MemoryARW memory;
        private final long rowLo;
        private long lastValue = Numbers.LONG_NULL;

        /**
         * Construct a partitioned ROWS-frame implementation that tracks the last value within a sliding
         * window of preceding rows using a ring buffer stored in off-heap memory.
         *
         * <p>Buffer capacity is derived from the absolute value of {@code rowsHi}. {@code rowsLo} is
         * stored for use when evaluating whether a row falls into the frame. The provided {@code memory}
         * is used as the backing MemoryARW ring buffer for per-partition frame storage.</p>
         *
         * @param rowsLo lower bound of the ROWS frame (preceding offset or other frame-specific lower bound)
         * @param rowsHi upper bound of the ROWS frame; its absolute value determines the initial ring buffer size
         * @param arg    function producing the input long values to be tracked
         * @param memory off-heap memory (MemoryARW) used as the ring buffer backing store for frame data
         */
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

        /**
         * Advance the per-partition ring buffer with the timestamp from the given record and update the cached last value.
         *
         * <p>This method:
         * - Looks up or creates partition state in the map keyed by the partition record.
         * - On first encounter allocates a contiguous memory region for the partition's circular buffer and initializes it with `Numbers.LONG_NULL`.
         * - Reads the current "oldest" index from partition state, writes the record timestamp into the buffer position pointed to by that index,
         * advances the stored oldest index (modulo buffer size), and updates the in-memory `lastValue` from the overwritten slot before the write.</p>
         *
         * @param record input record whose timestamp is appended into the partition's rows-frame ring buffer
         */
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
            long d = arg.getLong(record);
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
            }

            lastValue = memory.getLong(startOffset + loIdx % bufferSize * Long.BYTES);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putLong(startOffset + loIdx % bufferSize * Long.BYTES, d);
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass handler: updates the internal `lastValue` with the given input record
         * and writes that value to the function output column at the provided record offset.
         * <p>
         * The method calls {@code computeNext(record)} to advance internal state (which may
         * change `lastValue`) and then stores `lastValue` into the SPI output slot for this
         * row using the configured `columnIndex`.
         *
         * @param record       the input record used to update the function state
         * @param recordOffset the memory offset/row index supplied by the WindowSPI where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reopens the function instance for reuse, resetting its transient state.
         * <p>
         * Calls super.reopen(), defers memory allocation until first use, and resets
         * the cached `lastValue` to the sentinel `Numbers.LONG_NULL`.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            lastValue = Numbers.LONG_NULL;
        }

        /**
         * Resets the function's state and releases its memory buffer.
         * <p>
         * This calls the superclass reset logic and closes the associated memory resource to free any
         * allocated native/storage buffers used by this instance.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Appends the query-plan representation of this window function to the given PlanSink.
         * <p>
         * The produced plan includes the function name and argument, an "ignore nulls" marker when
         * applicable, and a "over (partition by ... rows between ... preceding and ... preceding)" frame
         * description. When rowLo equals Long.MAX_VALUE the lower bound is rendered as "unbounded";
         * otherwise the absolute value of rowLo is used.
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
            if (rowLo == Long.MAX_VALUE) {
                sink.val("unbounded");
            } else {
                sink.val(Math.abs(rowLo));
            }
            sink.val(" preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        /**
         * Reset the function to its initial state before reuse.
         *
         * <p>Performs superclass reset, releases/clears any allocated ring-buffer memory, and
         * clears the cached last value by setting it to `Numbers.LONG_NULL`.</p>
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Numbers.LONG_NULL;
        }
    }

    // Handles last_value() over ([order by ts] range between x preceding and y preceding ); no partition by key
    public static class LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
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
        protected long lastValue = Numbers.LONG_NULL;
        protected long size;
        protected long startOffset;

        /**
         * Constructs a range-framed last_value function for long inputs that maintains a sliding
         * window of (timestamp, value) records in a native circular buffer.
         * <p>
         * The frame is defined by [rangeLo, rangeHi] (both inclusive). If `rangeLo` equals
         * Long.MIN_VALUE the lower bound is treated as unbounded. The constructor computes
         * absolute min/max differences used to evict old entries and allocates an initial
         * ring-buffer in native memory sized from the SQL window store page configuration.
         * <p>
         * Internal fields initialized:
         * - frameLoBounded: whether the lower bound is finite.
         * - minDiff / maxDiff: absolute differences derived from the provided bounds.
         * - timestampIndex: index of the timestamp column in the input record.
         * - initialCapacity / capacity: buffer sizing in records.
         * - memory: native CARW memory backing the circular buffer.
         * - startOffset: byte offset of the buffer start relative to the memory page base.
         * - firstIdx: index of the first element in the ring buffer (initialized to 0).
         * <p>
         * Note: This constructor allocates native memory for the ring buffer; callers are
         * responsible for the function's lifecycle methods (reset/close/reopen) for proper
         * resource management.
         *
         * @param rangeLo      lower range bound (use Long.MIN_VALUE for unbounded lower bound)
         * @param rangeHi      upper range bound
         * @param arg          input value function (the value whose "last" is tracked)
         * @param timestampIdx index of the timestamp column in the input record
         */
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

        /**
         * Releases resources held by this function.
         * <p>
         * Invokes the superclass close behavior and closes the associated memory buffer.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
        }

        /**
         * Process the next input record: slide the range-based buffer, evict out-of-range entries,
         * update the current `lastValue`, and append the record's (timestamp, value) pair into the
         * memory-backed ring buffer, expanding and re-aligning the buffer if needed.
         *
         * <p>Behavior details:
         * - Uses the current record's timestamp to remove entries older than `maxDiff` (evict from
         * buffer head).
         * - Collapses entries that are at least `minDiff` away so only one candidate remains at the
         * head used for `lastValue`.
         * - Sets `lastValue` to the value paired with the retained head entry or to `Numbers.LONG_NULL`
         * if the buffer is empty after eviction.
         * - Appends the current record's (timestamp, value) to the tail of the ring buffer, growing
         * the underlying memory region and compacting wrapped contents when capacity is reached.
         *
         * @param record the input record whose timestamp and value are appended and used to adjust the frame
         */
        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getLong(record);
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
                lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Numbers.LONG_NULL;
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
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reinitializes the function's internal sliding buffer to its starting state.
         * <p>
         * Clears the remembered last value and resets the in-memory ring buffer to the
         * configured initial capacity: allocates/advances memory, resets indices
         * (start offset and first index) and size so the function behaves as if newly opened.
         */
        @Override
        public void reopen() {
            lastValue = Numbers.LONG_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        /**
         * Resets the function's state and releases its memory buffer.
         * <p>
         * This calls the superclass reset logic and closes the associated memory resource to free any
         * allocated native/storage buffers used by this instance.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        /**
         * Appends a textual plan fragment for this last_value function to the provided PlanSink.
         * <p>
         * The emitted form is:
         * name(arg)[ ignore nulls] over (range between {maxDiff|unbounded} preceding and {minDiff} preceding)
         * <p>
         * It includes the function name and argument, an optional "ignore nulls" token, and the RANGE frame
         * bounds using `maxDiff` (or "unbounded") and `minDiff`.
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
            if (frameLoBounded) {
                sink.val(maxDiff);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            sink.val(minDiff).val(" preceding");
            sink.val(')');
        }

        /**
         * Reset the function's internal ring-buffer state to its initial empty condition.
         * <p>
         * Clears stored values and metadata so the function behaves as if newly constructed:
         * sets the last observed value to NULL, restores capacity to the initial capacity,
         * truncates the backing memory, recalculates the start offset for the buffer pages,
         * and resets the buffer indices and size to zero.
         */
        @Override
        public void toTop() {
            super.toTop();
            lastValue = Numbers.LONG_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    // Handles last_value() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        private long lastValue = Numbers.LONG_NULL;
        private int loIdx = 0;

        /**
         * Create a rows-framed last_value implementation backed by a ring buffer.
         *
         * @param arg    the value-producing function for each row
         * @param rowsLo lower bound of the ROWS frame (preceding offset)
         * @param rowsHi upper bound of the ROWS frame (following offset). The absolute
         *               value of this parameter determines the internal buffer capacity.
         * @param memory memory-backed ring buffer used to store recent row values for the frame
         */
        public LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            initBuffer();
        }

        /**
         * Release resources held by this function.
         * <p>
         * Calls the superclass close handler and closes the internal buffer used for window state.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        /**
         * Advance the ring buffer by one entry for the given input row.
         * <p>
         * Reads the long value currently stored at the buffer slot indicated by {@code loIdx}
         * into {@code lastValue}, writes the current record's timestamp into that slot,
         * and increments {@code loIdx} (wrapping around {@code bufferSize}).
         *
         * @param record the input row whose timestamp will be written into the buffer
         */
        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getLong((long) loIdx * Long.BYTES);
            buffer.putLong((long) loIdx * Long.BYTES, arg.getLong(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        /**
         * Returns the current last long value tracked by this function for the active row/frame.
         *
         * @param rec record (ignored)
         * @return the most recent non-null value observed for the active partition/frame
         */
        @Override
        public long getLong(Record rec) {
            return lastValue;
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates this window function requires no separate passes.
         *
         * @return the pass count constant {@link WindowFunction#ZERO_PASS}
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * First-pass handler: updates the internal `lastValue` with the given input record
         * and writes that value to the function output column at the provided record offset.
         * <p>
         * The method calls {@code computeNext(record)} to advance internal state (which may
         * change `lastValue`) and then stores `lastValue` into the SPI output slot for this
         * row using the configured `columnIndex`.
         *
         * @param record       the input record used to update the function state
         * @param recordOffset the memory offset/row index supplied by the WindowSPI where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reinitializes the function's mutable state so it can be reused.
         * <p>
         * Resets the cached last value to SQL NULL, resets the low index used by the
         * ring buffer, and (re)initializes the underlying buffer structure.
         */
        @Override
        public void reopen() {
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
        }

        /**
         * Reset the function to its initial state for reuse.
         *
         * <p>Releases and closes the internal buffer, clears the cached last value
         * (sets it to {@code Numbers.LONG_NULL}), resets the sliding window start
         * index to zero, and delegates common reset work to the superclass.</p>
         */
        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
        }

        /**
         * Appends a textual plan representation of this ROWS-framed last_value function to the given PlanSink.
         * <p>
         * The output includes the function name and argument, an optional "ignore nulls" marker,
         * and the ROWS frame specification ("rows between {n|unbounded} preceding and {bufferSize} preceding").
         *
         * @param sink target PlanSink to receive the plan string
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
            if (rowsLo == Long.MIN_VALUE) {
                sink.val(" unbounded preceding and ");
            } else {
                sink.val(Math.abs(rowsLo));
                sink.val(" preceding and ");
            }
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        /**
         * Reset the function's internal state to the initial position.
         *
         * <p>Clears the cached last value, resets the buffer start index, reinitializes the ring
         * buffer and delegates to the superclass {@code toTop()} to reset any inherited state.
         */
        @Override
        public void toTop() {
            super.toTop();
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
        }

        /**
         * Fill the internal buffer with sentinel null values.
         * <p>
         * Initializes each long slot in the backing buffer (at offsets 0, 8, 16, ...) up to
         * bufferSize entries to Numbers.LONG_NULL, marking them as empty/unset.
         */
        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Numbers.LONG_NULL);
            }
        }
    }

    // last_value() over () - empty clause, no partition by no order by, no frame == default frame
    public static class LastValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowLongFunction {
        private boolean found;
        private long value = Numbers.LONG_NULL;

        /**
         * Construct a LastValueOverWholeResultSetFunction for computing LAST_VALUE over the whole result set.
         *
         * @param arg function that produces the long input values (the argument to LAST_VALUE)
         */
        public LastValueOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        /**
         * Returns the canonical name of this window function.
         *
         * @return the function name ("last_value")
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that pass1 should scan rows in the backward direction.
         *
         * @return {@link Pass1ScanDirection#BACKWARD} to request a backward pass1 scan.
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Pass 1 handler for unbounded ROWS frames that ignores NULLs: records the first seen (latest non-null)
         * timestamp value and writes it to the output column for every processed row.
         * <p>
         * This method updates the internal `value` with the timestamp from the current input record the first
         * time a non-null value is encountered, sets `found` to true, and stores `value` into the window
         * output column at the memory location identified by `recordOffset` and `columnIndex`.
         *
         * @param record       the input record being processed
         * @param recordOffset memory offset (row address) in the SPI where the output value should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = arg.getLong(record);
                found = true;
            }
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function's internal state to its initial values.
         * <p>
         * Clears any stored last value and marks that no value has been found yet.
         */
        @Override
        public void reset() {
            super.reset();
            value = Numbers.LONG_NULL;
            found = false;
        }

        /**
         * Reset internal state to its initial "no value" condition.
         * <p>
         * Sets the stored long value to NULL and clears the found flag. Called when the function's
         * cursor is repositioned to the start (top) of the input so subsequent processing begins
         * with a clean state.
         */
        @Override
        public void toTop() {
            super.toTop();
            value = Numbers.LONG_NULL;
            found = false;
        }
    }

    static {
        LAST_VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_VALUE_COLUMN_TYPES.add(ColumnType.LONG);
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
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer
    }
}