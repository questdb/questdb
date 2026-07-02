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
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

/**
 * Type-agnostic building blocks shared by the last_value window function over DATE and TIMESTAMP
 * arguments. Each window shape has one abstract base here (holding the framing logic and the
 * native-long value buffers), and a thin DATE or TIMESTAMP subclass in
 * {@link LastValueTimestampWindowFunctionFactory} / {@link LastValueDateWindowFunctionFactory} that
 * only adds the per-type accessor. Two of the IGNORE NULLS bases extend their RESPECT NULLS sibling;
 * the rest are independent.
 * <p>
 * {@link #newInstance} is the single dispatcher: the caller passes the per-type constructor references
 * and the dispatcher selects the shape from the window context, so the same control flow serves both
 * DATE and TIMESTAMP arguments.
 */
public class LastValueWindowFunctionFactoryHelper {

    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV;
    public static final ArrayColumnTypes LAST_VALUE_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV;
    public static final String NAME = "last_value";

    static Function newInstance(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            boolean supportNullsDesc,
            PartitionConstructor partitionConstructor,
            PartitionRangeConstructor partitionRangeConstructor,
            PartitionRowsConstructor partitionRowsConstructor,
            RangeConstructor rangeConstructor,
            RowsConstructor rowsConstructor,
            WholeResultSetConstructor wholeResultSetConstructor,
            IncludeCurrentConstructor includeCurrentConstructor,
            IncludeCurrentPartitionRowsConstructor includeCurrentPartitionRowsConstructor,
            PartitionConstructor notNullPartitionConstructor,
            PartitionConstructor notNullUnboundedPartitionRowsConstructor,
            PartitionRangeConstructor notNullPartitionRangeConstructor,
            PartitionRowsConstructor notNullPartitionRowsConstructor,
            WholeResultSetConstructor notNullCurrentRowConstructor,
            WholeResultSetConstructor notNullWholeResultSetConstructor,
            WholeResultSetConstructor notNullUnboundedRowsConstructor,
            RangeConstructor notNullRangeConstructor,
            RowsConstructor notNullRowsConstructor
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc);
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new AbstractWindowFunctionFactory.TimestampNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE,
                    windowContext.getPartitionByRecord(),
                    Numbers.LONG_NULL
            );
        }
        return windowContext.isIgnoreNulls() ?
                generateIgnoreNullsFunction(
                        position,
                        args,
                        configuration,
                        windowContext,
                        notNullPartitionConstructor,
                        notNullUnboundedPartitionRowsConstructor,
                        notNullPartitionRangeConstructor,
                        notNullCurrentRowConstructor,
                        notNullPartitionRowsConstructor,
                        notNullWholeResultSetConstructor,
                        notNullUnboundedRowsConstructor,
                        notNullRangeConstructor,
                        notNullRowsConstructor
                ) :
                generateRespectNullsFunction(
                        position,
                        args,
                        configuration,
                        windowContext,
                        partitionConstructor,
                        includeCurrentPartitionRowsConstructor,
                        partitionRangeConstructor,
                        partitionRowsConstructor,
                        wholeResultSetConstructor,
                        includeCurrentConstructor,
                        rangeConstructor,
                        rowsConstructor
                );
    }

    private static Function generateIgnoreNullsFunction(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            PartitionConstructor notNullPartitionConstructor,
            PartitionConstructor notNullUnboundedPartitionRowsConstructor,
            PartitionRangeConstructor notNullPartitionRangeConstructor,
            WholeResultSetConstructor notNullCurrentRowConstructor,
            PartitionRowsConstructor notNullPartitionRowsConstructor,
            WholeResultSetConstructor notNullWholeResultSetConstructor,
            WholeResultSetConstructor notNullUnboundedRowsConstructor,
            RangeConstructor notNullRangeConstructor,
            RowsConstructor notNullRowsConstructor
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        final boolean liveView = windowContext.isLiveView();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving last over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                LAST_VALUE_COLUMN_TYPES
                        );
                        return notNullPartitionConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0)
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
                                LAST_VALUE_COLUMN_TYPES
                        );
                        // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                        return notNullUnboundedPartitionRowsConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0)
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV
                                        : LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                        // moving last over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return notNullPartitionRangeConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                partitionByKeyTypes,
                                liveView
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
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
                                LAST_VALUE_COLUMN_TYPES
                        );
                        return notNullUnboundedPartitionRowsConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0)
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return notNullCurrentRowConstructor.newFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                LAST_VALUE_COLUMN_TYPES
                        );
                        return notNullPartitionConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0)
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                }
                // between [unbounded | x] preceding and [x preceding | current row] (but not unbounded preceding to current row )
                else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving average over preceding N rows
                        return notNullPartitionRowsConstructor.newFunction(
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
                    return notNullWholeResultSetConstructor.newFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    // if lower bound is unbounded then it's the same as over ()
                    return notNullUnboundedRowsConstructor.newFunction(args.get(0));
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();

                    // last_value() ignore nulls over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return notNullRangeConstructor.newFunction(
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
                    return notNullUnboundedRowsConstructor.newFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return notNullCurrentRowConstructor.newFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return notNullWholeResultSetConstructor.newFunction(args.get(0));
                } // between [unbounded | x] preceding and [y preceding | current row]
                else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                        return notNullRowsConstructor.newFunction(
                                args.get(0),
                                rowsLo,
                                rowsHi,
                                mem
                        );
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private static Function generateRespectNullsFunction(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            PartitionConstructor partitionConstructor,
            IncludeCurrentPartitionRowsConstructor includeCurrentPartitionRowsConstructor,
            PartitionRangeConstructor partitionRangeConstructor,
            PartitionRowsConstructor partitionRowsConstructor,
            WholeResultSetConstructor wholeResultSetConstructor,
            IncludeCurrentConstructor includeCurrentConstructor,
            RangeConstructor rangeConstructor,
            RowsConstructor rowsConstructor
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        final boolean liveView = windowContext.isLiveView();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving last over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                LAST_VALUE_COLUMN_TYPES
                        );
                        return partitionConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0)
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } // between [unbounded preceding | x preceding] and current row
                else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return includeCurrentPartitionRowsConstructor.newFunction(
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
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV
                                        : LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                        // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return partitionRangeConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                partitionByKeyTypes,
                                liveView
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // whole partition
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                LAST_VALUE_COLUMN_TYPES
                        );
                        return partitionConstructor.newFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0)
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } //between [unbounded preceding | x preceding] and current row
                else if (rowsHi == 0) {
                    return includeCurrentPartitionRowsConstructor.newFunction(
                            rowsLo,
                            false,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } //between [unbounded | x] preceding and x preceding
                else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving last over preceding N rows
                        return partitionRowsConstructor.newFunction(
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
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return wholeResultSetConstructor.newFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return includeCurrentConstructor.newFunction(rowsLo, true, args.get(0));
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // last_value() over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return rangeConstructor.newFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return wholeResultSetConstructor.newFunction(args.get(0));
                } else if (rowsHi == 0) {
                    return includeCurrentConstructor.newFunction(rowsLo, false, args.get(0));
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                        return rowsConstructor.newFunction(
                                args.get(0),
                                rowsLo,
                                rowsHi,
                                mem
                        );
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    @FunctionalInterface
    interface IncludeCurrentConstructor {
        WindowFunction newFunction(long rowsLo, boolean frameIncludesCurrentValue, Function arg);
    }

    @FunctionalInterface
    interface IncludeCurrentPartitionRowsConstructor {
        WindowFunction newFunction(long rowsLo, boolean frameIncludesCurrentValue, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg);
    }

    @FunctionalInterface
    interface PartitionConstructor {
        WindowFunction newFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg);
    }

    @FunctionalInterface
    interface PartitionRangeConstructor {
        WindowFunction newFunction(
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
        );
    }

    @FunctionalInterface
    interface PartitionRowsConstructor {
        WindowFunction newFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        );
    }

    @FunctionalInterface
    interface RangeConstructor {
        WindowFunction newFunction(long rangeLo, long rangeHi, Function arg, CairoConfiguration configuration, int timestampIdx);
    }

    @FunctionalInterface
    interface RowsConstructor {
        WindowFunction newFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory);
    }

    @FunctionalInterface
    interface WholeResultSetConstructor {
        WindowFunction newFunction(Function arg);
    }

    // Handles last_value() ignore nulls over (rows between unbounded preceding and current row); there's no partition by.
    abstract static class LastNotNullOverUnboundedRowsFrameBase extends BaseWindowFunction {
        protected long lastValue = Numbers.LONG_NULL;

        /**
         * Create a last_value window function that ignores NULLs for the ROWS frame
         * "UNBOUNDED PRECEDING TO CURRENT ROW".
         *
         * @param arg the function that produces the timestamp values to be considered
         */
        public LastNotNullOverUnboundedRowsFrameBase(Function arg) {
            super(arg);
        }

        /**
         * Process a single input record and update the stored last timestamp.
         * <p>
         * If the function argument's timestamp for the given record is not NULL,
         * updates the internal lastValue to that timestamp. NULL timestamps are ignored.
         *
         * @param record input record from which the timestamp is read
         */
        @Override
        public void computeNext(Record record) {
            long d = readArgValue(record);
            if (d != Numbers.LONG_NULL) {
                lastValue = d;
            }
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Computes the next last_value for the current input record and writes it into the output column.
         * <p>
         * The method updates internal state by calling {@code computeNext(record)} and then writes the
         * current {@code lastValue} (stored as a long timestamp) into the memory address for the row
         * determined by {@code recordOffset} and {@code columnIndex} via the provided {@code WindowSPI}.
         *
         * @param record       the input record to process
         * @param recordOffset memory offset/row address where the output value should be written
         * @param spi          window SPI used to resolve the output address for the row
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reset the function state to its initial condition.
         * <p>
         * Calls the superclass reset then clears the cached last timestamp by setting
         * {@code lastValue} to {@link Numbers#LONG_NULL}.
         */
        @Override
        public void reset() {
            super.reset();
            lastValue = Numbers.LONG_NULL;
        }

        /**
         * Appends a textual execution-plan fragment describing this function.
         * <p>
         * The produced fragment has the form:
         * `last_value(arg) ignore nulls over (rows between unbounded preceding and current row)`.
         */
        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        /**
         * Reset function state for top-of-window processing.
         * <p>
         * Clears any previously stored last value and delegates common reset work to the superclass.
         */
        @Override
        public void toTop() {
            super.toTop();
            lastValue = Numbers.LONG_NULL;
        }
    }

    // handle last_value() ignore nulls (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    abstract static class LastNotNullValueOverCurrentRowBase extends BaseWindowFunction {
        protected long value = Numbers.LONG_NULL;

        /**
         * Creates a window function that returns the last non-null timestamp value for the current row frame.
         * <p>
         * This implementation ignores NULLs and yields the argument's timestamp for the current row (used for
         * frames equivalent to `CURRENT ROW`).
         *
         * @param arg the function providing the timestamp value evaluated for each row
         */
        LastNotNullValueOverCurrentRowBase(Function arg) {
            super(arg);
        }

        /**
         * Reads the timestamp value from the given record and stores it as the next last-value.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = readArgValue(record);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of passes over the data required to compute this window function.
         *
         * @return the pass count (ZERO_PASS)
         */
        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Advance the function's state for the given input record and write the resulting timestamp
         * to the output column at the provided record offset.
         * <p>
         * This computes the next value for the window function (updating internal state) and stores
         * the computed timestamp into the output column slot identified by recordOffset.
         *
         * @param record       the input record used to compute the next value
         * @param recordOffset byte offset of the output row where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles last_value() ignore nulls over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    abstract static class LastNotNullValueOverPartitionBase extends BasePartitionedWindowFunction {
        /**
         * Creates a partition-aware implementation of `last_value(...)` that ignores NULLs.
         *
         * @param arg the input timestamp expression whose last non-NULL value will be tracked per partition
         */
        public LastNotNullValueOverPartitionBase(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that the first pass should scan rows from the end toward the start.
         *
         * @return Pass1ScanDirection.BACKWARD to perform a backward (reverse) pass1 scan
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Returns the number of passes required by this window function.
         *
         * @return {@code WindowFunction.TWO_PASS} indicating the function performs a two-pass computation
         */
        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First-pass handler that records the last non-null timestamp for the current partition if not already present.
         * <p>
         * For the given input record this method:
         * - materializes the partition key from `record`,
         * - looks up the partition in `map`,
         * - if the partition has no stored value and the argument timestamp is not null, stores that timestamp as the partition's last value.
         * <p>
         * This method does not overwrite an existing per-partition value and ignores null timestamps.
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                long d = readArgValue(record);
                if (d != Numbers.LONG_NULL) {
                    MapValue value = key.createValue();
                    value.putTimestamp(0, d);
                }
            }
        }

        /**
         * Second-pass writer for a partitioned last_value aggregation.
         * <p>
         * Looks up the current partition's stored timestamp in the per-partition map and writes it
         * into the output column at the provided recordOffset. If the partition has no stored value,
         * writes Numbers.LONG_NULL.
         * <p>
         * The method updates the partitionByRecord from the supplied record, derives the map key via
         * partitionBySink, and writes the resulting long timestamp directly to the output memory
         * location returned by {@code spi.getAddress(recordOffset, columnIndex)}.
         *
         * @param record       the current input record used to resolve the partition key
         * @param recordOffset byte offset in the output block where the result should be written
         * @param spi          runtime window SPI used to obtain the output memory address
         */
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

    // Handles last_value() ignore nulls over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    abstract static class LastNotNullValueOverPartitionRangeFrameBase extends LastValueOverPartitionRangeFrameBase {

        private final boolean frameIncludesCurrentValue;

        /**
         * Create a partitioned RANGE-frame implementation of LAST_VALUE that ignores NULLs.
         * <p>
         * This constructor initializes per-partition state using the provided Map and memory ring
         * buffer and configures the frame bounds for [rangeLo, rangeHi]. If {@code rangeHi} is
         * zero the frame is considered to include the current row (sets {@code frameIncludesCurrentValue}
         * to true).
         *
         * @param map               per-partition Map used to store partition-specific state
         * @param partitionByRecord record describing the partition key for lookups
         * @param partitionBySink   sink that serializes the partition key into map key memory
         * @param rangeLo           lower bound of the RANGE frame (inclusive offset)
         * @param rangeHi           upper bound of the RANGE frame (inclusive offset); zero means include current row
         * @param arg               argument function that produces the timestamp value to track
         * @param memory            backing memory (resizable ring buffer) for frame storage
         * @param initialBufferSize initial capacity (in entries) for the ring buffer
         * @param timestampIdx      index of the ordering timestamp column within stored entries
         */
        public LastNotNullValueOverPartitionRangeFrameBase(
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
            frameIncludesCurrentValue = rangeHi == 0;
        }

        /**
         * Processes the next input record: updates the per-partition ring buffer and computes the current
         * last_value for the frame.
         *
         * <p>This method advances window state for the partition corresponding to the supplied record:
         * - looks up or creates the partition map entry containing ring-buffer metadata (start offset,
         * size, capacity, first index);
         * - prunes entries outside the frame range based on configured maxDiff/minDiff bounds;
         * - appends the current record (timestamp and value) into the ring buffer when the argument value
         * is not NULL, expanding the buffer if needed;
         * - adjusts the buffer's first index to locate the element that represents the last_value for the
         * current frame and updates the instance field {@code lastValue} accordingly;
         * - writes updated ring-buffer metadata back into the map value.
         *
         * <p>State is stored in off-heap memory referenced by the map value. The memory layout for each
         * partition's buffer is: consecutive pairs of 8-byte timestamp and 8-byte value (record size = 16
         * bytes). The map value fields are:
         * 0 - start offset (relative to base page address), 1 - size, 2 - capacity, 3 - first index.
         *
         * @param record the input record to incorporate into the window state (used to determine
         *               partition key, the ordering timestamp and the argument value)
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
            long d = readArgValue(record);

            if (mapValue.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
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
                        AbstractWindowFunctionFactory.expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
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
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // handles last_value() ignore nulls over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    abstract static class LastNotNullValueOverPartitionRowsFrameBase extends BasePartitionedWindowFunction {
        //number of values we need to keep to compute over frame
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        private final ArrayColumnTypes mapValueTypes;
        // holds fixed-size ring buffers of timestamp values
        private final MemoryARW memory;
        protected long lastValue = Numbers.LONG_NULL;

        /**
         * Creates a partitioned rows-frame function that computes the last non-null timestamp within a ROWS frame.
         *
         * <p>The constructor configures internal frame bookkeeping based on the provided row bounds:
         * if {@code rowsLo} is {@link Long#MIN_VALUE} the frame is treated as unbounded preceding; otherwise
         * the frame lower bound is considered bounded and {@code frameSize} and {@code bufferSize} are
         * derived from the supplied {@code rowsLo} and {@code rowsHi} values. {@code frameIncludesCurrentValue}
         * will be true when {@code rowsHi == 0} (frame ends at the current row).</p>
         *
         * @param map               per-partition map used to store partition-local state
         * @param partitionByRecord virtual record that represents the partition key for the current row
         * @param partitionBySink   sink that serializes partition key fields into the map key
         * @param rowsLo            frame lower bound in ROWS coordinates (use {@code Long.MIN_VALUE} for UNBOUNDED PRECEDING)
         * @param rowsHi            frame upper bound in ROWS coordinates (0 indicates the current row)
         * @param arg               the argument function that produces the timestamp value to be considered
         * @param memory            ring-buffer memory region used to store per-partition frame entries
         */
        public LastNotNullValueOverPartitionRowsFrameBase(
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
            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 3;
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
        }

        /**
         * Advances the per-partition ROWS-frame state for the given record and writes the computed
         * "last value" timestamp into the partition map and underlying ring buffer.
         *
         * <p>For the partition identified by {@code record}, this method:
         * - loads or initializes partition state (buffer start offset, lo index, and last seen value);
         * - reads the current row timestamp from the argument function and updates the in-memory ring buffer;
         * - computes the last non-null timestamp visible in the current frame according to
         * {@code frameIncludesCurrentValue} and {@code frameLoBounded} and stores it into the map;
         * - advances the partition's lo index (oldest element pointer) and persists state back to the map.</p>
         *
         * <p>Side effects: updates the provided partition {@code MapValue} (timestamp, lo index, start offset)
         * and writes the current row timestamp into {@code memory} at the partition's ring-buffer slot.</p>
         *
         * @param record the input record whose partition and timestamp are processed for the next frame step
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            long d = readArgValue(record);

            if (value.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
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
                this.lastValue = value.getTimestamp(0);
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
                        this.lastValue = value.getTimestamp(0);
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
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.getColumnCount();
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void onSnapshotRestoreBegin() {
            super.onSnapshotRestoreBegin();
            memory.truncate();
        }

        /**
         * Computes the next last_value for the current input record and writes it into the output column.
         * <p>
         * The method updates internal state by calling {@code computeNext(record)} and then writes the
         * current {@code lastValue} (stored as a long timestamp) into the memory address for the row
         * determined by {@code recordOffset} and {@code columnIndex} via the provided {@code WindowSPI}.
         *
         * @param record       the input record to process
         * @param recordOffset memory offset/row address where the output value should be written
         * @param spi          window SPI used to resolve the output address for the row
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reopens the function, resetting any transient state from a previous lifecycle.
         * <p>
         * This implementation delegates to the superclass and leaves memory allocation
         * deferred until first use (lazy allocation).
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            tombstoneCount = 0;
        }

        /**
         * Reset function state and release owned memory.
         * <p>
         * Invokes the superclass reset logic and closes the associated MemoryARW instance
         * to free native resources used by this window function.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                final long startOffset = value.getLong(2);
                value.putTimestamp(0, Numbers.LONG_NULL);
                value.putLong(1, 0L);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Numbers.LONG_NULL);
                }
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            final long ringBytes = (long) bufferSize * Long.BYTES;
            final long partitionLastValue = source.getLong(offset);
            offset += Long.BYTES;
            final long loIdx = source.getLong(offset);
            offset += Long.BYTES;
            final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
            for (int i = 0; i < bufferSize; i++) {
                memory.putLong(newStartOffset + (long) i * Long.BYTES, source.getLong(offset));
                offset += Long.BYTES;
            }
            value.putTimestamp(0, partitionLastValue);
            value.putLong(1, loIdx);
            value.putLong(2, newStartOffset);
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
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
        public void snapshotPartitionState(MemoryA sink, MapValue value) {
            sink.putLong(value.getTimestamp(0));
            sink.putLong(value.getLong(1));
            final long startOffset = value.getLong(2);
            for (int i = 0; i < bufferSize; i++) {
                sink.putLong(memory.getLong(startOffset + (long) i * Long.BYTES));
            }
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        /**
         * Appends a human-readable plan entry for this window function to the given PlanSink.
         * <p>
         * The produced text has the form:
         * "{name}({arg}) ignore nulls over (partition by {partition-expr} rows between {bufferSize} preceding and {bound})"
         * where {bound} is "current row" when the frame includes the current row, or
         * "{bufferSize + 1 - frameSize} preceding" otherwise.
         * <p>
         * This is used to describe the function, its argument, the ignore-null behavior,
         * the partitioning expression, and the ROWS frame bounds in execution plans.
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
         * Reset internal state when evaluation moves to the top of the partition/result set.
         * <p>
         * Calls the superclass reset, truncates the backing memory buffer, and clears the
         * cached last timestamp value so the function starts fresh for the next evaluation.
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Numbers.LONG_NULL;
            tombstoneCount = 0;
        }
    }

    // Handles last_value() ignore nulls over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    abstract static class LastNotNullValueOverRangeFrameBase extends LastValueOverRangeFrameBase {

        /**
         * Creates a last_value window function that ignores NULLs for a RANGE frame.
         *
         * @param rangeLo       lower bound of the RANGE frame (in ordering units, may be Long.MIN_VALUE for UNBOUNDED PRECEDING)
         * @param rangeHi       upper bound of the RANGE frame (in ordering units, may be Long.MAX_VALUE for UNBOUNDED FOLLOWING)
         * @param arg           function producing the timestamp value to evaluate
         * @param configuration Cairo configuration used for memory sizing and limits
         * @param timestampIdx  index of the ordering timestamp within the input record (used to compare range bounds)
         */
        public LastNotNullValueOverRangeFrameBase(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        /**
         * Advance the sliding range-frame state for the given record and update the cached last-value.
         *
         * <p>This method:
         * <ul>
         *   <li>Prunes older buffer entries that are outside the upper bound (maxDiff) relative to the
         *       current record timestamp.</li>
         *   <li>Appends the current record's argument timestamp/value pair to the ring buffer if the
         *       argument is not NULL, growing and realigning the buffer when capacity is exhausted.</li>
         *   <li>Prunes entries that are not within the lower bound (minDiff) so only the most-recent
         *       element satisfying the frame's range remains as the candidate for LAST_VALUE.</li>
         *   <li>Updates the instance's lastValue to the stored value for the current frame or to
         *       Numbers.LONG_NULL when no candidate exists.</li>
         * </ul>
         *
         * <p>Buffer layout: entries are stored as consecutive pairs of longs (timestamp, value). The
         * implementation uses startOffset, firstIdx, size and capacity to manage a circular buffer in the
         * backing MemoryARW; when resized, contents are copied to a new address and firstIdx is adjusted
         * accordingly.
         *
         * @param record input record whose timestamp and argument are used to advance the frame
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
            long d = readArgValue(record);
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
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    // Handles last_value() ignore nulls over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    abstract static class LastNotNullValueOverRowsFrameBase extends BaseWindowFunction implements Reopenable {
        // holds fixed-size ring buffers of timestamp values
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private long cacheValue = Numbers.LONG_NULL;
        protected long lastValue = Numbers.LONG_NULL;
        private int loIdx = 0;

        /**
         * Creates a rows-framed, ignore-null last_value implementation that keeps a sliding
         * ring buffer of recent timestamp values.
         * <p>
         * The constructor computes internal buffer and frame sizes from the ROWS frame bounds:
         * - If rowsLo is finite (not Long.MIN_VALUE), the frame is a bounded window of
         * size (rowsHi - rowsLo + (rowsHi &lt; 0 ? 1 : 0)) and bufferSize is |rowsLo|.
         * * Otherwise the frame is an unbounded-rows preceding frame with frameSize = |rowsHi|
         * and bufferSize = frameSize.
         * It also records whether the frame includes the current row (rowsHi == 0) and
         * initializes the provided MemoryARW as the underlying ring buffer.
         * <p>
         * Precondition: not both rowsLo == Long.MIN_VALUE and rowsHi == 0 (asserted).
         *
         * @param arg    the input function that produces timestamp values for each row
         * @param rowsLo lower bound of the ROWS frame (can be Long.MIN_VALUE for UNBOUNDED PRECEDING)
         * @param rowsHi upper bound of the ROWS frame (0 when the frame includes current row)
         * @param memory preallocated MemoryARW used as the ring buffer storage for timestamps
         */
        public LastNotNullValueOverRowsFrameBase(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
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
         * Releases resources held by this function instance.
         * <p>
         * Calls the superclass {@code close()} and closes the internal {@code buffer}.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        /**
         * Advance the frame by one row and compute the next last_value for the current frame.
         *
         * <p>This reads the timestamp from {@code arg} for the supplied {@code record}, updates
         * internal state used to produce last_value (including {@code lastValue} and {@code cacheValue}),
         * pushes the read timestamp into the circular {@code buffer} at the current {@code loIdx},
         * and advances {@code loIdx} by one.</p>
         *
         * <p>Behavior summary:
         * - If the current row's timestamp is non-null and {@code frameIncludesCurrentValue} is true,
         * the current timestamp becomes the new {@code lastValue}.
         * - Otherwise, when the frame has a lower bound ({@code frameLoBounded}):
         * - Prefer the newest element in the frame (at index {@code loIdx + frameSize - 1}) if non-null.
         * - If that slot is null and {@code lastValue} is null, scan backwards within the frame
         * to find the most recent non-null timestamp and use it as {@code lastValue}.
         * - If a non-null {@code lastValue} already exists, keep it.
         * - If the frame has no lower bound, prefer the element at {@code loIdx}; if that is null,
         * fall back to {@code cacheValue}.
         *
         * <p>After computing {@code lastValue}, the method updates {@code cacheValue} to the computed
         * value, unless that value is the element being evicted from the buffer (in which case
         * {@code cacheValue} is set to NULL to indicate it is invalid). Finally, the current row's
         * timestamp is written into the buffer at {@code loIdx} and {@code loIdx} is incremented
         * modulo {@code bufferSize}.</p>
         *
         * @param record the current input record whose argument timestamp is used to update the window state
         */
        @Override
        public void computeNext(Record record) {
            long d = readArgValue(record);
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
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Computes the next last_value for the current input record and writes it into the output column.
         * <p>
         * The method updates internal state by calling {@code computeNext(record)} and then writes the
         * current {@code lastValue} (stored as a long timestamp) into the memory address for the row
         * determined by {@code recordOffset} and {@code columnIndex} via the provided {@code WindowSPI}.
         *
         * @param record       the input record to process
         * @param recordOffset memory offset/row address where the output value should be written
         * @param spi          window SPI used to resolve the output address for the row
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reopens the function instance by resetting internal state for a new scan.
         * <p>
         * Sets the cached last timestamp to NULL, resets the lowest buffer index and
         * reinitializes the internal ring buffer so the function can be reused safely.
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
         * Calls the superclass reset, closes the internal ring buffer, clears cached and last
         * timestamp values (setting them to NULL), and resets the low index pointer.
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
         * Appends this function's execution-plan fragment to the given PlanSink.
         * <p>
         * The generated fragment has the form:
         * `last_value({arg}) ignore nulls over ( rows between {bufferSize} preceding and {N} preceding )`
         * or when the frame includes the current row:
         * `last_value({arg}) ignore nulls over ( rows between {bufferSize} preceding and current row )`.
         * {bufferSize} is written from the instance field `bufferSize`; the second bound is
         * either "current row" when `frameIncludesCurrentValue` is true, or computed as
         * `bufferSize + 1 - frameSize` otherwise.
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
         * Reset the function's runtime state to the start-of-stream (top) position.
         *
         * <p>Resets the inherited state by calling {@code super.toTop()}, clears the cached
         * last-value and cacheValue markers to SQL NULL, resets the low index used by the
         * ring buffer, and (re)initializes the internal buffer storage.</p>
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
         * Initialize the internal ring buffer by setting each slot to the sentinel `Numbers.LONG_NULL`.
         * <p>
         * Marks `bufferSize` consecutive long slots (at offsets 0, 8, 16, ...) as empty/unset.
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
    abstract static class LastNotNullValueOverUnboundedPartitionRowsFrameBase extends BasePartitionedWindowFunction {
        protected long value = Numbers.LONG_NULL;

        /**
         * Construct a partitioned, ROWS-framed `last_value` implementation that ignores NULLs
         * for windows defined as "UNBOUNDED PRECEDING ... CURRENT ROW".
         *
         * @param map               per-partition state map used to track the last non-null value for each partition
         * @param partitionByRecord record providing the partition key for the current row
         * @param partitionBySink   sink used to write the partition key into the map key
         * @param arg               function that produces the timestamp value evaluated by this window function
         */
        public LastNotNullValueOverUnboundedPartitionRowsFrameBase(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Compute and store the last non-null timestamp for the partition of the supplied record.
         * <p>
         * Looks up (or creates) the partition entry in the map using partitionByRecord and partitionBySink,
         * then reads the argument timestamp for the current record:
         * - If this is a newly created partition entry, the timestamp value (including LONG_NULL) is stored
         * into the map and assigned to the instance field `value`.
         * - If the partition entry already exists and the argument timestamp is not LONG_NULL, the map is
         * updated with the new timestamp and `value` is set to it.
         * - If the partition entry exists and the argument timestamp is LONG_NULL, the previously stored
         * timestamp from the map is assigned to `value` (no map update).
         * <p>
         * Side effects: mutates partitionByRecord, the map (inserting/updating the MapValue), and the
         * instance field `value`.
         */
        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long d = readArgValue(record);
            if (mapValue.isNew()) {
                mapValue.putTimestamp(0, d);
                value = d;
            } else {
                if (d != Numbers.LONG_NULL) {
                    mapValue.putTimestamp(0, d);
                    value = d;
                } else {
                    value = mapValue.getTimestamp(0);
                }
            }
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * Advance the function's state for the given input record and write the resulting timestamp
         * to the output column at the provided record offset.
         * <p>
         * This computes the next value for the window function (updating internal state) and stores
         * the computed timestamp into the output column slot identified by recordOffset.
         *
         * @param record       the input record used to compute the next value
         * @param recordOffset byte offset of the output row where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Append a human-readable execution plan fragment for this function to the given sink.
         * <p>
         * The plan produced has the form:
         * `last_value({arg}) ignore nulls over (partition by {partitionByFunctions} rows between unbounded preceding and current row)`.
         *
         * @param sink destination for the plan text
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
    abstract static class LastNotNullValueOverWholeResultSetBase extends BaseWindowFunction {

        private boolean found;
        protected long value = Numbers.LONG_NULL;

        /**
         * Creates a LastNotNullValueOverWholeResultSetBase that computes the last non-null timestamp
         * across the entire result set (no partitioning, no ordering).
         *
         * @param arg source timestamp function whose non-null values are considered for the last_value result
         */
        public LastNotNullValueOverWholeResultSetBase(Function arg) {
            super(arg);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that the first pass should scan rows from the end toward the start.
         *
         * @return Pass1ScanDirection.BACKWARD to perform a backward (reverse) pass1 scan
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Returns the number of processing passes this function requires.
         *
         * @return {@code TWO_PASS} indicating the function performs two passes over the data
         */
        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        /**
         * Indicates that this window function ignores NULL input values.
         *
         * @return true when NULLs are ignored by the function's computation
         */
        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        /**
         * First pass handler that captures the first non-null timestamp from the input stream.
         * <p>
         * When invoked, if no value has been found yet, reads the timestamp from {@code arg} for
         * the given {@code record}. If the timestamp is not SQL NULL, stores it as the function's
         * value and marks it as found.
         *
         * @param record       the current row record
         * @param recordOffset byte offset of the record in the current data page (unused by this implementation)
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                long d = readArgValue(record);
                if (d != Numbers.LONG_NULL) {
                    found = true;
                    this.value = d;
                }
            }
        }

        /**
         * Writes the function's current timestamp value into the output column for the given row.
         * <p>
         * This method computes the destination address for the output column using the provided
         * WindowSPI and recordOffset, then stores the long timestamp value directly into memory.
         *
         * @param record       the current input record (unused by this implementation but provided by the SPI)
         * @param recordOffset offset of the target output row within the window's memory region
         */
        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset this function's internal state to its initial (pre-computation) values.
         * <p>
         * Calls super.reset() and clears the stored timestamp and found flag so the
         * function behaves as if no rows have been processed.
         */
        @Override
        public void reset() {
            super.reset();
            value = Numbers.LONG_NULL;
            found = false;
        }

        /**
         * Reset this function to the top-of-window state.
         * <p>
         * Calls the superclass implementation and clears the cached last-value state:
         * sets the stored timestamp to `LONG_NULL` and clears the `found` flag so
         * subsequent processing starts fresh for a new window/top context.
         */
        @Override
        public void toTop() {
            super.toTop();
            value = Numbers.LONG_NULL;
            found = false;
        }
    }

    // Handles last_value() over (rows/range between [unbounded preceding x preceding] and current row); there's no partition by.
    abstract static class LastValueIncludeCurrentFrameBase extends BaseWindowFunction {
        private final boolean isRange;
        private final long rowsLo;
        protected long value = Numbers.LONG_NULL;

        /**
         * Creates a LastValueIncludeCurrentFrameBase.
         *
         * @param rowsLo  number of rows (or range lower bound) to look back from the current row; 0 for current-row-only
         * @param isRange true when the frame is a RANGE frame, false when it is a ROWS frame
         * @param arg     the argument function that provides the timestamp values this window function evaluates
         */
        public LastValueIncludeCurrentFrameBase(long rowsLo, boolean isRange, Function arg) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
        }

        /**
         * Reads the timestamp value from the given record and stores it as the next last-value.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = readArgValue(record);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Advance the function's state for the given input record and write the resulting timestamp
         * to the output column at the provided record offset.
         * <p>
         * This computes the next value for the window function (updating internal state) and stores
         * the computed timestamp into the output column slot identified by recordOffset.
         *
         * @param record       the input record used to compute the next value
         * @param recordOffset byte offset of the output row where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset the function's internal state to its initial condition.
         * <p>
         * This clears any stored last-timestamp value and delegates common reset work
         * to the superclass.
         */
        @Override
        public void reset() {
            super.reset();
            value = Numbers.LONG_NULL;
        }

        /**
         * Appends a concise execution-plan fragment for this window function to the given PlanSink.
         * <p>
         * The emitted fragment contains the function name and argument, an optional
         * "ignore nulls" marker, and the window frame clause (either "range between" or
         * "rows between") with the lower bound (numeric or "unbounded") followed by
         * "preceding and current row".
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
         * Reset state for a new top-level pass.
         * <p>
         * Delegates general reset logic to the superclass and clears the stored timestamp value
         * by setting {@code value} to {@link io.questdb.std.Numbers#LONG_NULL}.
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
    abstract static class LastValueIncludeCurrentPartitionRowsFrameBase extends BasePartitionedWindowFunction {
        private final boolean isRange;
        private final long rowsLo;
        protected long value = Numbers.LONG_NULL;

        /**
         * Creates a partitioned last_value implementation for frames that always include the current row.
         * <p>
         * This constructor builds a per-partition function that evaluates `last_value()` where the frame
         * includes the current row (e.g., `... BETWEEN ... AND CURRENT ROW`). It supports both ROWS and
         * RANGE modes.
         *
         * @param rowsLo            number of rows (if ROWS) or range lower bound (if RANGE) relative to the current row;
         *                          represents the frame's lower bound (preceding offset)
         * @param isRange           true when the frame is a RANGE frame, false when it is a ROWS frame
         * @param partitionByRecord record describing the partition key for per-partition state
         * @param partitionBySink   sink used to serialize partition key values
         * @param arg               the argument function producing the timestamp values evaluated by last_value
         */
        public LastValueIncludeCurrentPartitionRowsFrameBase(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
        }

        /**
         * Reads the timestamp value from the given record and stores it as the next last-value.
         *
         * @param record the current input record to read the timestamp from
         */
        @Override
        public void computeNext(Record record) {
            value = readArgValue(record);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Advance the function's state for the given input record and write the resulting timestamp
         * to the output column at the provided record offset.
         * <p>
         * This computes the next value for the window function (updating internal state) and stores
         * the computed timestamp into the output column slot identified by recordOffset.
         *
         * @param record       the input record used to compute the next value
         * @param recordOffset byte offset of the output row where the result should be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Appends a textual execution-plan representation of this function to the given PlanSink.
         * <p>
         * The emitted plan has the form:
         * `name(arg) [ignore nulls] over (partition by {partitionFuncs} [range|rows] between {n|unbounded} preceding and current row)`.
         * If {@code rowsLo == Long.MIN_VALUE} the method prints "unbounded" for the lower bound; otherwise it prints the absolute value of {@code rowsLo}.
         *
         * @param sink the PlanSink to write the plan text into
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
    abstract static class LastValueOverPartitionBase extends BasePartitionedWindowFunction {
        /**
         * Create a partitioned implementation of `last_value(TIMESTAMP)` that keeps the most
         * recent timestamp value for each partition.
         *
         * @param arg function that produces the timestamp value to track (the argument to `last_value`)
         */
        public LastValueOverPartitionBase(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that the first pass should scan rows from the end toward the start.
         *
         * @return Pass1ScanDirection.BACKWARD to perform a backward (reverse) pass1 scan
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Process a single input record for the first pass: update per-partition last-non-null timestamp
         * and write the current partition's last timestamp to the output column.
         * <p>
         * For the record's partition key this method either creates a new map entry with the current
         * argument timestamp (if none existed) or reads the existing stored timestamp. The chosen
         * timestamp is then written to the output address computed from recordOffset and columnIndex.
         *
         * @param record       the input record being processed
         * @param recordOffset output memory offset for the current record where the result timestamp is written
         * @param spi          window service provider used to resolve the output address
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long val;
            if (value.isNew()) {
                long d = readArgValue(record);
                value.putTimestamp(0, d);
                val = d;
            } else {
                val = value.getTimestamp(0);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles last_value() over (partition by x order by ts range between y preceding and z preceding)
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    abstract static class LastValueOverPartitionRangeFrameBase extends BasePartitionedWindowFunction {
        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
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
        protected final AbstractWindowFunctionFactory.RingBufferDesc memoryDesc = new AbstractWindowFunctionFactory.RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected long lastValue = Numbers.LONG_NULL;

        /**
         * Constructs a partitioned RANGE-frame implementation of `last_value` for TIMESTAMP values.
         * <p>
         * This constructor initializes frame bounds and ring-buffer configuration for a per-partition
         * range-based window. It computes internal flags and thresholds from the provided
         * rangeLo/rangeHi bounds and stores references to the partition map, argument function, and
         * backing memory for the ring buffer.
         *
         * @param rangeLo           inclusive lower bound of the RANGE frame relative to the current row's ordering value;
         *                          use Long.MIN_VALUE to indicate unbounded preceding
         * @param rangeHi           inclusive upper bound of the RANGE frame relative to the current row's ordering value
         * @param initialBufferSize initial capacity for the per-partition ring buffer (number of entries)
         * @param timestampIdx      index of the ordering timestamp column within the function's record layout
         */
        public LastValueOverPartitionRangeFrameBase(
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

            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 4;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        /**
         * Releases resources held by this function instance.
         * <p>
         * Calls the superclass close, closes the associated MemoryARW buffer, and clears the internal free list.
         * Safe to call multiple times (idempotent) provided the underlying resources support repeated close.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        /**
         * Advances per-partition ring buffer state for the incoming record and appends the record's
         * timestamp/value pair, maintaining frame bounds and updating the partition's last non-null value.
         * <p>
         * This method:
         * - Loads the partition key from the supplied record and obtains or creates the partition map entry.
         * - Prunes buffered elements that fall outside the RANGE frame defined by minDiff/maxDiff.
         * - Ensures the per-partition ring buffer has capacity (growing it if needed) and appends the
         * current record's `[timestamp, value]` pair.
         * - Updates the stored buffer metadata (start offset, size, capacity, first index) in the map
         * and updates the function's lastValue to the newest in-frame non-null value.
         * <p>
         * Side effects: modifies off-heap ring-buffer memory, may expand that memory, and updates the
         * partition map entry used to persist buffer state across records.
         *
         * @param record the current input record whose partition key, timestamp and value are processed
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
            long d = readArgValue(record);

            if (mapValue.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
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
                    AbstractWindowFunctionFactory.expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
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
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.getColumnCount();
        }

        @Override
        public void onSnapshotRestoreBegin() {
            super.onSnapshotRestoreBegin();
            memory.truncate();
            freeList.clear();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reopens the function for a new processing pass and resets internal state.
         * <p>
         * Calls the superclass reopen() and clears the stored last timestamp (sets it to LONG_NULL)
         * so the function starts with no remembered value.
         */
        @Override
        public void reopen() {
            super.reopen();
            lastValue = Numbers.LONG_NULL;
            tombstoneCount = 0;
        }

        /**
         * Reset the function's internal state to its initial condition.
         * <p>
         * Calls super.reset(), closes the associated memory buffer, and clears the free-list used for buffer reuse.
         */
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
                value.putLong(1, 0L);
                value.putLong(3, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            final long size = source.getLong(offset);
            offset += Long.BYTES;
            final long capacity = Math.max(size, initialBufferSize);
            final long newStartOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            for (long i = 0; i < size; i++) {
                memory.putLong(newStartOffset + i * RECORD_SIZE, source.getLong(offset));
                offset += Long.BYTES;
                memory.putLong(newStartOffset + i * RECORD_SIZE + Long.BYTES, source.getLong(offset));
                offset += Long.BYTES;
            }
            value.putLong(0, newStartOffset);
            value.putLong(1, size);
            value.putLong(2, capacity);
            value.putLong(3, 0L);
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
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
        public void snapshotPartitionState(MemoryA sink, MapValue value) {
            final long startOffset = value.getLong(0);
            final long size = value.getLong(1);
            final long capacity = value.getLong(2);
            final long firstIdx = value.getLong(3);
            sink.putLong(size);
            for (long i = 0; i < size; i++) {
                final long idx = (firstIdx + i) % capacity;
                sink.putLong(memory.getLong(startOffset + idx * RECORD_SIZE));
                sink.putLong(memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES));
            }
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        /**
         * Appends this function's plan representation to the given PlanSink.
         * <p>
         * The fragment includes the function name and argument, an optional
         * "ignore nulls" marker, the PARTITION BY expression, and the RANGE frame
         * bounds (emits "unbounded" when the lower bound is unbounded).
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
         * Reset this function to the top-of-window state.
         *
         * <p>This clears any accumulated frame state so the function behaves as if starting
         * processing a new partition/result set: it delegates to the superclass reset,
         * truncates the backing memory buffer, clears the free-list of recycled slots,
         * and clears the cached last timestamp value.</p>
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Numbers.LONG_NULL;
            tombstoneCount = 0;
        }
    }

    // handles last_value() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    abstract static class LastValueOverPartitionRowsFrameBase extends BasePartitionedWindowFunction {
        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        private final ArrayColumnTypes mapValueTypes;
        // holds fixed-size ring buffers of timestamp values
        private final MemoryARW memory;
        private final long rowLo;
        protected long lastValue = Numbers.LONG_NULL;

        /**
         * Constructs a partitioned ROWS-frame last_value function that uses a per-partition ring buffer.
         * <p>
         * The ring buffer capacity is initialized from the absolute value of the frame's high bound (rowsHi).
         *
         * @param rowsLo the lower bound of the ROWS frame (relative to the current row)
         * @param rowsHi the upper bound of the ROWS frame (relative to the current row); its absolute value
         *               determines the initial ring buffer size
         * @param memory pre-allocated MemoryARW used to back the per-partition ring buffers
         */
        public LastValueOverPartitionRowsFrameBase(
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
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 3;
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
        }

        /**
         * Process the next input record and advance the per-partition rows ring buffer.
         *
         * <p>Looks up or creates the partition entry in {@code map}, initializes a fixed-size
         * ring buffer in {@code memory} on first encounter, then:
         * <ul>
         *   <li>reads the current oldest value from the buffer into {@code lastValue},</li>
         *   <li>advances the oldest index stored in the map (wrapping by {@code bufferSize}),</li>
         *   <li>writes the current argument timestamp into the buffer slot that was just read.</li>
         * </ul>
         *
         * <p>On buffer initialization all slots are filled with {@code Numbers.LONG_NULL}. The method
         * updates the map value fields (index at key 0 and buffer start offset at key 1) and writes
         * into the off-heap {@code memory} region; it does not return a value.
         *
         * @param record source record whose partition key and timestamp argument are used
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
            long d = readArgValue(record);
            if (value.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
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
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.getColumnCount();
        }

        @Override
        public void onSnapshotRestoreBegin() {
            super.onSnapshotRestoreBegin();
            memory.truncate();
        }

        /**
         * Computes the next last_value for the current input record and writes it into the output column.
         * <p>
         * The method updates internal state by calling {@code computeNext(record)} and then writes the
         * current {@code lastValue} (stored as a long timestamp) into the memory address for the row
         * determined by {@code recordOffset} and {@code columnIndex} via the provided {@code WindowSPI}.
         *
         * @param record       the input record to process
         * @param recordOffset memory offset/row address where the output value should be written
         * @param spi          window SPI used to resolve the output address for the row
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Prepare the function for reuse by reopening its state.
         * <p>
         * Calls {@code super.reopen()} and resets the cached last timestamp to {@code Numbers.LONG_NULL}
         * so any per-call memory is allocated lazily on first use.
         */
        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            lastValue = Numbers.LONG_NULL;
            tombstoneCount = 0;
        }

        /**
         * Reset function state and release owned memory.
         * <p>
         * Invokes the superclass reset logic and closes the associated MemoryARW instance
         * to free native resources used by this window function.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
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
                    memory.putLong(startOffset + (long) i * Long.BYTES, Numbers.LONG_NULL);
                }
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            final long ringBytes = (long) bufferSize * Long.BYTES;
            final long loIdx = source.getLong(offset);
            offset += Long.BYTES;
            final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
            for (int i = 0; i < bufferSize; i++) {
                memory.putLong(newStartOffset + (long) i * Long.BYTES, source.getLong(offset));
                offset += Long.BYTES;
            }
            value.putLong(0, loIdx);
            value.putLong(1, newStartOffset);
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
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
        public void snapshotPartitionState(MemoryA sink, MapValue value) {
            sink.putLong(value.getLong(0));
            final long startOffset = value.getLong(1);
            for (int i = 0; i < bufferSize; i++) {
                sink.putLong(memory.getLong(startOffset + (long) i * Long.BYTES));
            }
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        /**
         * Appends a textual plan representation of this window function to the provided PlanSink.
         * <p>
         * The output includes the function name and argument, an optional "ignore nulls" marker,
         * the PARTITION BY expression(s), and the ROWS frame bounds. If the lower bound equals
         * Long.MAX_VALUE it is rendered as "unbounded"; the upper bound is rendered as the
         * configured buffer size in "N preceding" form.
         *
         * @param sink the PlanSink to write the plan representation to
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
            if (rowLo == Long.MIN_VALUE) {
                sink.val("unbounded");
            } else {
                sink.val(Math.abs(rowLo));
            }
            sink.val(" preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        /**
         * Reset internal state when evaluation moves to the top of the partition/result set.
         * <p>
         * Calls the superclass reset, truncates the backing memory buffer, and clears the
         * cached last timestamp value so the function starts fresh for the next evaluation.
         */
        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Numbers.LONG_NULL;
            tombstoneCount = 0;
        }
    }

    // Handles last_value() over ([order by ts] range between x preceding and y preceding ); no partition by key
    abstract static class LastValueOverRangeFrameBase extends BaseWindowFunction implements Reopenable {
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
         * Construct a range-framed last_value window function for TIMESTAMP values.
         * <p>
         * Initializes frame bounds, computes min/max time diffs used to evict out-of-frame entries,
         * and allocates a circular native memory ring buffer sized from the Cairo configuration.
         *
         * @param rangeLo       lower RANGE bound relative to current row (use Long.MIN_VALUE for unbounded)
         * @param rangeHi       upper RANGE bound relative to current row
         * @param arg           argument function that produces the timestamp values
         * @param configuration Cairo configuration used to determine page size and max pages for the buffer
         * @param timestampIdx  index of the timestamp column within stored records (used by range logic)
         */
        public LastValueOverRangeFrameBase(
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
            // memory allocates lazily on reopen(), under the tracker bound by the cursor
            firstIdx = 0;
        }

        /**
         * Releases resources held by this function and its memory buffer.
         * <p>
         * Calls the superclass close logic and then closes the associated MemoryARW to free native memory.
         */
        @Override
        public void close() {
            super.close();
            memory.close();
        }

        /**
         * Process the next input record and update the in-memory ring buffer and aggregator state.
         *
         * <p>This method:
         * <ul>
         *   <li>Reads the current row timestamp and argument value from {@code record}.</li>
         *   <li>Evicts buffered entries whose timestamp is outside the allowed range relative to the current
         *       timestamp (greater than {@code maxDiff} or handled by {@code minDiff} semantics).</li>
         *   <li>Maintains a single entry that satisfies the {@code minDiff} constraint as the candidate
         *       for the "last" value and updates {@code lastValue} accordingly (or sets it to NULL if no
         *       candidate exists).</li>
         *   <li>Ensures the ring buffer has capacity (growing and reordering underlying memory if full),
         *       then appends the current [timestamp, value] pair into the buffer and updates {@code size},
         *       {@code firstIdx}, {@code capacity}, and {@code startOffset} as needed.</li>
         * </ul>
         * <p>
         * The method mutates internal state fields including the ring buffer memory, {@code firstIdx},
         * {@code size}, {@code capacity}, {@code startOffset}, and {@code lastValue}.
         *
         * @param record the current input record from which the timestamp and value are read
         */
        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            long d = readArgValue(record);
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
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reinitializes the function's in-memory ring buffer and state to its startup configuration.
         *
         * <p>Resets the cached last value to SQL NULL, restores the buffer capacity to the initial
         * capacity, allocates (or reattaches) the underlying memory region for that capacity, and
         * resets index/counter fields (first index and element count) so the buffer is empty.</p>
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
         * Reset function state and release owned memory.
         * <p>
         * Invokes the superclass reset logic and closes the associated MemoryARW instance
         * to free native resources used by this window function.
         */
        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
        }

        /**
         * Appends this function's execution-plan fragment to the given PlanSink.
         * <p>
         * The produced text looks like:
         * `last_value({arg}) [ignore nulls] over (range between {maxDiff|unbounded} preceding and {minDiff} preceding)`
         * <p>
         * This describes a RANGE frame with the function argument, optional "ignore nulls" flag,
         * and the lower/upper range bounds (either a numeric maxDiff or "unbounded" for the lower bound,
         * and minDiff for the upper bound).
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
         * Reset the function's transient state to the top of processing.
         * <p>
         * Clears the tracked last value, resets buffer capacity and indices, truncates
         * the backing memory buffer and prepares start offsets so the ring buffer is
         * empty and ready to be reused from the beginning.
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
    abstract static class LastValueOverRowsFrameBase extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        protected long lastValue = Numbers.LONG_NULL;
        private int loIdx = 0;

        /**
         * Constructs a ROWS-frame LastValue function that uses a ring buffer backed by the provided memory.
         *
         * @param arg    the input function that produces timestamp values for each row
         * @param rowsLo lower bound of the ROWS frame (relative to the current row)
         * @param rowsHi upper bound of the ROWS frame (relative to the current row); its absolute value is used to set the buffer capacity
         * @param memory writable memory region used as the ring buffer storage
         */
        public LastValueOverRowsFrameBase(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            initBuffer();
        }

        /**
         * Releases resources held by this function instance.
         * <p>
         * Calls the superclass {@code close()} and closes the internal {@code buffer}.
         */
        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        /**
         * Advance the rows-based ring buffer for the current row.
         * <p>
         * Reads the timestamp currently at the buffer position indicated by {@code loIdx}
         * into the field {@code lastValue}, overwrites that slot with the timestamp
         * produced by {@code readArgValue(record)}, and advances {@code loIdx}
         * (wrapping around by {@code bufferSize}).
         *
         * @param record the current input record whose timestamp is written into the buffer
         */
        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getLong((long) loIdx * Long.BYTES);
            buffer.putLong((long) loIdx * Long.BYTES, readArgValue(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the number of processing passes this window function requires.
         *
         * @return 0 indicating no processing passes are required for this function
         */
        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * Computes the next last_value for the current input record and writes it into the output column.
         * <p>
         * The method updates internal state by calling {@code computeNext(record)} and then writes the
         * current {@code lastValue} (stored as a long timestamp) into the memory address for the row
         * determined by {@code recordOffset} and {@code columnIndex} via the provided {@code WindowSPI}.
         *
         * @param record       the input record to process
         * @param recordOffset memory offset/row address where the output value should be written
         * @param spi          window SPI used to resolve the output address for the row
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        /**
         * Reopens the function instance by resetting internal state for a new scan.
         * <p>
         * Sets the cached last timestamp to NULL, resets the lowest buffer index and
         * reinitializes the internal ring buffer so the function can be reused safely.
         */
        @Override
        public void reopen() {
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
        }

        /**
         * Resets the function's internal state to its initial condition.
         * <p>
         * Calls the superclass reset, closes and releases the ring buffer, clears the cached
         * last value (sets it to SQL NULL), and resets the low index pointer to 0.
         */
        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
        }

        /**
         * Appends a textual execution-plan fragment for this window function to the given sink.
         * <p>
         * The fragment includes the function name and its argument, an optional
         * "ignore nulls" clause, and a ROWS frame description of the form
         * "rows between {lo} preceding and {bufferSize} preceding". If `rowsLo`
         * equals Long.MIN_VALUE it is rendered as "unbounded preceding".
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
                sink.val("unbounded");
            } else {
                sink.val(Math.abs(rowsLo));
            }
            sink.val(" preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        /**
         * Reset internal state to the top of processing so the function can start fresh.
         * <p>
         * Clears the stored last timestamp, resets the low index used by the ring buffer,
         * reinitializes the buffer storage, and delegates common reset work to the superclass.
         */
        @Override
        public void toTop() {
            super.toTop();
            lastValue = Numbers.LONG_NULL;
            loIdx = 0;
            initBuffer();
        }

        /**
         * Initialize the internal ring buffer by setting each slot to the sentinel `Numbers.LONG_NULL`.
         * <p>
         * Marks `bufferSize` consecutive long slots (at offsets 0, 8, 16, ...) as empty/unset.
         */
        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Numbers.LONG_NULL);
            }
        }
    }

    // last_value() over () - empty clause, no partition by no order by, no frame == default frame
    abstract static class LastValueOverWholeResultSetBase extends BaseWindowFunction {
        private boolean found;
        protected long value = Numbers.LONG_NULL;

        /**
         * Creates a LastValueOverWholeResultSetBase that computes `last_value` over the entire result set.
         *
         * @param arg function that produces the TIMESTAMP values to evaluate; its result is used as the candidate values
         */
        public LastValueOverWholeResultSetBase(Function arg) {
            super(arg);
        }

        /**
         * Returns the SQL name of this window function.
         *
         * @return the function name (constant {@link #NAME})
         */
        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Indicates that the first pass should scan rows from the end toward the start.
         *
         * @return Pass1ScanDirection.BACKWARD to perform a backward (reverse) pass1 scan
         */
        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        /**
         * Captures the first non-null timestamp returned by the argument and writes that stored value
         * into the output memory for the given record offset. On the very first call this reads the
         * timestamp from {@code arg} and stores it; on every call it writes the stored timestamp to
         * the SPI-backed output slot.
         *
         * @param record       source record to evaluate
         * @param recordOffset memory offset (as provided by the WindowSPI) where the result must be written
         */
        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = readArgValue(record);
                found = true;
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        /**
         * Reset this function's internal state to its initial (pre-computation) values.
         * <p>
         * Calls super.reset() and clears the stored timestamp and found flag so the
         * function behaves as if no rows have been processed.
         */
        @Override
        public void reset() {
            super.reset();
            value = Numbers.LONG_NULL;
            found = false;
        }

        /**
         * Reset this function to the top-of-window state.
         * <p>
         * Calls the superclass implementation and clears the cached last-value state:
         * sets the stored timestamp to `LONG_NULL` and clears the `found` flag so
         * subsequent processing starts fresh for a new window/top context.
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
        LAST_VALUE_COLUMN_TYPES.add(ColumnType.TIMESTAMP);
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // position of current oldest element
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native array start offset, requires updating on resize
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native buffer size
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // native buffer capacity
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG); // index of last buffered element

        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV = new ArrayColumnTypes();
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // native array start offset
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // native buffer size
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // native buffer capacity
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG); // index of last buffered element
        LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // position of current oldest element
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // start offset of native array
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // count of values in buffer (unused, kept for layout symmetry)
        LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)

        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.TIMESTAMP);
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG); // count of values in buffer

        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.TIMESTAMP); // lastValue
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // position of current oldest element
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG); // start offset of native array
        LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)
    }
}