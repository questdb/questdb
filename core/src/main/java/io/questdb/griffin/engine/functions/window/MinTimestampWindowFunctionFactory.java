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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class MinTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {
    public static final MaxTimestampWindowFunctionFactory.TimestampComparator LESS_THAN = (a, b) -> a < b;
    public static final String NAME = "min";
    private static final String SIGNATURE = NAME + "(N)";

    /**
     * Returns the function signature provided by this factory.
     *
     * @return the signature string (e.g., {@code "min(N)"})
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Creates a window-function instance that computes the minimum timestamp for the current window context.
     * <p>
     * The concrete implementation is chosen based on the WindowContext (partitioning, framing mode — RANGE or ROWS,
     * ordering, and row bounds). The method may allocate native resources (maps and circular buffers) for stateful
     * implementations and ensures those resources are freed on allocation failure. If the window bounds specify an
     * empty frame (rowsHi &lt; rowsLo) a TimestampNullFunction is returned.
     *
     * @param position parser position of the function call used for error reporting
     * @param args     the function arguments (first argument is the timestamp expression)
     * @return a Function that computes the minimum timestamp for the configured window
     * @throws SqlException if the WindowContext is invalid or the requested combination of framing/ordering/partitioning
     *                      is not supported
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
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord,
                    Numbers.LONG_NULL);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving min over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MaxTimestampWindowFunctionFactory.MAX_COLUMN_TYPES
                        );

                        return new MaxTimestampWindowFunctionFactory.MaxMinOverPartitionFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                LESS_THAN,
                                NAME
                        );
                    } catch (Throwable e) {
                        Misc.free(map);
                        throw e;
                    }
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MaxTimestampWindowFunctionFactory.MAX_COLUMN_TYPES
                        );

                        return new MaxTimestampWindowFunctionFactory.MaxMinOverUnboundedPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                LESS_THAN,
                                NAME
                        );
                    } catch (Throwable e) {
                        Misc.free(map);
                        throw e;
                    }
                } // range between [unbounded | x] preceding and [x preceding | current row], except unbounded preceding to current row
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
                                rowsLo == Long.MIN_VALUE ? MaxTimestampWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES :
                                        MaxTimestampWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES
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

                        // moving min over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new MaxTimestampWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
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
                                LESS_THAN,
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
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                MaxTimestampWindowFunctionFactory.MAX_COLUMN_TYPES
                        );

                        return new MaxTimestampWindowFunctionFactory.MaxMinOverUnboundedPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                LESS_THAN,
                                NAME
                        );
                    } catch (Throwable e) {
                        Misc.free(map);
                        throw e;
                    }
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new MaxTimestampWindowFunctionFactory.MaxMinOverCurrentRowFunction(args.get(0), NAME);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MaxTimestampWindowFunctionFactory.MAX_COLUMN_TYPES
                    );

                    return new MaxTimestampWindowFunctionFactory.MaxMinOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            LESS_THAN,
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
                                rowsLo == Long.MIN_VALUE ? MaxTimestampWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_COLUMN_TYPES :
                                        MaxTimestampWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES
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

                        // moving min over preceding N rows
                        return new MaxTimestampWindowFunctionFactory.MaxMinOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                dequeMem,
                                LESS_THAN,
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
                    return new MaxTimestampWindowFunctionFactory.MaxMinOverWholeResultSetFunction(args.get(0), LESS_THAN, NAME);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new MaxTimestampWindowFunctionFactory.MaxMinOverUnboundedRowsFrameFunction(args.get(0), LESS_THAN, NAME);
                } // range between [unbounded | x] preceding and [x preceding | current row]
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
                        // moving min over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new MaxTimestampWindowFunctionFactory.MaxMinOverRangeFrameFunction(
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                configuration,
                                mem,
                                dequeMem,
                                timestampIndex,
                                LESS_THAN,
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
                    return new MaxTimestampWindowFunctionFactory.MaxMinOverUnboundedRowsFrameFunction(args.get(0), LESS_THAN, NAME);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new MaxTimestampWindowFunctionFactory.MaxMinOverCurrentRowFunction(args.get(0), NAME);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new MaxTimestampWindowFunctionFactory.MaxMinOverWholeResultSetFunction(args.get(0), LESS_THAN, NAME);
                } // between [unbounded | x] preceding and [x preceding | current row]
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
                        return new MaxTimestampWindowFunctionFactory.MaxMinOverRowsFrameFunction(
                                args.get(0),
                                rowsLo,
                                rowsHi,
                                mem,
                                dequeMem,
                                LESS_THAN,
                                NAME
                        );
                    } catch (Throwable e) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw e;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }
}