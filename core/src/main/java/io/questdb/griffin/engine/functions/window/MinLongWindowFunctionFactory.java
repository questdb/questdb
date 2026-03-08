/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \___/\__,_|\___||___/\__|____/|____/
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

public class MinLongWindowFunctionFactory extends AbstractWindowFunctionFactory {
    public static final MaxLongWindowFunctionFactory.LongComparator LESS_THAN = (a, b) -> a < b;
    public static final String NAME = "min";
    private static final String SIGNATURE = NAME + "(L)";

    /**
     * Returns the textual signature that identifies this window function.
     *
     * @return the function signature, "min(L)"
     */
    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    /**
     * Create a window function instance that computes the minimum of long values for the configured window.
     * <p>
     * This method selects and wires one of several specialized implementations based on
     * window framing (ROWS or RANGE), presence of PARTITION BY, ordering/timestamp usage,
     * and the rowsLo/rowsHi bounds. It also allocates and attaches any required map or
     * circular buffer memory resources; allocated resources are freed on error.
     *
     * @param position     position of the function in the SQL statement (used for error reporting)
     * @param args         the function argument list (first argument is the value to aggregate)
     * @param argPositions source positions for each argument (passed through to validation)
     * @return a Function instance that computes the min(L) for the specified window
     * @throws SqlException if the window parameters are not supported (for example, RANGE used
     *                      with a non-designated timestamp ordering) or if the combination of framing/partitioning
     *                      is not implemented
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
                // moving min over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MaxLongWindowFunctionFactory.MAX_COLUMN_TYPES
                    );

                    return new MaxLongWindowFunctionFactory.MaxMinOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            LESS_THAN,
                            NAME
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MaxLongWindowFunctionFactory.MAX_COLUMN_TYPES
                    );

                    return new MaxLongWindowFunctionFactory.MaxMinOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            LESS_THAN,
                            NAME
                    );
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
                                rowsLo == Long.MIN_VALUE ? MaxLongWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES :
                                        MaxLongWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES
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
                        return new MaxLongWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
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
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MaxLongWindowFunctionFactory.MAX_COLUMN_TYPES
                    );

                    return new MaxLongWindowFunctionFactory.MaxMinOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            LESS_THAN,
                            NAME
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new MaxLongWindowFunctionFactory.MaxMinOverCurrentRowFunction(args.get(0), NAME);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            MaxLongWindowFunctionFactory.MAX_COLUMN_TYPES
                    );

                    return new MaxLongWindowFunctionFactory.MaxMinOverPartitionFunction(
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
                                rowsLo == Long.MIN_VALUE ? MaxLongWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_COLUMN_TYPES :
                                        MaxLongWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES
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
                        return new MaxLongWindowFunctionFactory.MaxMinOverPartitionRowsFrameFunction(
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
                    return new MaxLongWindowFunctionFactory.MaxMinOverWholeResultSetFunction(args.get(0), LESS_THAN, NAME);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new MaxLongWindowFunctionFactory.MaxMinOverUnboundedRowsFrameFunction(args.get(0), LESS_THAN, NAME);
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
                        return new MaxLongWindowFunctionFactory.MaxMinOverRangeFrameFunction(
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
                    return new MaxLongWindowFunctionFactory.MaxMinOverUnboundedRowsFrameFunction(args.get(0), LESS_THAN, NAME);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new MaxLongWindowFunctionFactory.MaxMinOverCurrentRowFunction(args.get(0), NAME);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new MaxLongWindowFunctionFactory.MaxMinOverWholeResultSetFunction(args.get(0), LESS_THAN, NAME);
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
                    } catch (Throwable e) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw e;
                    }
                    return new MaxLongWindowFunctionFactory.MaxMinOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem,
                            dequeMem,
                            LESS_THAN,
                            NAME
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }
}