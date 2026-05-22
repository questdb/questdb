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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class MaxDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "max";
    static final Decimal128Comparator GREATER_THAN_128 = (a, b) -> a.compareTo(b) > 0;
    static final Decimal256Comparator GREATER_THAN_256 = (a, b) -> a.compareTo(b) > 0;
    static final Decimal64Comparator GREATER_THAN_64 = (a, b) -> a > b;
    static final Decimal128Comparator LESS_THAN_128 = (a, b) -> a.compareTo(b) < 0;
    static final Decimal256Comparator LESS_THAN_256 = (a, b) -> a.compareTo(b) < 0;
    static final Decimal64Comparator LESS_THAN_64 = (a, b) -> a < b;
    static final ArrayColumnTypes MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL128_OVER_PARTITION_ROWS_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL128_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL256_OVER_PARTITION_ROWS_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL256_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES;
    static final ArrayColumnTypes MAX_DECIMAL64_TYPES;
    private static final String SIGNATURE = NAME + "(Ξ)";

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
        return newMaxMinInstance(this, position, args, configuration, sqlExecutionContext,
                GREATER_THAN_64, GREATER_THAN_128, GREATER_THAN_256, NAME);
    }

    private static Function newMaxMinInstanceDecimal128(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal128Comparator comparator,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL128_TYPES);
                    return new Decimal128MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL128_TYPES);
                    return new Decimal128MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES : MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal128MaxMinOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL128_TYPES);
                    return new Decimal128MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL128_TYPES);
                    return new Decimal128MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL128_OVER_PARTITION_ROWS_TYPES : MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal128MaxMinOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal128MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal128MaxMinOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, mem, dequeMem, timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else {
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal128MaxMinOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, dequeMem, comparator, name, argType);
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

    private static Function newMaxMinInstanceDecimal16(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal64Comparator comparator,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal16MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal16MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES : MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal16MaxMinOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal16MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal16MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES : MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal16MaxMinOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal16MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal16MaxMinOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, mem, dequeMem, timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else {
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal16MaxMinOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, dequeMem, comparator, name, argType);
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

    private static Function newMaxMinInstanceDecimal256(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal256Comparator comparator,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL256_TYPES);
                    return new Decimal256MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL256_TYPES);
                    return new Decimal256MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES : MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal256MaxMinOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL256_TYPES);
                    return new Decimal256MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL256_TYPES);
                    return new Decimal256MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL256_OVER_PARTITION_ROWS_TYPES : MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal256MaxMinOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal256MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal256MaxMinOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, mem, dequeMem, timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else {
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal256MaxMinOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, dequeMem, comparator, name, argType);
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

    private static Function newMaxMinInstanceDecimal32(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal64Comparator comparator,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal32MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal32MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES : MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal32MaxMinOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal32MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal32MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES : MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal32MaxMinOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal32MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal32MaxMinOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, mem, dequeMem, timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else {
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal32MaxMinOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, dequeMem, comparator, name, argType);
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

    private static Function newMaxMinInstanceDecimal64(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal64Comparator comparator,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal64MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal64MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES : MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal64MaxMinOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal64MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal64MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES : MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal64MaxMinOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal64MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal64MaxMinOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, mem, dequeMem, timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else {
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal64MaxMinOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, dequeMem, comparator, name, argType);
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

    private static Function newMaxMinInstanceDecimal8(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal64Comparator comparator,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal8MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal8MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES : MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal8MaxMinOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal8MaxMinOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, MAX_DECIMAL64_TYPES);
                    return new Decimal8MaxMinOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, comparator, name, argType);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                                rowsLo == Long.MIN_VALUE ? MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES : MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal8MaxMinOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, dequeMem, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal8MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal8MaxMinOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, mem, dequeMem, timestampIndex, comparator, name, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        Misc.free(dequeMem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8MaxMinOverUnboundedRowsFrameFunction(arg, comparator, name, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8MaxMinOverCurrentRowFunction(arg, name, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8MaxMinOverWholeResultSetFunction(arg, comparator, name, argType);
                } else {
                    MemoryARW mem = null;
                    MemoryARW dequeMem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        if (rowsLo != Long.MIN_VALUE) {
                            dequeMem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        }
                        return new Decimal8MaxMinOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, dequeMem, comparator, name, argType);
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

    static Function newMaxMinInstance(
            AbstractWindowFunctionFactory factory,
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            Decimal64Comparator comparator,
            Decimal128Comparator comparator128,
            Decimal256Comparator comparator256,
            String name
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, factory.supportNullsDesc());
        int framingMode = windowContext.getFramingMode();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();
        int tag = ColumnType.tagOf(argType);

        if (rowsHi < rowsLo) {
            boolean isRange = framingMode == WindowExpression.FRAMING_RANGE;
            return switch (tag) {
                case ColumnType.DECIMAL8 ->
                        new Decimal8NullFunction(arg, name, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL16 ->
                        new Decimal16NullFunction(arg, name, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL32 ->
                        new Decimal32NullFunction(arg, name, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL64 ->
                        new Decimal64NullFunction(arg, name, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL128 ->
                        new Decimal128NullFunction(arg, name, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                default -> new Decimal256NullFunction(arg, name, rowsLo, rowsHi, isRange, partitionByRecord, argType);
            };
        }

        return switch (tag) {
            case ColumnType.DECIMAL8 ->
                    newMaxMinInstanceDecimal8(position, args, configuration, sqlExecutionContext, comparator, name);
            case ColumnType.DECIMAL16 ->
                    newMaxMinInstanceDecimal16(position, args, configuration, sqlExecutionContext, comparator, name);
            case ColumnType.DECIMAL32 ->
                    newMaxMinInstanceDecimal32(position, args, configuration, sqlExecutionContext, comparator, name);
            case ColumnType.DECIMAL64 ->
                    newMaxMinInstanceDecimal64(position, args, configuration, sqlExecutionContext, comparator, name);
            case ColumnType.DECIMAL128 ->
                    newMaxMinInstanceDecimal128(position, args, configuration, sqlExecutionContext, comparator128, name);
            case ColumnType.DECIMAL256 ->
                    newMaxMinInstanceDecimal256(position, args, configuration, sqlExecutionContext, comparator256, name);
            default ->
                    throw SqlException.$(position, name).put(" is not yet implemented for ").put(ColumnType.nameOf(tag));
        };
    }

    @FunctionalInterface
    public interface Decimal128Comparator {
        boolean isBetter(Decimal128 candidate, Decimal128 current);
    }

    @FunctionalInterface
    public interface Decimal256Comparator {
        boolean isBetter(Decimal256 candidate, Decimal256 current);
    }

    @FunctionalInterface
    public interface Decimal64Comparator {
        boolean isBetter(long a, long b);
    }

    public static class Decimal128MaxMinOverCurrentRowFunction extends BaseWindowFunction {

        private final String name;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128MaxMinOverCurrentRowFunction(Function arg, String name, int type) {
            super(arg);
            this.name = name;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, value);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }
    }

    public static class Decimal128MaxMinOverPartitionFunction extends BasePartitionedWindowFunction {
        private final Decimal128Comparator comparator;
        private final Decimal128 curr = new Decimal128();
        private final String name;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128MaxMinOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal128Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            arg.getDecimal128(record, scratch);
            if (scratch.isNull()) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            if (mv.isNew()) {
                mv.putDecimal128(0, scratch);
            } else {
                mv.getDecimal128(0, curr);
                if (curr.isNull() || isBetter(scratch, curr)) {
                    mv.putDecimal128(0, scratch);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null) {
                mv.getDecimal128(0, scratch);
                Unsafe.putLong(addr, scratch.getHigh());
                Unsafe.putLong(addr + Long.BYTES, scratch.getLow());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {
        private static final int DEQUE_RECORD_SIZE = 16;
        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal128Comparator comparator;
        private final Decimal128 dequeBack = new Decimal128();
        private final LongList dequeFreeList = new LongList();
        private final Decimal128 dequeFront = new Decimal128();
        private final int dequeInitialBufferSize;
        private final MemoryARW dequeMemory;
        private final Decimal128 evicted = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final Decimal128 maxMin = new Decimal128();
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final Decimal128 oldMax = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final int timestampIndex;
        private final int type;
        private final Decimal128 val = new Decimal128();
        private final Decimal128 value = new Decimal128();

        public Decimal128MaxMinOverPartitionRangeFrameFunction(
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
                Decimal128Comparator comparator,
                String name,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            maxMin.ofRawNull();
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
            arg.getDecimal128(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }
                if (!scratch.isNull()) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    if (frameIncludesCurrentValue) {
                        maxMin.copyFrom(scratch);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        maxMin.ofRawNull();
                        frameSize = 0;
                        size = 1;
                    }
                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putDecimal128(dequeStartOffset, scratch.getHigh(), scratch.getLow());
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    maxMin.ofRawNull();
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
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                if (dequeStartIndex != dequeEndIndex) {
                                    dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE, dequeFront);
                                    memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, evicted);
                                    if (dequeFront.compareTo(evicted) == 0) {
                                        dequeStartIndex++;
                                    }
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

                if (!scratch.isNull()) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putDecimal128(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, value);
                            while (dequeStartIndex != dequeEndIndex) {
                                dequeMemory.getDecimal128(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE, dequeBack);
                                if (isBetter(value, dequeBack)) {
                                    dequeEndIndex--;
                                } else {
                                    break;
                                }
                            }
                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }
                            dequeMemory.putDecimal128(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, value.getHigh(), value.getLow());
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        maxMin.ofRawNull();
                    } else {
                        dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE, maxMin);
                    }
                } else {
                    mapValue.getDecimal128(5, oldMax);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, val);
                            if (oldMax.isNull() || isBetter(val, oldMax)) {
                                oldMax.copyFrom(val);
                            }
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                    firstIdx = newFirstIdx;
                    maxMin.copyFrom(oldMax);
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
                mapValue.putDecimal128(5, maxMin);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHigh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
            maxMin.ofRawNull();
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal128Comparator comparator;
        private final Decimal128 dequeBack = new Decimal128();
        private final int dequeBufferSize;
        private final Decimal128 dequeFront = new Decimal128();
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal128 hiValue = new Decimal128();
        private final Decimal128 loValue = new Decimal128();
        private final Decimal128 max = new Decimal128();
        private final Decimal128 maxMin = new Decimal128();
        private final MemoryARW memory;
        private final String name;
        private final Decimal128 nullScratch = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal128Comparator comparator,
                String name,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long loIdx;
            long startOffset;
            arg.getDecimal128(record, scratch);
            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal128.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !scratch.isNull()) {
                    maxMin.copyFrom(scratch);
                } else {
                    maxMin.ofRawNull();
                }
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal128(startOffset + (long) i * Decimal128.BYTES, nullScratch.getHigh(), nullScratch.getLow());
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Decimal128.BYTES) - dequeMemory.getPageAddress(0);
                    if (!scratch.isNull() && frameIncludesCurrentValue) {
                        dequeMemory.putDecimal128(dequeStartOffset, scratch.getHigh(), scratch.getLow());
                        dequeEndIndex++;
                    }
                } else {
                    value.putDecimal128(2, maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                if (frameIncludesCurrentValue) {
                    hiValue.copyFrom(scratch);
                } else {
                    memory.getDecimal128(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Decimal128.BYTES, hiValue);
                }
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);
                    if (!hiValue.isNull()) {
                        while (dequeStartIndex != dequeEndIndex) {
                            dequeMemory.getDecimal128(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Decimal128.BYTES, dequeBack);
                            if (isBetter(hiValue, dequeBack)) {
                                dequeEndIndex--;
                            } else {
                                break;
                            }
                        }
                        dequeMemory.putDecimal128(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Decimal128.BYTES, hiValue.getHigh(), hiValue.getLow());
                        dequeEndIndex++;
                        dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Decimal128.BYTES, maxMin);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Decimal128.BYTES, maxMin);
                        } else {
                            maxMin.ofRawNull();
                        }
                    }
                } else {
                    value.getDecimal128(2, max);
                    if (!hiValue.isNull()) {
                        if (max.isNull() || isBetter(hiValue, max)) {
                            max.copyFrom(hiValue);
                            value.putDecimal128(2, max);
                        }
                    }
                    maxMin.copyFrom(max);
                }
                if (frameLoBounded) {
                    memory.getDecimal128(startOffset + loIdx * Decimal128.BYTES, loValue);
                    if (!loValue.isNull() && dequeStartIndex != dequeEndIndex) {
                        dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Decimal128.BYTES, dequeFront);
                        if (loValue.compareTo(dequeFront) == 0) {
                            dequeStartIndex++;
                        }
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
            memory.putDecimal128(startOffset + loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHigh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getLow());
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal128Comparator comparator;
        private final Decimal128 dequeBack = new Decimal128();
        private final Decimal128 dequeFront = new Decimal128();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final Decimal128 maxMin = new Decimal128();
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final Decimal128 scratch = new Decimal128();
        private final int timestampIndex;
        private final int type;
        private final Decimal128 val = new Decimal128();
        private final Decimal128 value = new Decimal128();
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal128MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                Decimal128Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin.ofRawNull();
            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Decimal128.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            arg.getDecimal128(record, scratch);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, val);
                            if (!val.isNull() && dequeStartIndex != dequeEndIndex) {
                                dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Decimal128.BYTES, dequeFront);
                                if (val.compareTo(dequeFront) == 0) {
                                    dequeStartIndex++;
                                }
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

            if (!scratch.isNull()) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDecimal128(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHigh(), scratch.getLow());
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, value);
                        while (dequeStartIndex != dequeEndIndex) {
                            dequeMemory.getDecimal128(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Decimal128.BYTES, dequeBack);
                            if (isBetter(value, dequeBack)) {
                                dequeEndIndex--;
                            } else {
                                break;
                            }
                        }
                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Decimal128.BYTES);
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;
                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Decimal128.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Decimal128.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Decimal128.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Decimal128.BYTES);
                                dequeStartIndex = 0;
                            }
                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }
                        dequeMemory.putDecimal128(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Decimal128.BYTES, value.getHigh(), value.getLow());
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    maxMin.ofRawNull();
                } else {
                    dequeMemory.getDecimal128(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Decimal128.BYTES, maxMin);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, val);
                        if (maxMin.isNull() || isBetter(val, maxMin)) {
                            maxMin.copyFrom(val);
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
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHigh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getLow());
        }

        @Override
        public void reopen() {
            maxMin.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Decimal128.BYTES) - dequeMemory.getPageAddress(0);
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
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeMemory.truncate();
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Decimal128.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal128Comparator comparator;
        private final Decimal128 dequeBack = new Decimal128();
        private final int dequeBufferSize;
        private final Decimal128 dequeFront = new Decimal128();
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal128 hiValue = new Decimal128();
        private final Decimal128 loValue = new Decimal128();
        private final Decimal128 max = new Decimal128();
        private final Decimal128 maxMin = new Decimal128();
        private final String name;
        private final Decimal128 nullScratch = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private long count = 0;
        private long dequeEndIndex;
        private long dequeStartIndex;
        private int loIdx = 0;

        public Decimal128MaxMinOverRowsFrameFunction(
                Function arg,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal128Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            max.ofRawNull();
            maxMin.ofRawNull();
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
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
            arg.getDecimal128(record, scratch);
            if (frameLoBounded && !frameIncludesCurrentValue) {
                buffer.getDecimal128((long) ((loIdx + frameSize - 1) % bufferSize) * Decimal128.BYTES, hiValue);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                buffer.getDecimal128((long) (loIdx % bufferSize) * Decimal128.BYTES, hiValue);
            } else {
                hiValue.copyFrom(scratch);
            }
            if (frameLoBounded) {
                if (!hiValue.isNull()) {
                    while (dequeStartIndex != dequeEndIndex) {
                        dequeMemory.getDecimal128(((dequeEndIndex - 1) % dequeBufferSize) * Decimal128.BYTES, dequeBack);
                        if (isBetter(hiValue, dequeBack)) {
                            dequeEndIndex--;
                        } else {
                            break;
                        }
                    }
                    dequeMemory.putDecimal128((dequeEndIndex % dequeBufferSize) * Decimal128.BYTES, hiValue.getHigh(), hiValue.getLow());
                    dequeEndIndex++;
                    dequeMemory.getDecimal128((dequeStartIndex % dequeBufferSize) * Decimal128.BYTES, maxMin);
                } else {
                    if (dequeStartIndex != dequeEndIndex) {
                        dequeMemory.getDecimal128((dequeStartIndex % dequeBufferSize) * Decimal128.BYTES, maxMin);
                    } else {
                        maxMin.ofRawNull();
                    }
                }
            } else {
                if (!hiValue.isNull()) {
                    if (max.isNull() || isBetter(hiValue, max)) {
                        max.copyFrom(hiValue);
                    }
                }
                maxMin.copyFrom(max);
            }

            if (frameLoBounded) {
                buffer.getDecimal128((long) loIdx * Decimal128.BYTES, loValue);
                if (!loValue.isNull() && dequeStartIndex != dequeEndIndex) {
                    dequeMemory.getDecimal128((dequeStartIndex % dequeBufferSize) * Decimal128.BYTES, dequeFront);
                    if (loValue.compareTo(dequeFront) == 0) {
                        dequeStartIndex++;
                    }
                }
            }
            count = Math.min(count + 1, bufferSize);
            buffer.putDecimal128((long) loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHigh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getLow());
        }

        @Override
        public void reopen() {
            maxMin.ofRawNull();
            max.ofRawNull();
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            maxMin.ofRawNull();
            max.ofRawNull();
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin.ofRawNull();
            max.ofRawNull();
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        private void initBuffer() {
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal128((long) i * Decimal128.BYTES, nullScratch.getHigh(), nullScratch.getLow());
            }
            if (dequeMemory != null) {
                dequeMemory.appendAddressFor((long) dequeBufferSize * Decimal128.BYTES);
            }
        }

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final Decimal128Comparator comparator;
        private final Decimal128 curr = new Decimal128();
        private final String name;
        private final Decimal128 nullVal = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128MaxMinOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal128Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            arg.getDecimal128(record, scratch);
            if (mv.isNew()) {
                if (!scratch.isNull()) {
                    mv.putDecimal128(0, scratch);
                    value.copyFrom(scratch);
                } else {
                    nullVal.ofRawNull();
                    mv.putDecimal128(0, nullVal);
                    value.ofRawNull();
                }
            } else {
                mv.getDecimal128(0, curr);
                if (!scratch.isNull() && (curr.isNull() || isBetter(scratch, curr))) {
                    mv.putDecimal128(0, scratch);
                    value.copyFrom(scratch);
                } else {
                    value.copyFrom(curr);
                }
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal128Comparator comparator;
        private final String name;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128MaxMinOverUnboundedRowsFrameFunction(Function arg, Decimal128Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull() && (value.isNull() || isBetter(scratch, value))) {
                value.copyFrom(scratch);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
        }

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal128MaxMinOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal128Comparator comparator;
        private final String name;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128MaxMinOverWholeResultSetFunction(Function arg, Decimal128Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
        }

        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull() && (value.isNull() || isBetter(scratch, value))) {
                value.copyFrom(scratch);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
        }

        private boolean isBetter(Decimal128 candidate, Decimal128 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal16MaxMinOverCurrentRowFunction extends BaseWindowFunction {

        private final String name;
        private final int type;
        private short value;

        public Decimal16MaxMinOverCurrentRowFunction(Function arg, String name, int type) {
            super(arg);
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal16(record);
        }

        @Override
        public short getDecimal16(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal16MaxMinOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;

        public Decimal16MaxMinOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            short s = arg.getDecimal16(record);
            if (s == Decimals.DECIMAL16_NULL) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            if (mv.isNew()) {
                mv.putLong(0, s);
            } else {
                short curr = (short) mv.getLong(0);
                if (curr == Decimals.DECIMAL16_NULL || comparator.isBetter(s, curr)) {
                    mv.putLong(0, s);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            short val = mv != null ? (short) mv.getLong(0) : Decimals.DECIMAL16_NULL;
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal16MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int DEQUE_RECORD_SIZE = Short.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final Decimal64Comparator comparator;
        private final LongList dequeFreeList = new LongList();
        private final int dequeInitialBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private short maxMin;

        public Decimal16MaxMinOverPartitionRangeFrameFunction(
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
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            short s = arg.getDecimal16(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }
                if (s != Decimals.DECIMAL16_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putShort(startOffset + Long.BYTES, s);
                    if (frameIncludesCurrentValue) {
                        maxMin = s;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        maxMin = Decimals.DECIMAL16_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putShort(dequeStartOffset, s);
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    maxMin = Decimals.DECIMAL16_NULL;
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
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                if (dequeStartIndex != dequeEndIndex &&
                                        dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE) ==
                                                memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES)) {
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

                if (s != Decimals.DECIMAL16_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, s);
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            while (dequeStartIndex != dequeEndIndex &&
                                    comparator.isBetter(val, dequeMemory.getShort(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE))) {
                                dequeEndIndex--;
                            }
                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }
                            dequeMemory.putShort(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, val);
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        maxMin = Decimals.DECIMAL16_NULL;
                    } else {
                        maxMin = dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE);
                    }
                } else {
                    short oldMax = (short) mapValue.getLong(5);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (oldMax == Decimals.DECIMAL16_NULL || comparator.isBetter(val, oldMax)) {
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
                    maxMin = oldMax;
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
                mapValue.putLong(5, maxMin);
            }
        }

        @Override
        public short getDecimal16(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            maxMin = Decimals.DECIMAL16_NULL;
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal16MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final String name;
        private final int type;
        private short maxMin;

        public Decimal16MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long loIdx;
            long startOffset;
            short s = arg.getDecimal16(record);
            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && s != Decimals.DECIMAL16_NULL) {
                    maxMin = s;
                } else {
                    maxMin = Decimals.DECIMAL16_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Short.BYTES) - dequeMemory.getPageAddress(0);
                    if (s != Decimals.DECIMAL16_NULL && frameIncludesCurrentValue) {
                        dequeMemory.putShort(dequeStartOffset, s);
                        dequeEndIndex++;
                    }
                } else {
                    value.putLong(2, maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                short hiValue = frameIncludesCurrentValue ? s : memory.getShort(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Short.BYTES);
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);
                    if (hiValue != Decimals.DECIMAL16_NULL) {
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(hiValue, dequeMemory.getShort(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Short.BYTES))) {
                            dequeEndIndex--;
                        }
                        dequeMemory.putShort(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Short.BYTES, hiValue);
                        dequeEndIndex++;
                        maxMin = dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Short.BYTES);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            maxMin = dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Short.BYTES);
                        } else {
                            maxMin = Decimals.DECIMAL16_NULL;
                        }
                    }
                } else {
                    short max = (short) value.getLong(2);
                    if (hiValue != Decimals.DECIMAL16_NULL) {
                        if (max == Decimals.DECIMAL16_NULL || comparator.isBetter(hiValue, max)) {
                            max = hiValue;
                            value.putLong(2, max);
                        }
                    }
                    maxMin = max;
                }
                if (frameLoBounded) {
                    short loValue = memory.getShort(startOffset + loIdx * Short.BYTES);
                    if (loValue != Decimals.DECIMAL16_NULL && dequeStartIndex != dequeEndIndex &&
                            loValue == dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Short.BYTES)) {
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
            memory.putShort(startOffset + loIdx * Short.BYTES, s);
        }

        @Override
        public short getDecimal16(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal16MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final Decimal64Comparator comparator;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private short maxMin;
        private long size;
        private long startOffset;

        public Decimal16MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin = Decimals.DECIMAL16_NULL;
            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Short.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            short s = arg.getDecimal16(record);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (val != Decimals.DECIMAL16_NULL && dequeStartIndex != dequeEndIndex && val ==
                                    dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Short.BYTES)) {
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

            if (s != Decimals.DECIMAL16_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, s);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(val, dequeMemory.getShort(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Short.BYTES))) {
                            dequeEndIndex--;
                        }
                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Short.BYTES);
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;
                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Short.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Short.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Short.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Short.BYTES);
                                dequeStartIndex = 0;
                            }
                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }
                        dequeMemory.putShort(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Short.BYTES, val);
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    maxMin = Decimals.DECIMAL16_NULL;
                } else {
                    maxMin = dequeMemory.getShort(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Short.BYTES);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        if (maxMin == Decimals.DECIMAL16_NULL || comparator.isBetter(val, maxMin)) {
                            maxMin = val;
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
        public short getDecimal16(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Short.BYTES) - dequeMemory.getPageAddress(0);
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (range between ");
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
            maxMin = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeMemory.truncate();
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Short.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }
    }

    public static class Decimal16MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final String name;
        private final int type;
        private long count = 0;
        private long dequeEndIndex;
        private long dequeStartIndex;
        private int loIdx = 0;
        private short max = Decimals.DECIMAL16_NULL;
        private short maxMin;

        public Decimal16MaxMinOverRowsFrameFunction(
                Function arg,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
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
            short s = arg.getDecimal16(record);
            short hiValue = s;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getShort((long) ((loIdx + frameSize - 1) % bufferSize) * Short.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getShort((long) (loIdx % bufferSize) * Short.BYTES);
            }
            if (frameLoBounded) {
                if (hiValue != Decimals.DECIMAL16_NULL) {
                    while (dequeStartIndex != dequeEndIndex &&
                            comparator.isBetter(hiValue, dequeMemory.getShort(((dequeEndIndex - 1) % dequeBufferSize) * Short.BYTES))) {
                        dequeEndIndex--;
                    }
                    dequeMemory.putShort((dequeEndIndex % dequeBufferSize) * Short.BYTES, hiValue);
                    dequeEndIndex++;
                    maxMin = dequeMemory.getShort((dequeStartIndex % dequeBufferSize) * Short.BYTES);
                } else {
                    if (dequeStartIndex != dequeEndIndex) {
                        maxMin = dequeMemory.getShort((dequeStartIndex % dequeBufferSize) * Short.BYTES);
                    } else {
                        maxMin = Decimals.DECIMAL16_NULL;
                    }
                }
            } else {
                if (hiValue != Decimals.DECIMAL16_NULL) {
                    if (max == Decimals.DECIMAL16_NULL || comparator.isBetter(hiValue, max)) {
                        max = hiValue;
                    }
                }
                maxMin = max;
            }

            if (frameLoBounded) {
                short loValue = buffer.getShort((long) loIdx * Short.BYTES);
                if (loValue != Decimals.DECIMAL16_NULL && dequeStartIndex != dequeEndIndex &&
                        loValue == dequeMemory.getShort((dequeStartIndex % dequeBufferSize) * Short.BYTES)) {
                    dequeStartIndex++;
                }
            }
            count = Math.min(count + 1, bufferSize);
            buffer.putShort((long) loIdx * Short.BYTES, s);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public short getDecimal16(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL16_NULL;
            max = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            maxMin = Decimals.DECIMAL16_NULL;
            max = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin = Decimals.DECIMAL16_NULL;
            max = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
            if (dequeMemory != null) {
                dequeMemory.appendAddressFor((long) dequeBufferSize * Short.BYTES);
            }
        }
    }

    public static class Decimal16MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private short value;

        public Decimal16MaxMinOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            short s = arg.getDecimal16(record);
            if (mv.isNew()) {
                if (s != Decimals.DECIMAL16_NULL) {
                    mv.putLong(0, s);
                    value = s;
                } else {
                    mv.putLong(0, Decimals.DECIMAL16_NULL);
                    value = Decimals.DECIMAL16_NULL;
                }
            } else {
                short curr = (short) mv.getLong(0);
                if (s != Decimals.DECIMAL16_NULL && (curr == Decimals.DECIMAL16_NULL || comparator.isBetter(s, curr))) {
                    mv.putLong(0, s);
                    value = s;
                } else {
                    value = curr;
                }
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    public static class Decimal16MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16MaxMinOverUnboundedRowsFrameFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL && (value == Decimals.DECIMAL16_NULL || comparator.isBetter(s, value))) {
                value = s;
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal16MaxMinOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16MaxMinOverWholeResultSetFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL && (value == Decimals.DECIMAL16_NULL || comparator.isBetter(s, value))) {
                value = s;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal256MaxMinOverCurrentRowFunction extends BaseWindowFunction {

        private final String name;
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256MaxMinOverCurrentRowFunction(Function arg, String name, int type) {
            super(arg);
            this.name = name;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, value);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }
    }

    public static class Decimal256MaxMinOverPartitionFunction extends BasePartitionedWindowFunction {
        private final Decimal256Comparator comparator;
        private final Decimal256 curr = new Decimal256();
        private final String name;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256MaxMinOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal256Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            arg.getDecimal256(record, scratch);
            if (scratch.isNull()) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            if (mv.isNew()) {
                mv.putDecimal256(0, scratch);
            } else {
                mv.getDecimal256(0, curr);
                if (curr.isNull() || isBetter(scratch, curr)) {
                    mv.putDecimal256(0, scratch);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null) {
                mv.getDecimal256(0, scratch);
                Unsafe.putLong(addr, scratch.getHh());
                Unsafe.putLong(addr + Long.BYTES, scratch.getHl());
                Unsafe.putLong(addr + 2 * Long.BYTES, scratch.getLh());
                Unsafe.putLong(addr + 3 * Long.BYTES, scratch.getLl());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            }
        }

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {
        private static final int DEQUE_RECORD_SIZE = 32;
        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256Comparator comparator;
        private final Decimal256 dequeBack = new Decimal256();
        private final LongList dequeFreeList = new LongList();
        private final Decimal256 dequeFront = new Decimal256();
        private final int dequeInitialBufferSize;
        private final MemoryARW dequeMemory;
        private final Decimal256 evicted = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final Decimal256 maxMin = new Decimal256();
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final Decimal256 oldMax = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 val = new Decimal256();
        private final Decimal256 value = new Decimal256();

        public Decimal256MaxMinOverPartitionRangeFrameFunction(
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
                Decimal256Comparator comparator,
                String name,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            maxMin.ofRawNull();
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
            arg.getDecimal256(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }
                if (!scratch.isNull()) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal256(startOffset + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                    if (frameIncludesCurrentValue) {
                        maxMin.copyRaw(scratch);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        maxMin.ofRawNull();
                        frameSize = 0;
                        size = 1;
                    }
                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putDecimal256(dequeStartOffset, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    maxMin.ofRawNull();
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
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                if (dequeStartIndex != dequeEndIndex) {
                                    dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE, dequeFront);
                                    memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, evicted);
                                    if (dequeFront.compareTo(evicted) == 0) {
                                        dequeStartIndex++;
                                    }
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

                if (!scratch.isNull()) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putDecimal256(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, value);
                            while (dequeStartIndex != dequeEndIndex) {
                                dequeMemory.getDecimal256(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE, dequeBack);
                                if (isBetter(value, dequeBack)) {
                                    dequeEndIndex--;
                                } else {
                                    break;
                                }
                            }
                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }
                            dequeMemory.putDecimal256(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, value.getHh(), value.getHl(), value.getLh(), value.getLl());
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        maxMin.ofRawNull();
                    } else {
                        dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE, maxMin);
                    }
                } else {
                    mapValue.getDecimal256(5, oldMax);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, val);
                            if (oldMax.isNull() || isBetter(val, oldMax)) {
                                oldMax.copyRaw(val);
                            }
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                    firstIdx = newFirstIdx;
                    maxMin.copyRaw(oldMax);
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
                mapValue.putDecimal256(5, maxMin);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, maxMin.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, maxMin.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
            maxMin.ofRawNull();
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal256Comparator comparator;
        private final Decimal256 dequeBack = new Decimal256();
        private final int dequeBufferSize;
        private final Decimal256 dequeFront = new Decimal256();
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal256 hiValue = new Decimal256();
        private final Decimal256 loValue = new Decimal256();
        private final Decimal256 max = new Decimal256();
        private final Decimal256 maxMin = new Decimal256();
        private final MemoryARW memory;
        private final String name;
        private final Decimal256 nullScratch = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal256Comparator comparator,
                String name,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long loIdx;
            long startOffset;
            arg.getDecimal256(record, scratch);
            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal256.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !scratch.isNull()) {
                    maxMin.copyRaw(scratch);
                } else {
                    maxMin.ofRawNull();
                }
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal256(startOffset + (long) i * Decimal256.BYTES, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Decimal256.BYTES) - dequeMemory.getPageAddress(0);
                    if (!scratch.isNull() && frameIncludesCurrentValue) {
                        dequeMemory.putDecimal256(dequeStartOffset, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                        dequeEndIndex++;
                    }
                } else {
                    value.putDecimal256(2, maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                if (frameIncludesCurrentValue) {
                    hiValue.copyRaw(scratch);
                } else {
                    memory.getDecimal256(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Decimal256.BYTES, hiValue);
                }
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);
                    if (!hiValue.isNull()) {
                        while (dequeStartIndex != dequeEndIndex) {
                            dequeMemory.getDecimal256(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Decimal256.BYTES, dequeBack);
                            if (isBetter(hiValue, dequeBack)) {
                                dequeEndIndex--;
                            } else {
                                break;
                            }
                        }
                        dequeMemory.putDecimal256(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Decimal256.BYTES, hiValue.getHh(), hiValue.getHl(), hiValue.getLh(), hiValue.getLl());
                        dequeEndIndex++;
                        dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Decimal256.BYTES, maxMin);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Decimal256.BYTES, maxMin);
                        } else {
                            maxMin.ofRawNull();
                        }
                    }
                } else {
                    value.getDecimal256(2, max);
                    if (!hiValue.isNull()) {
                        if (max.isNull() || isBetter(hiValue, max)) {
                            max.copyRaw(hiValue);
                            value.putDecimal256(2, max);
                        }
                    }
                    maxMin.copyRaw(max);
                }
                if (frameLoBounded) {
                    memory.getDecimal256(startOffset + loIdx * Decimal256.BYTES, loValue);
                    if (!loValue.isNull() && dequeStartIndex != dequeEndIndex) {
                        dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Decimal256.BYTES, dequeFront);
                        if (loValue.compareTo(dequeFront) == 0) {
                            dequeStartIndex++;
                        }
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
            memory.putDecimal256(startOffset + loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, maxMin.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, maxMin.getLl());
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256Comparator comparator;
        private final Decimal256 dequeBack = new Decimal256();
        private final Decimal256 dequeFront = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final Decimal256 maxMin = new Decimal256();
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final Decimal256 scratch = new Decimal256();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 val = new Decimal256();
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal256MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                Decimal256Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin.ofRawNull();
            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Decimal256.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            arg.getDecimal256(record, scratch);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, val);
                            if (!val.isNull() && dequeStartIndex != dequeEndIndex) {
                                dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Decimal256.BYTES, dequeFront);
                                if (val.compareTo(dequeFront) == 0) {
                                    dequeStartIndex++;
                                }
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

            if (!scratch.isNull()) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDecimal256(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, value);
                        while (dequeStartIndex != dequeEndIndex) {
                            dequeMemory.getDecimal256(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Decimal256.BYTES, dequeBack);
                            if (isBetter(value, dequeBack)) {
                                dequeEndIndex--;
                            } else {
                                break;
                            }
                        }
                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Decimal256.BYTES);
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;
                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Decimal256.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Decimal256.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Decimal256.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Decimal256.BYTES);
                                dequeStartIndex = 0;
                            }
                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }
                        dequeMemory.putDecimal256(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Decimal256.BYTES, value.getHh(), value.getHl(), value.getLh(), value.getLl());
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    maxMin.ofRawNull();
                } else {
                    dequeMemory.getDecimal256(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Decimal256.BYTES, maxMin);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, val);
                        if (maxMin.isNull() || isBetter(val, maxMin)) {
                            maxMin.copyRaw(val);
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
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, maxMin.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, maxMin.getLl());
        }

        @Override
        public void reopen() {
            maxMin.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Decimal256.BYTES) - dequeMemory.getPageAddress(0);
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
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeMemory.truncate();
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Decimal256.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256Comparator comparator;
        private final Decimal256 dequeBack = new Decimal256();
        private final int dequeBufferSize;
        private final Decimal256 dequeFront = new Decimal256();
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal256 hiValue = new Decimal256();
        private final Decimal256 loValue = new Decimal256();
        private final Decimal256 max = new Decimal256();
        private final Decimal256 maxMin = new Decimal256();
        private final String name;
        private final Decimal256 nullScratch = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private long count = 0;
        private long dequeEndIndex;
        private long dequeStartIndex;
        private int loIdx = 0;

        public Decimal256MaxMinOverRowsFrameFunction(
                Function arg,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal256Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            max.ofRawNull();
            maxMin.ofRawNull();
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
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
            arg.getDecimal256(record, scratch);
            if (frameLoBounded && !frameIncludesCurrentValue) {
                buffer.getDecimal256((long) ((loIdx + frameSize - 1) % bufferSize) * Decimal256.BYTES, hiValue);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                buffer.getDecimal256((long) (loIdx % bufferSize) * Decimal256.BYTES, hiValue);
            } else {
                hiValue.copyRaw(scratch);
            }
            if (frameLoBounded) {
                if (!hiValue.isNull()) {
                    while (dequeStartIndex != dequeEndIndex) {
                        dequeMemory.getDecimal256(((dequeEndIndex - 1) % dequeBufferSize) * Decimal256.BYTES, dequeBack);
                        if (isBetter(hiValue, dequeBack)) {
                            dequeEndIndex--;
                        } else {
                            break;
                        }
                    }
                    dequeMemory.putDecimal256((dequeEndIndex % dequeBufferSize) * Decimal256.BYTES, hiValue.getHh(), hiValue.getHl(), hiValue.getLh(), hiValue.getLl());
                    dequeEndIndex++;
                    dequeMemory.getDecimal256((dequeStartIndex % dequeBufferSize) * Decimal256.BYTES, maxMin);
                } else {
                    if (dequeStartIndex != dequeEndIndex) {
                        dequeMemory.getDecimal256((dequeStartIndex % dequeBufferSize) * Decimal256.BYTES, maxMin);
                    } else {
                        maxMin.ofRawNull();
                    }
                }
            } else {
                if (!hiValue.isNull()) {
                    if (max.isNull() || isBetter(hiValue, max)) {
                        max.copyRaw(hiValue);
                    }
                }
                maxMin.copyRaw(max);
            }

            if (frameLoBounded) {
                buffer.getDecimal256((long) loIdx * Decimal256.BYTES, loValue);
                if (!loValue.isNull() && dequeStartIndex != dequeEndIndex) {
                    dequeMemory.getDecimal256((dequeStartIndex % dequeBufferSize) * Decimal256.BYTES, dequeFront);
                    if (loValue.compareTo(dequeFront) == 0) {
                        dequeStartIndex++;
                    }
                }
            }
            count = Math.min(count + 1, bufferSize);
            buffer.putDecimal256((long) loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(maxMin);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, maxMin.getHh());
            Unsafe.putLong(addr + Long.BYTES, maxMin.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, maxMin.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, maxMin.getLl());
        }

        @Override
        public void reopen() {
            maxMin.ofRawNull();
            max.ofRawNull();
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            maxMin.ofRawNull();
            max.ofRawNull();
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin.ofRawNull();
            max.ofRawNull();
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        private void initBuffer() {
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal256((long) i * Decimal256.BYTES, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
            }
            if (dequeMemory != null) {
                dequeMemory.appendAddressFor((long) dequeBufferSize * Decimal256.BYTES);
            }
        }

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final Decimal256Comparator comparator;
        private final Decimal256 curr = new Decimal256();
        private final String name;
        private final Decimal256 nullVal = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256MaxMinOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal256Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            arg.getDecimal256(record, scratch);
            if (mv.isNew()) {
                if (!scratch.isNull()) {
                    mv.putDecimal256(0, scratch);
                    value.copyRaw(scratch);
                } else {
                    nullVal.ofRawNull();
                    mv.putDecimal256(0, nullVal);
                    value.ofRawNull();
                }
            } else {
                mv.getDecimal256(0, curr);
                if (!scratch.isNull() && (curr.isNull() || isBetter(scratch, curr))) {
                    mv.putDecimal256(0, scratch);
                    value.copyRaw(scratch);
                } else {
                    value.copyRaw(curr);
                }
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256Comparator comparator;
        private final String name;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256MaxMinOverUnboundedRowsFrameFunction(Function arg, Decimal256Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull() && (value.isNull() || isBetter(scratch, value))) {
                value.copyRaw(scratch);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
        }

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal256MaxMinOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256Comparator comparator;
        private final String name;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256MaxMinOverWholeResultSetFunction(Function arg, Decimal256Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull() && (value.isNull() || isBetter(scratch, value))) {
                value.copyRaw(scratch);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
        }

        private boolean isBetter(Decimal256 candidate, Decimal256 current) {
            return comparator.isBetter(candidate, current);
        }
    }

    public static class Decimal32MaxMinOverCurrentRowFunction extends BaseWindowFunction {

        private final String name;
        private final int type;
        private int value;

        public Decimal32MaxMinOverCurrentRowFunction(Function arg, String name, int type) {
            super(arg);
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal32(record);
        }

        @Override
        public int getDecimal32(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal32MaxMinOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;

        public Decimal32MaxMinOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            int i = arg.getDecimal32(record);
            if (i == Decimals.DECIMAL32_NULL) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            if (mv.isNew()) {
                mv.putLong(0, i);
            } else {
                int curr = (int) mv.getLong(0);
                if (curr == Decimals.DECIMAL32_NULL || comparator.isBetter(i, curr)) {
                    mv.putLong(0, i);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            int val = mv != null ? (int) mv.getLong(0) : Decimals.DECIMAL32_NULL;
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal32MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int DEQUE_RECORD_SIZE = Integer.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal64Comparator comparator;
        private final LongList dequeFreeList = new LongList();
        private final int dequeInitialBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private int maxMin;

        public Decimal32MaxMinOverPartitionRangeFrameFunction(
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
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            int i = arg.getDecimal32(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }
                if (i != Decimals.DECIMAL32_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putInt(startOffset + Long.BYTES, i);
                    if (frameIncludesCurrentValue) {
                        maxMin = i;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        maxMin = Decimals.DECIMAL32_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putInt(dequeStartOffset, i);
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    maxMin = Decimals.DECIMAL32_NULL;
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                if (dequeStartIndex != dequeEndIndex &&
                                        dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE) ==
                                                memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES)) {
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

                if (i != Decimals.DECIMAL32_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, i);
                    size++;
                }

                if (frameLoBounded) {
                    for (long j = frameSize; j < size; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            while (dequeStartIndex != dequeEndIndex &&
                                    comparator.isBetter(val, dequeMemory.getInt(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE))) {
                                dequeEndIndex--;
                            }
                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }
                            dequeMemory.putInt(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, val);
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        maxMin = Decimals.DECIMAL32_NULL;
                    } else {
                        maxMin = dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE);
                    }
                } else {
                    int oldMax = (int) mapValue.getLong(5);
                    newFirstIdx = firstIdx;
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (oldMax == Decimals.DECIMAL32_NULL || comparator.isBetter(val, oldMax)) {
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
                    maxMin = oldMax;
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
                mapValue.putLong(5, maxMin);
            }
        }

        @Override
        public int getDecimal32(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            maxMin = Decimals.DECIMAL32_NULL;
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal32MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final String name;
        private final int type;
        private int maxMin;

        public Decimal32MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long loIdx;
            long startOffset;
            int i = arg.getDecimal32(record);
            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && i != Decimals.DECIMAL32_NULL) {
                    maxMin = i;
                } else {
                    maxMin = Decimals.DECIMAL32_NULL;
                }
                for (int j = 0; j < bufferSize; j++) {
                    memory.putInt(startOffset + (long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Integer.BYTES) - dequeMemory.getPageAddress(0);
                    if (i != Decimals.DECIMAL32_NULL && frameIncludesCurrentValue) {
                        dequeMemory.putInt(dequeStartOffset, i);
                        dequeEndIndex++;
                    }
                } else {
                    value.putLong(2, maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                int hiValue = frameIncludesCurrentValue ? i : memory.getInt(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);
                    if (hiValue != Decimals.DECIMAL32_NULL) {
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(hiValue, dequeMemory.getInt(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Integer.BYTES))) {
                            dequeEndIndex--;
                        }
                        dequeMemory.putInt(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Integer.BYTES, hiValue);
                        dequeEndIndex++;
                        maxMin = dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Integer.BYTES);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            maxMin = dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Integer.BYTES);
                        } else {
                            maxMin = Decimals.DECIMAL32_NULL;
                        }
                    }
                } else {
                    int max = (int) value.getLong(2);
                    if (hiValue != Decimals.DECIMAL32_NULL) {
                        if (max == Decimals.DECIMAL32_NULL || comparator.isBetter(hiValue, max)) {
                            max = hiValue;
                            value.putLong(2, max);
                        }
                    }
                    maxMin = max;
                }
                if (frameLoBounded) {
                    int loValue = memory.getInt(startOffset + loIdx * Integer.BYTES);
                    if (loValue != Decimals.DECIMAL32_NULL && dequeStartIndex != dequeEndIndex &&
                            loValue == dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Integer.BYTES)) {
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
            memory.putInt(startOffset + loIdx * Integer.BYTES, i);
        }

        @Override
        public int getDecimal32(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal32MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal64Comparator comparator;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private int maxMin;
        private long size;
        private long startOffset;

        public Decimal32MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin = Decimals.DECIMAL32_NULL;
            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Integer.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            int i = arg.getDecimal32(record);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (val != Decimals.DECIMAL32_NULL && dequeStartIndex != dequeEndIndex && val ==
                                    dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Integer.BYTES)) {
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

            if (i != Decimals.DECIMAL32_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, i);
                size++;
            }

            if (frameLoBounded) {
                for (long j = frameSize, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(val, dequeMemory.getInt(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Integer.BYTES))) {
                            dequeEndIndex--;
                        }
                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Integer.BYTES);
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;
                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Integer.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Integer.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Integer.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Integer.BYTES);
                                dequeStartIndex = 0;
                            }
                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }
                        dequeMemory.putInt(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Integer.BYTES, val);
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    maxMin = Decimals.DECIMAL32_NULL;
                } else {
                    maxMin = dequeMemory.getInt(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Integer.BYTES);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        if (maxMin == Decimals.DECIMAL32_NULL || comparator.isBetter(val, maxMin)) {
                            maxMin = val;
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
        public int getDecimal32(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Integer.BYTES) - dequeMemory.getPageAddress(0);
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (range between ");
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
            maxMin = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeMemory.truncate();
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Integer.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }
    }

    public static class Decimal32MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final String name;
        private final int type;
        private long count = 0;
        private long dequeEndIndex;
        private long dequeStartIndex;
        private int loIdx = 0;
        private int max = Decimals.DECIMAL32_NULL;
        private int maxMin;

        public Decimal32MaxMinOverRowsFrameFunction(
                Function arg,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
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
            int i = arg.getDecimal32(record);
            int hiValue = i;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getInt((long) ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getInt((long) (loIdx % bufferSize) * Integer.BYTES);
            }
            if (frameLoBounded) {
                if (hiValue != Decimals.DECIMAL32_NULL) {
                    while (dequeStartIndex != dequeEndIndex &&
                            comparator.isBetter(hiValue, dequeMemory.getInt(((dequeEndIndex - 1) % dequeBufferSize) * Integer.BYTES))) {
                        dequeEndIndex--;
                    }
                    dequeMemory.putInt((dequeEndIndex % dequeBufferSize) * Integer.BYTES, hiValue);
                    dequeEndIndex++;
                    maxMin = dequeMemory.getInt((dequeStartIndex % dequeBufferSize) * Integer.BYTES);
                } else {
                    if (dequeStartIndex != dequeEndIndex) {
                        maxMin = dequeMemory.getInt((dequeStartIndex % dequeBufferSize) * Integer.BYTES);
                    } else {
                        maxMin = Decimals.DECIMAL32_NULL;
                    }
                }
            } else {
                if (hiValue != Decimals.DECIMAL32_NULL) {
                    if (max == Decimals.DECIMAL32_NULL || comparator.isBetter(hiValue, max)) {
                        max = hiValue;
                    }
                }
                maxMin = max;
            }

            if (frameLoBounded) {
                int loValue = buffer.getInt((long) loIdx * Integer.BYTES);
                if (loValue != Decimals.DECIMAL32_NULL && dequeStartIndex != dequeEndIndex &&
                        loValue == dequeMemory.getInt((dequeStartIndex % dequeBufferSize) * Integer.BYTES)) {
                    dequeStartIndex++;
                }
            }
            count = Math.min(count + 1, bufferSize);
            buffer.putInt((long) loIdx * Integer.BYTES, i);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public int getDecimal32(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL32_NULL;
            max = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            maxMin = Decimals.DECIMAL32_NULL;
            max = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin = Decimals.DECIMAL32_NULL;
            max = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int j = 0; j < bufferSize; j++) {
                buffer.putInt((long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
            }
            if (dequeMemory != null) {
                dequeMemory.appendAddressFor((long) dequeBufferSize * Integer.BYTES);
            }
        }
    }

    public static class Decimal32MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private int value;

        public Decimal32MaxMinOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            int i = arg.getDecimal32(record);
            if (mv.isNew()) {
                if (i != Decimals.DECIMAL32_NULL) {
                    mv.putLong(0, i);
                    value = i;
                } else {
                    mv.putLong(0, Decimals.DECIMAL32_NULL);
                    value = Decimals.DECIMAL32_NULL;
                }
            } else {
                int curr = (int) mv.getLong(0);
                if (i != Decimals.DECIMAL32_NULL && (curr == Decimals.DECIMAL32_NULL || comparator.isBetter(i, curr))) {
                    mv.putLong(0, i);
                    value = i;
                } else {
                    value = curr;
                }
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    public static class Decimal32MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32MaxMinOverUnboundedRowsFrameFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL && (value == Decimals.DECIMAL32_NULL || comparator.isBetter(i, value))) {
                value = i;
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal32MaxMinOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32MaxMinOverWholeResultSetFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL && (value == Decimals.DECIMAL32_NULL || comparator.isBetter(i, value))) {
                value = i;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal64MaxMinOverCurrentRowFunction extends BaseWindowFunction {

        private final String name;
        private final int type;
        private long value;

        public Decimal64MaxMinOverCurrentRowFunction(Function arg, String name, int type) {
            super(arg);
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal64(record);
        }

        @Override
        public long getDecimal64(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal64MaxMinOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;

        public Decimal64MaxMinOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long d = arg.getDecimal64(record);
            if (d == Decimals.DECIMAL64_NULL) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            if (mv.isNew()) {
                mv.putLong(0, d);
            } else {
                long curr = mv.getLong(0);
                if (Decimal64.isNull(curr) || comparator.isBetter(d, curr)) {
                    mv.putLong(0, d);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long val = mv != null ? mv.getLong(0) : Decimals.DECIMAL64_NULL;
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal64MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int DEQUE_RECORD_SIZE = Long.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal64Comparator comparator;
        private final LongList dequeFreeList = new LongList();
        private final int dequeInitialBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private long maxMin;

        public Decimal64MaxMinOverPartitionRangeFrameFunction(
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
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            long d = arg.getDecimal64(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }
                if (d != Decimals.DECIMAL64_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    if (frameIncludesCurrentValue) {
                        maxMin = d;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        maxMin = Decimals.DECIMAL64_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putLong(dequeStartOffset, d);
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    maxMin = Decimals.DECIMAL64_NULL;
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
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
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

                if (d != Decimals.DECIMAL64_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            long value = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            while (dequeStartIndex != dequeEndIndex &&
                                    comparator.isBetter(value, dequeMemory.getLong(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE))) {
                                dequeEndIndex--;
                            }
                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
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
                        maxMin = Decimals.DECIMAL64_NULL;
                    } else {
                        maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE);
                    }
                } else {
                    long oldMax = mapValue.getLong(5);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (Decimal64.isNull(oldMax) || comparator.isBetter(val, oldMax)) {
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
                    maxMin = oldMax;
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
                mapValue.putLong(5, maxMin);
            }
        }

        @Override
        public long getDecimal64(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            maxMin = Decimals.DECIMAL64_NULL;
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal64MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final String name;
        private final int type;
        private long maxMin;

        public Decimal64MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long loIdx;
            long startOffset;
            long d = arg.getDecimal64(record);
            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && d != Decimals.DECIMAL64_NULL) {
                    maxMin = d;
                } else {
                    maxMin = Decimals.DECIMAL64_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Long.BYTES) - dequeMemory.getPageAddress(0);
                    if (d != Decimals.DECIMAL64_NULL && frameIncludesCurrentValue) {
                        dequeMemory.putLong(dequeStartOffset, d);
                        dequeEndIndex++;
                    }
                } else {
                    value.putLong(2, maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                long hiValue = frameIncludesCurrentValue ? d : memory.getLong(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);
                    if (hiValue != Decimals.DECIMAL64_NULL) {
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(hiValue, dequeMemory.getLong(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Long.BYTES))) {
                            dequeEndIndex--;
                        }
                        dequeMemory.putLong(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Long.BYTES, hiValue);
                        dequeEndIndex++;
                        maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Long.BYTES);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Long.BYTES);
                        } else {
                            maxMin = Decimals.DECIMAL64_NULL;
                        }
                    }
                } else {
                    long max = value.getLong(2);
                    if (hiValue != Decimals.DECIMAL64_NULL) {
                        if (Decimal64.isNull(max) || comparator.isBetter(hiValue, max)) {
                            max = hiValue;
                            value.putLong(2, max);
                        }
                    }
                    maxMin = max;
                }
                if (frameLoBounded) {
                    long loValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    if (loValue != Decimals.DECIMAL64_NULL && dequeStartIndex != dequeEndIndex &&
                            loValue == dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Long.BYTES)) {
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
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        @Override
        public long getDecimal64(Record rec) {
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
        public int getType() {
            return type;
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal64MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal64Comparator comparator;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private long maxMin;
        private long size;
        private long startOffset;

        public Decimal64MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin = Decimals.DECIMAL64_NULL;
            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Long.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            long d = arg.getDecimal64(record);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (val != Decimals.DECIMAL64_NULL && dequeStartIndex != dequeEndIndex && val ==
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

            if (d != Decimals.DECIMAL64_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        long value = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(value, dequeMemory.getLong(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Long.BYTES))) {
                            dequeEndIndex--;
                        }
                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Long.BYTES);
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;
                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Long.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
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
                    maxMin = Decimals.DECIMAL64_NULL;
                } else {
                    maxMin = dequeMemory.getLong(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Long.BYTES);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        if (Decimal64.isNull(maxMin) || comparator.isBetter(val, maxMin)) {
                            maxMin = val;
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
        public long getDecimal64(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL64_NULL;
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
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeMemory.truncate();
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Long.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }
    }

    public static class Decimal64MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final String name;
        private final int type;
        private long count = 0;
        private long dequeEndIndex;
        private long dequeStartIndex;
        private int loIdx = 0;
        private long max = Decimals.DECIMAL64_NULL;
        private long maxMin;

        public Decimal64MaxMinOverRowsFrameFunction(
                Function arg,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
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
            long d = arg.getDecimal64(record);
            long hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) (loIdx % bufferSize) * Long.BYTES);
            }
            if (frameLoBounded) {
                if (hiValue != Decimals.DECIMAL64_NULL) {
                    while (dequeStartIndex != dequeEndIndex &&
                            comparator.isBetter(hiValue, dequeMemory.getLong(((dequeEndIndex - 1) % dequeBufferSize) * Long.BYTES))) {
                        dequeEndIndex--;
                    }
                    dequeMemory.putLong((dequeEndIndex % dequeBufferSize) * Long.BYTES, hiValue);
                    dequeEndIndex++;
                    maxMin = dequeMemory.getLong((dequeStartIndex % dequeBufferSize) * Long.BYTES);
                } else {
                    if (dequeStartIndex != dequeEndIndex) {
                        maxMin = dequeMemory.getLong((dequeStartIndex % dequeBufferSize) * Long.BYTES);
                    } else {
                        maxMin = Decimals.DECIMAL64_NULL;
                    }
                }
            } else {
                if (hiValue != Decimals.DECIMAL64_NULL) {
                    if (Decimal64.isNull(max) || comparator.isBetter(hiValue, max)) {
                        max = hiValue;
                    }
                }
                maxMin = max;
            }

            if (frameLoBounded) {
                long loValue = buffer.getLong((long) loIdx * Long.BYTES);
                if (loValue != Decimals.DECIMAL64_NULL && dequeStartIndex != dequeEndIndex &&
                        loValue == dequeMemory.getLong((dequeStartIndex % dequeBufferSize) * Long.BYTES)) {
                    dequeStartIndex++;
                }
            }
            count = Math.min(count + 1, bufferSize);
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getDecimal64(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL64_NULL;
            max = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            maxMin = Decimals.DECIMAL64_NULL;
            max = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin = Decimals.DECIMAL64_NULL;
            max = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
            if (dequeMemory != null) {
                dequeMemory.appendAddressFor((long) dequeBufferSize * Long.BYTES);
            }
        }
    }

    public static class Decimal64MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private long value;

        public Decimal64MaxMinOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            long d = arg.getDecimal64(record);
            if (mv.isNew()) {
                if (d != Decimals.DECIMAL64_NULL) {
                    mv.putLong(0, d);
                    value = d;
                } else {
                    mv.putLong(0, Decimals.DECIMAL64_NULL);
                    value = Decimals.DECIMAL64_NULL;
                }
            } else {
                long curr = mv.getLong(0);
                if (d != Decimals.DECIMAL64_NULL && (Decimal64.isNull(curr) || comparator.isBetter(d, curr))) {
                    mv.putLong(0, d);
                    value = d;
                } else {
                    value = curr;
                }
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    public static class Decimal64MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64MaxMinOverUnboundedRowsFrameFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL && (Decimal64.isNull(value) || comparator.isBetter(d, value))) {
                value = d;
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal64MaxMinOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64MaxMinOverWholeResultSetFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL && (Decimal64.isNull(value) || comparator.isBetter(d, value))) {
                value = d;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal8MaxMinOverCurrentRowFunction extends BaseWindowFunction {

        private final String name;
        private final int type;
        private byte value;

        public Decimal8MaxMinOverCurrentRowFunction(Function arg, String name, int type) {
            super(arg);
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal8(record);
        }

        @Override
        public byte getDecimal8(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal8MaxMinOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;

        public Decimal8MaxMinOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            byte b = arg.getDecimal8(record);
            if (b == Decimals.DECIMAL8_NULL) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            if (mv.isNew()) {
                mv.putLong(0, b);
            } else {
                byte curr = (byte) mv.getLong(0);
                if (curr == Decimals.DECIMAL8_NULL || comparator.isBetter(b, curr)) {
                    mv.putLong(0, b);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            byte val = mv != null ? (byte) mv.getLong(0) : Decimals.DECIMAL8_NULL;
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal8MaxMinOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int DEQUE_RECORD_SIZE = Byte.BYTES;
        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final Decimal64Comparator comparator;
        private final LongList dequeFreeList = new LongList();
        private final int dequeInitialBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private byte maxMin;

        public Decimal8MaxMinOverPartitionRangeFrameFunction(
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
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.dequeMemory = dequeMemory;
            this.dequeInitialBufferSize = initialBufferSize;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            byte b = arg.getDecimal8(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (frameLoBounded) {
                    dequeCapacity = dequeInitialBufferSize;
                    dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * DEQUE_RECORD_SIZE) - dequeMemory.getPageAddress(0);
                }
                if (b != Decimals.DECIMAL8_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putByte(startOffset + Long.BYTES, b);
                    if (frameIncludesCurrentValue) {
                        maxMin = b;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        maxMin = Decimals.DECIMAL8_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                    if (frameLoBounded && frameIncludesCurrentValue) {
                        dequeMemory.putByte(dequeStartOffset, b);
                        dequeEndIndex++;
                    }
                } else {
                    size = 0;
                    maxMin = Decimals.DECIMAL8_NULL;
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
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                if (dequeStartIndex != dequeEndIndex &&
                                        dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE) ==
                                                memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES)) {
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

                if (b != Decimals.DECIMAL8_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, b);
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            byte value = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            while (dequeStartIndex != dequeEndIndex &&
                                    comparator.isBetter(value, dequeMemory.getByte(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * DEQUE_RECORD_SIZE))) {
                                dequeEndIndex--;
                            }
                            if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                                memoryDesc.reset(dequeCapacity, dequeStartOffset, dequeEndIndex - dequeStartIndex, dequeStartIndex, dequeFreeList);
                                expandRingBuffer(dequeMemory, memoryDesc, DEQUE_RECORD_SIZE);
                                dequeCapacity = memoryDesc.capacity;
                                dequeStartOffset = memoryDesc.startOffset;
                                dequeStartIndex = memoryDesc.firstIdx;
                                dequeEndIndex = dequeStartIndex + memoryDesc.size;
                            }
                            dequeMemory.putByte(dequeStartOffset + (dequeEndIndex % dequeCapacity) * DEQUE_RECORD_SIZE, value);
                            dequeEndIndex++;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                    if (dequeStartIndex == dequeEndIndex) {
                        maxMin = Decimals.DECIMAL8_NULL;
                    } else {
                        maxMin = dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeCapacity) * DEQUE_RECORD_SIZE);
                    }
                } else {
                    byte oldMax = (byte) mapValue.getLong(5);
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (oldMax == Decimals.DECIMAL8_NULL || comparator.isBetter(val, oldMax)) {
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
                    maxMin = oldMax;
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
                mapValue.putLong(5, maxMin);
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            super.reopen();
            maxMin = Decimals.DECIMAL8_NULL;
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal8MaxMinOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final String name;
        private final int type;
        private byte maxMin;

        public Decimal8MaxMinOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long loIdx;
            long startOffset;
            byte b = arg.getDecimal8(record);
            long dequeStartOffset = 0;
            long dequeStartIndex = 0;
            long dequeEndIndex = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && b != Decimals.DECIMAL8_NULL) {
                    maxMin = b;
                } else {
                    maxMin = Decimals.DECIMAL8_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
                if (frameLoBounded) {
                    dequeStartOffset = dequeMemory.appendAddressFor((long) dequeBufferSize * Byte.BYTES) - dequeMemory.getPageAddress(0);
                    if (b != Decimals.DECIMAL8_NULL && frameIncludesCurrentValue) {
                        dequeMemory.putByte(dequeStartOffset, b);
                        dequeEndIndex++;
                    }
                } else {
                    value.putLong(2, maxMin);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                byte hiValue = frameIncludesCurrentValue ? b : memory.getByte(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Byte.BYTES);
                if (frameLoBounded) {
                    dequeStartOffset = value.getLong(2);
                    dequeStartIndex = value.getLong(3);
                    dequeEndIndex = value.getLong(4);
                    if (hiValue != Decimals.DECIMAL8_NULL) {
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(hiValue, dequeMemory.getByte(dequeStartOffset + ((dequeEndIndex - 1) % dequeBufferSize) * Byte.BYTES))) {
                            dequeEndIndex--;
                        }
                        dequeMemory.putByte(dequeStartOffset + (dequeEndIndex % dequeBufferSize) * Byte.BYTES, hiValue);
                        dequeEndIndex++;
                        maxMin = dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Byte.BYTES);
                    } else {
                        if (dequeStartIndex != dequeEndIndex) {
                            maxMin = dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Byte.BYTES);
                        } else {
                            maxMin = Decimals.DECIMAL8_NULL;
                        }
                    }
                } else {
                    byte max = (byte) value.getLong(2);
                    if (hiValue != Decimals.DECIMAL8_NULL) {
                        if (max == Decimals.DECIMAL8_NULL || comparator.isBetter(hiValue, max)) {
                            max = hiValue;
                            value.putLong(2, max);
                        }
                    }
                    maxMin = max;
                }
                if (frameLoBounded) {
                    byte loValue = memory.getByte(startOffset + loIdx * Byte.BYTES);
                    if (loValue != Decimals.DECIMAL8_NULL && dequeStartIndex != dequeEndIndex &&
                            loValue == dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeBufferSize) * Byte.BYTES)) {
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
            memory.putByte(startOffset + loIdx * Byte.BYTES, b);
        }

        @Override
        public byte getDecimal8(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), maxMin);
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
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
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

    public static class Decimal8MaxMinOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final Decimal64Comparator comparator;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private final int type;
        private long capacity;
        private long dequeCapacity;
        private long dequeEndIndex = 0;
        private MemoryARW dequeMemory;
        private long dequeStartIndex = 0;
        private long dequeStartOffset;
        private long firstIdx;
        private long frameSize;
        private byte maxMin;
        private long size;
        private long startOffset;

        public Decimal8MaxMinOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            maxMin = Decimals.DECIMAL8_NULL;
            if (frameLoBounded) {
                this.dequeMemory = dequeMemory;
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Byte.BYTES) - dequeMemory.getPageAddress(0);
            }
            this.comparator = comparator;
            this.name = name;
            this.type = type;
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
            byte b = arg.getDecimal8(record);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            if (val != Decimals.DECIMAL8_NULL && dequeStartIndex != dequeEndIndex && val ==
                                    dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Byte.BYTES)) {
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

            if (b != Decimals.DECIMAL8_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, b);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        byte value = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        while (dequeStartIndex != dequeEndIndex &&
                                comparator.isBetter(value, dequeMemory.getByte(dequeStartOffset + ((dequeEndIndex - 1) % dequeCapacity) * Byte.BYTES))) {
                            dequeEndIndex--;
                        }
                        if (dequeEndIndex - dequeStartIndex == dequeCapacity) {
                            long newAddress = dequeMemory.appendAddressFor((dequeCapacity << 1) * Byte.BYTES);
                            long oldAddress = dequeMemory.getPageAddress(0) + dequeStartOffset;
                            if (dequeStartIndex == 0) {
                                Vect.memcpy(newAddress, oldAddress, dequeCapacity * Byte.BYTES);
                            } else {
                                dequeStartIndex %= dequeCapacity;
                                long firstPieceSize = (dequeCapacity - dequeStartIndex) * Byte.BYTES;
                                Vect.memcpy(newAddress, oldAddress + dequeStartIndex * Byte.BYTES, firstPieceSize);
                                Vect.memcpy(newAddress + firstPieceSize, oldAddress, dequeStartIndex * Byte.BYTES);
                                dequeStartIndex = 0;
                            }
                            dequeStartOffset = newAddress - dequeMemory.getPageAddress(0);
                            dequeEndIndex = dequeStartIndex + dequeCapacity;
                            dequeCapacity <<= 1;
                        }
                        dequeMemory.putByte(dequeStartOffset + (dequeEndIndex % dequeCapacity) * Byte.BYTES, value);
                        dequeEndIndex++;
                        frameSize++;
                    } else {
                        break;
                    }
                }
                if (dequeStartIndex == dequeEndIndex) {
                    maxMin = Decimals.DECIMAL8_NULL;
                } else {
                    maxMin = dequeMemory.getByte(dequeStartOffset + (dequeStartIndex % dequeCapacity) * Byte.BYTES);
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        if (maxMin == Decimals.DECIMAL8_NULL || comparator.isBetter(val, maxMin)) {
                            maxMin = val;
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
        public byte getDecimal8(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Byte.BYTES) - dequeMemory.getPageAddress(0);
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
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            if (dequeMemory != null) {
                dequeCapacity = initialCapacity;
                dequeMemory.truncate();
                dequeStartOffset = dequeMemory.appendAddressFor(dequeCapacity * Byte.BYTES) - dequeMemory.getPageAddress(0);
                dequeStartIndex = 0;
                dequeEndIndex = 0;
            }
        }
    }

    public static class Decimal8MaxMinOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64Comparator comparator;
        private final int dequeBufferSize;
        private final MemoryARW dequeMemory;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final String name;
        private final int type;
        private long count = 0;
        private long dequeEndIndex;
        private long dequeStartIndex;
        private int loIdx = 0;
        private byte max = Decimals.DECIMAL8_NULL;
        private byte maxMin;

        public Decimal8MaxMinOverRowsFrameFunction(
                Function arg,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                MemoryARW dequeMemory,
                Decimal64Comparator comparator,
                String name,
                int type
        ) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
                dequeBufferSize = (int) (rowsHi - rowsLo + 1);
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
                dequeBufferSize = 0;
            }
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.dequeMemory = dequeMemory;
            this.comparator = comparator;
            this.name = name;
            this.type = type;
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
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
            byte b = arg.getDecimal8(record);
            byte hiValue = b;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getByte((long) ((loIdx + frameSize - 1) % bufferSize) * Byte.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getByte((long) (loIdx % bufferSize) * Byte.BYTES);
            }
            if (frameLoBounded) {
                if (hiValue != Decimals.DECIMAL8_NULL) {
                    while (dequeStartIndex != dequeEndIndex &&
                            comparator.isBetter(hiValue, dequeMemory.getByte(((dequeEndIndex - 1) % dequeBufferSize) * Byte.BYTES))) {
                        dequeEndIndex--;
                    }
                    dequeMemory.putByte((dequeEndIndex % dequeBufferSize) * Byte.BYTES, hiValue);
                    dequeEndIndex++;
                    maxMin = dequeMemory.getByte((dequeStartIndex % dequeBufferSize) * Byte.BYTES);
                } else {
                    if (dequeStartIndex != dequeEndIndex) {
                        maxMin = dequeMemory.getByte((dequeStartIndex % dequeBufferSize) * Byte.BYTES);
                    } else {
                        maxMin = Decimals.DECIMAL8_NULL;
                    }
                }
            } else {
                if (hiValue != Decimals.DECIMAL8_NULL) {
                    if (max == Decimals.DECIMAL8_NULL || comparator.isBetter(hiValue, max)) {
                        max = hiValue;
                    }
                }
                maxMin = max;
            }

            if (frameLoBounded) {
                byte loValue = buffer.getByte((long) loIdx * Byte.BYTES);
                if (loValue != Decimals.DECIMAL8_NULL && dequeStartIndex != dequeEndIndex &&
                        loValue == dequeMemory.getByte((dequeStartIndex % dequeBufferSize) * Byte.BYTES)) {
                    dequeStartIndex++;
                }
            }
            count = Math.min(count + 1, bufferSize);
            buffer.putByte((long) loIdx * Byte.BYTES, b);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public byte getDecimal8(Record rec) {
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), maxMin);
        }

        @Override
        public void reopen() {
            maxMin = Decimals.DECIMAL8_NULL;
            max = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            if (dequeMemory != null) {
                dequeMemory.close();
            }
            maxMin = Decimals.DECIMAL8_NULL;
            max = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
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
            maxMin = Decimals.DECIMAL8_NULL;
            max = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
            dequeStartIndex = 0;
            dequeEndIndex = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
            if (dequeMemory != null) {
                dequeMemory.appendAddressFor((long) dequeBufferSize * Byte.BYTES);
            }
        }
    }

    public static class Decimal8MaxMinOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private byte value;

        public Decimal8MaxMinOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, Decimal64Comparator comparator, String name, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            byte b = arg.getDecimal8(record);
            if (mv.isNew()) {
                if (b != Decimals.DECIMAL8_NULL) {
                    mv.putLong(0, b);
                    value = b;
                } else {
                    mv.putLong(0, Decimals.DECIMAL8_NULL);
                    value = Decimals.DECIMAL8_NULL;
                }
            } else {
                byte curr = (byte) mv.getLong(0);
                if (b != Decimals.DECIMAL8_NULL && (curr == Decimals.DECIMAL8_NULL || comparator.isBetter(b, curr))) {
                    mv.putLong(0, b);
                    value = b;
                } else {
                    value = curr;
                }
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    public static class Decimal8MaxMinOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8MaxMinOverUnboundedRowsFrameFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL && (value == Decimals.DECIMAL8_NULL || comparator.isBetter(b, value))) {
                value = b;
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
        }
    }

    public static class Decimal8MaxMinOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal64Comparator comparator;
        private final String name;
        private final int type;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8MaxMinOverWholeResultSetFunction(Function arg, Decimal64Comparator comparator, String name, int type) {
            super(arg);
            this.comparator = comparator;
            this.name = name;
            this.type = type;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getPassCount() {
            return TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL && (value == Decimals.DECIMAL8_NULL || comparator.isBetter(b, value))) {
                value = b;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
        }
    }

    static {
        MAX_DECIMAL64_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL64_TYPES.add(ColumnType.LONG);

        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES = new ArrayColumnTypes();
        for (int i = 0; i < 9; i++) {
            MAX_DECIMAL64_OVER_PARTITION_RANGE_BOUNDED_TYPES.add(ColumnType.LONG);
        }

        MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES = new ArrayColumnTypes();
        for (int i = 0; i < 5; i++) {
            MAX_DECIMAL64_OVER_PARTITION_ROWS_BOUNDED_TYPES.add(ColumnType.LONG);
        }

        MAX_DECIMAL128_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL128_TYPES.add(ColumnType.DECIMAL128);

        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL128);

        MAX_DECIMAL128_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL128);

        MAX_DECIMAL256_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL256_TYPES.add(ColumnType.DECIMAL256);

        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL256);

        MAX_DECIMAL256_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        MAX_DECIMAL256_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        MAX_DECIMAL256_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL256);
    }
}
