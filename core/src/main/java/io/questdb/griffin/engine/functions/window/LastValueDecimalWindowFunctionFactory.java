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
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class LastValueDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES;
    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES;
    public static final ArrayColumnTypes LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_DECIMAL128_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_DECIMAL256_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES;
    public static final ArrayColumnTypes LAST_VALUE_DECIMAL64_TYPES;
    public static final String NAME = "last_value";
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
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();
        int tag = ColumnType.tagOf(argType);

        if (rowsHi < rowsLo) {
            boolean isRange = windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE;
            VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
            return switch (tag) {
                case ColumnType.DECIMAL8 ->
                        new Decimal8NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL16 ->
                        new Decimal16NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL32 ->
                        new Decimal32NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL64 ->
                        new Decimal64NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL128 ->
                        new Decimal128NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                default -> new Decimal256NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
            };
        }

        return switch (tag) {
            case ColumnType.DECIMAL8 -> windowContext.isIgnoreNulls()
                    ? generateDecimal8IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal8RespectNulls(position, args, configuration, windowContext, argType);
            case ColumnType.DECIMAL16 -> windowContext.isIgnoreNulls()
                    ? generateDecimal16IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal16RespectNulls(position, args, configuration, windowContext, argType);
            case ColumnType.DECIMAL32 -> windowContext.isIgnoreNulls()
                    ? generateDecimal32IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal32RespectNulls(position, args, configuration, windowContext, argType);
            case ColumnType.DECIMAL128 -> windowContext.isIgnoreNulls()
                    ? generateDecimal128IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal128RespectNulls(position, args, configuration, windowContext, argType);
            case ColumnType.DECIMAL256 -> windowContext.isIgnoreNulls()
                    ? generateDecimal256IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal256RespectNulls(position, args, configuration, windowContext, argType);
            case ColumnType.DECIMAL64 -> windowContext.isIgnoreNulls()
                    ? generateDecimal64IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal64RespectNulls(position, args, configuration, windowContext, argType);
            default ->
                    throw SqlException.$(position, "last_value is not yet implemented for ").put(ColumnType.nameOf(tag));
        };
    }

    private Function generateDecimal128IgnoreNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal128LastNotNullValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal128LastNotNullValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal128LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128LastNotNullValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal128LastNotNullValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal128RespectNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal128LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, true, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal128LastValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    return new Decimal128LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, false, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal128LastValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new Decimal128LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal128LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128LastValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    return new Decimal128LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal128LastValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal16IgnoreNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal16LastNotNullValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal16LastNotNullValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal16LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16LastNotNullValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal16LastNotNullValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal16RespectNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal16LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, true, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal16LastValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    return new Decimal16LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, false, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal16LastValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new Decimal16LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal16LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16LastValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    return new Decimal16LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal16LastValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal256IgnoreNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal256LastNotNullValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal256LastNotNullValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal256LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256LastNotNullValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal256LastNotNullValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal256RespectNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal256LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, true, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal256LastValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    return new Decimal256LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, false, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal256LastValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new Decimal256LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal256LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256LastValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    return new Decimal256LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal256LastValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal32IgnoreNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal32LastNotNullValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal32LastNotNullValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal32LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32LastNotNullValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal32LastNotNullValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal32RespectNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal32LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, true, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal32LastValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    return new Decimal32LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, false, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal32LastValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new Decimal32LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal32LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32LastValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    return new Decimal32LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal32LastValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal64IgnoreNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal64LastNotNullValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal64LastNotNullValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal64LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64LastNotNullValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal64LastNotNullValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal64RespectNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal64LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, true, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal64LastValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    return new Decimal64LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, false, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal64LastValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new Decimal64LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal64LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64LastValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    return new Decimal64LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal64LastValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal8IgnoreNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal8LastNotNullValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8LastNotNullValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8LastNotNullValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal8LastNotNullValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal8LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8LastNotNullValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8LastNotNullOverUnboundedRowsFrameFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8LastNotNullValueOverCurrentRowFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8LastNotNullValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal8LastNotNullValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function generateDecimal8RespectNulls(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int argType
    ) throws SqlException {
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal8LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, true, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal8LastValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8LastValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsHi == 0) {
                    return new Decimal8LastValueIncludeCurrentPartitionRowsFrameFunction(rowsLo, false, partitionByRecord, partitionBySink, args.get(0), argType);
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal8LastValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                            rowsLo, rowsHi, args.get(0), mem, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    return new Decimal8LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    return new Decimal8LastValueIncludeCurrentFrameFunction(rowsLo, true, args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8LastValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8LastValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsHi == 0) {
                    return new Decimal8LastValueIncludeCurrentFrameFunction(rowsLo, false, args.get(0), argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal8LastValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    public static class Decimal128LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal128 lastValue = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LastNotNullOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            lastValue.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                lastValue.copyFrom(scratch);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            lastValue.ofRawNull();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
        }
    }

    static class Decimal128LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private final Decimal128 value = new Decimal128();

        Decimal128LastNotNullValueOverCurrentRowFunction(Function arg, int type) {
            super(arg);
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
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }
    }

    static class Decimal128LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                arg.getDecimal128(record, scratch);
                if (!scratch.isNull()) {
                    MapValue value = key.createValue();
                    value.putDecimal128(0, scratch);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (value != null) {
                value.getDecimal128(0, scratch);
                Unsafe.putLong(addr, scratch.getHigh());
                Unsafe.putLong(addr + Long.BYTES, scratch.getLow());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    public static class Decimal128LastNotNullValueOverPartitionRangeFrameFunction extends Decimal128LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public Decimal128LastNotNullValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, type);
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal128(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!scratch.isNull()) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    size = 1;
                    if (frameIncludesCurrentValue) {
                        lastValue.copyFrom(scratch);
                    } else {
                        lastValue.ofRawNull();
                    }
                } else {
                    lastValue.ofRawNull();
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    memory.getDecimal128(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
                } else {
                    lastValue.ofRawNull();
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal128LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal128 cacheValue = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal128 last = new Decimal128();
        private final Decimal128 lastValue = new Decimal128();
        private final MemoryARW memory;
        private final Decimal128 nullScratch = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final Decimal128 slotValue = new Decimal128();
        private final int type;

        public Decimal128LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            lastValue.ofRawNull();
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

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * 16L) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !scratch.isNull()) {
                    lastValue.copyFrom(scratch);
                } else {
                    lastValue.ofRawNull();
                }
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal128(startOffset + (long) i * 16L, nullScratch.getHigh(), nullScratch.getLow());
                }
            } else {
                value.getDecimal128(0, lastValue);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (!scratch.isNull() && frameIncludesCurrentValue) {
                    lastValue.copyFrom(scratch);
                } else if (frameLoBounded) {
                    memory.getDecimal128(startOffset + (loIdx + frameSize - 1) % bufferSize * 16L, last);
                    if (!last.isNull()) {
                        lastValue.copyFrom(last);
                    } else if (lastValue.isNull()) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            memory.getDecimal128(startOffset + (loIdx + i) % bufferSize * 16L, last);
                            if (!last.isNull()) {
                                lastValue.copyFrom(last);
                                break;
                            }
                        }
                    }
                } else {
                    memory.getDecimal128(startOffset + loIdx % bufferSize * 16L, last);
                    if (!last.isNull()) {
                        lastValue.copyFrom(last);
                    } else {
                        value.getDecimal128(0, lastValue);
                    }
                }
            }

            cacheValue.copyFrom(lastValue);
            if (frameLoBounded) {
                memory.getDecimal128(startOffset + loIdx % bufferSize * 16L, slotValue);
                if (!slotValue.isNull() && slotValue.getHigh() == lastValue.getHigh() && slotValue.getLow() == lastValue.getLow()) {
                    cacheValue.ofRawNull();
                }
            }
            value.putDecimal128(0, cacheValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);
            memory.putDecimal128(startOffset + loIdx * 16L, scratch.getHigh(), scratch.getLow());
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue.ofRawNull();
        }
    }

    public static class Decimal128LastNotNullValueOverRangeFrameFunction extends Decimal128LastValueOverRangeFrameFunction {

        public Decimal128LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, type);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
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
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDecimal128(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHigh(), scratch.getLow());
                size++;
            }

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                memory.getDecimal128(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
            } else {
                lastValue.ofRawNull();
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal128LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal128 cacheValue = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal128 lastValue = new Decimal128();
        private final Decimal128 nullScratch = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final Decimal128 slotValue = new Decimal128();
        private final int type;
        private int loIdx = 0;

        public Decimal128LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            this.type = type;
            cacheValue.ofRawNull();
            lastValue.ofRawNull();
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            lastValue.copyFrom(cacheValue);
            if (!scratch.isNull() && frameIncludesCurrentValue) {
                lastValue.copyFrom(scratch);
            } else if (frameLoBounded) {
                buffer.getDecimal128((long) (loIdx + frameSize - 1) % bufferSize * 16L, slotValue);
                if (!slotValue.isNull()) {
                    lastValue.copyFrom(slotValue);
                } else if (lastValue.isNull()) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        buffer.getDecimal128((long) (loIdx + i) % bufferSize * 16L, slotValue);
                        if (!slotValue.isNull()) {
                            lastValue.copyFrom(slotValue);
                            break;
                        }
                    }
                }
            } else {
                buffer.getDecimal128((long) loIdx % bufferSize * 16L, slotValue);
                if (!slotValue.isNull()) {
                    lastValue.copyFrom(slotValue);
                } else {
                    lastValue.copyFrom(cacheValue);
                }
            }

            cacheValue.copyFrom(lastValue);
            if (frameLoBounded) {
                buffer.getDecimal128((long) loIdx % bufferSize * 16L, slotValue);
                if (!slotValue.isNull() && slotValue.getHigh() == lastValue.getHigh() && slotValue.getLow() == lastValue.getLow()) {
                    cacheValue.ofRawNull();
                }
            }
            buffer.putDecimal128((long) loIdx * 16L, scratch.getHigh(), scratch.getLow());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reopen() {
            lastValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue.ofRawNull();
            cacheValue.ofRawNull();
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
            cacheValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal128((long) i * 16L, nullScratch.getHigh(), nullScratch.getLow());
            }
        }
    }

    public static class Decimal128LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            arg.getDecimal128(record, scratch);
            if (mapValue.isNew()) {
                mapValue.putDecimal128(0, scratch);
                value.copyFrom(scratch);
            } else {
                if (!scratch.isNull()) {
                    mapValue.putDecimal128(0, scratch);
                    value.copyFrom(scratch);
                } else {
                    mapValue.getDecimal128(0, value);
                }
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
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
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    public static class Decimal128LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();
        private boolean found;

        public Decimal128LastNotNullValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                arg.getDecimal128(record, scratch);
                if (!scratch.isNull()) {
                    found = true;
                    value.copyFrom(scratch);
                }
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            found = false;
        }
    }

    public static class Decimal128LastValueIncludeCurrentFrameFunction extends BaseWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg, int type) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
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
            return NAME;
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

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
        }
    }

    public static class Decimal128LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
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
            return NAME;
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

    static class Decimal128LastValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                arg.getDecimal128(record, scratch);
                value.putDecimal128(0, scratch);
            } else {
                value.getDecimal128(0, scratch);
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, scratch.getHigh());
            Unsafe.putLong(addr + Long.BYTES, scratch.getLow());
        }
    }

    public static class Decimal128LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + 16;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final Decimal128 lastValue = new Decimal128();
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final Decimal128 scratch = new Decimal128();
        protected final int timestampIndex;
        protected final int type;

        public Decimal128LastValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.type = type;
            lastValue.ofRawNull();
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal128(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                size = 1;
                lastValue.ofRawNull();
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    memory.getDecimal128(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
                } else {
                    lastValue.ofRawNull();
                }

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

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue.ofRawNull();
        }
    }

    public static class Decimal128LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal128 lastValue = new Decimal128();
        private final MemoryARW memory;
        private final Decimal128 nullScratch = new Decimal128();
        private final long rowLo;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.type = type;
            lastValue.ofRawNull();
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
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * 16L) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal128(startOffset + (long) i * 16L, nullScratch.getHigh(), nullScratch.getLow());
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            memory.getDecimal128(startOffset + loIdx % bufferSize * 16L, lastValue);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putDecimal128(startOffset + loIdx % bufferSize * 16L, scratch.getHigh(), scratch.getLow());
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue.ofRawNull();
        }
    }

    public static class Decimal128LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        protected static final int RECORD_SIZE = Long.BYTES + 16;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final Decimal128 lastValue = new Decimal128();
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final Decimal128 scratch = new Decimal128();
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long size;
        protected long startOffset;

        public Decimal128LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
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
            this.type = type;
            lastValue.ofRawNull();
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                memory.getDecimal128(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
            } else {
                lastValue.ofRawNull();
            }

            if (size == capacity) {
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                long oldAddress = memory.getPageAddress(0) + startOffset;
                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }
                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putDecimal128(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHigh(), scratch.getLow());
            size++;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reopen() {
            lastValue.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    public static class Decimal128LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal128 lastValue = new Decimal128();
        private final Decimal128 nullScratch = new Decimal128();
        private final long rowsLo;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private int loIdx = 0;

        public Decimal128LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            this.type = type;
            lastValue.ofRawNull();
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            buffer.getDecimal128((long) loIdx * 16L, lastValue);
            arg.getDecimal128(record, scratch);
            buffer.putDecimal128((long) loIdx * 16L, scratch.getHigh(), scratch.getLow());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getLow());
        }

        @Override
        public void reopen() {
            lastValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue.ofRawNull();
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal128((long) i * 16L, nullScratch.getHigh(), nullScratch.getLow());
            }
        }
    }

    public static class Decimal128LastValueOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();
        private boolean found;

        public Decimal128LastValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                found = true;
                arg.getDecimal128(record, scratch);
                value.copyFrom(scratch);
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            found = false;
        }
    }

    public static class Decimal16LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final int type;
        private short lastValue = Decimals.DECIMAL16_NULL;

        public Decimal16LastNotNullOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                lastValue = s;
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reset() {
            super.reset();
            lastValue = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL16_NULL;
        }
    }

    static class Decimal16LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private short value = Decimals.DECIMAL16_NULL;

        Decimal16LastNotNullValueOverCurrentRowFunction(Function arg, int type) {
            super(arg);
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
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal16LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal16LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                short s = arg.getDecimal16(record);
                if (s != Decimals.DECIMAL16_NULL) {
                    MapValue value = key.createValue();
                    value.putLong(0, s);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            short val = value != null ? (short) value.getLong(0) : Decimals.DECIMAL16_NULL;
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal16LastNotNullValueOverPartitionRangeFrameFunction extends Decimal16LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public Decimal16LastNotNullValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, type);
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            short s = arg.getDecimal16(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (s != Decimals.DECIMAL16_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putShort(startOffset + Long.BYTES, s);
                    size = 1;
                    lastValue = frameIncludesCurrentValue ? s : Decimals.DECIMAL16_NULL;
                } else {
                    lastValue = Decimals.DECIMAL16_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getShort(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL16_NULL;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal16LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int type;
        private short lastValue = Decimals.DECIMAL16_NULL;

        public Decimal16LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
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

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && s != Decimals.DECIMAL16_NULL) {
                    lastValue = s;
                } else {
                    lastValue = Decimals.DECIMAL16_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
            } else {
                lastValue = (short) value.getLong(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (s != Decimals.DECIMAL16_NULL && frameIncludesCurrentValue) {
                    lastValue = s;
                } else if (frameLoBounded) {
                    short last = memory.getShort(startOffset + (loIdx + frameSize - 1) % bufferSize * Short.BYTES);
                    if (last != Decimals.DECIMAL16_NULL) {
                        lastValue = last;
                    } else if (lastValue == Decimals.DECIMAL16_NULL) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            short v = memory.getShort(startOffset + (loIdx + i) % bufferSize * Short.BYTES);
                            if (v != Decimals.DECIMAL16_NULL) {
                                lastValue = v;
                                break;
                            }
                        }
                    }
                } else {
                    short last = memory.getShort(startOffset + loIdx % bufferSize * Short.BYTES);
                    if (last != Decimals.DECIMAL16_NULL) {
                        lastValue = last;
                    } else {
                        lastValue = (short) value.getLong(0);
                    }
                }
            }

            short nextLastValue = lastValue;
            if (frameLoBounded && memory.getShort(startOffset + loIdx % bufferSize * Short.BYTES) == lastValue) {
                nextLastValue = Decimals.DECIMAL16_NULL;
            }
            value.putLong(0, nextLastValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);
            memory.putShort(startOffset + loIdx * Short.BYTES, s);
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal16LastNotNullValueOverRangeFrameFunction extends Decimal16LastValueOverRangeFrameFunction {

        public Decimal16LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, type);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
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
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, s);
                size++;
            }

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getShort(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL16_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal16LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int type;
        private short cacheValue = Decimals.DECIMAL16_NULL;
        private short lastValue = Decimals.DECIMAL16_NULL;
        private int loIdx = 0;

        public Decimal16LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            lastValue = cacheValue;
            if (s != Decimals.DECIMAL16_NULL && frameIncludesCurrentValue) {
                lastValue = s;
            } else if (frameLoBounded) {
                short last = buffer.getShort((long) (loIdx + frameSize - 1) % bufferSize * Short.BYTES);
                if (last != Decimals.DECIMAL16_NULL) {
                    lastValue = last;
                } else if (lastValue == Decimals.DECIMAL16_NULL) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        short v = buffer.getShort((long) (loIdx + i) % bufferSize * Short.BYTES);
                        if (v != Decimals.DECIMAL16_NULL) {
                            lastValue = v;
                            break;
                        }
                    }
                }
            } else {
                short last = buffer.getShort((long) loIdx % bufferSize * Short.BYTES);
                if (last != Decimals.DECIMAL16_NULL) {
                    lastValue = last;
                } else {
                    lastValue = cacheValue;
                }
            }

            cacheValue = lastValue;
            if (frameLoBounded && buffer.getShort((long) loIdx % bufferSize * Short.BYTES) == lastValue) {
                cacheValue = Decimals.DECIMAL16_NULL;
            }
            buffer.putShort((long) loIdx * Short.BYTES, s);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL16_NULL;
            cacheValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL16_NULL;
            cacheValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
        }
    }

    public static class Decimal16LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int type;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            short s = arg.getDecimal16(record);
            if (mapValue.isNew()) {
                mapValue.putLong(0, s);
                value = s;
            } else {
                if (s != Decimals.DECIMAL16_NULL) {
                    mapValue.putLong(0, s);
                    value = s;
                } else {
                    value = (short) mapValue.getLong(0);
                }
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

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

    public static class Decimal16LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16LastNotNullValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                short s = arg.getDecimal16(record);
                if (s != Decimals.DECIMAL16_NULL) {
                    found = true;
                    value = s;
                }
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL16_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
            found = false;
        }
    }

    public static class Decimal16LastValueIncludeCurrentFrameFunction extends BaseWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg, int type) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
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
            return NAME;
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

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal16LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
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
            return NAME;
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

    static class Decimal16LastValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal16LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            short val;
            if (value.isNew()) {
                short s = arg.getDecimal16(record);
                value.putLong(0, s);
                val = s;
            } else {
                val = (short) value.getLong(0);
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal16LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected short lastValue = Decimals.DECIMAL16_NULL;

        public Decimal16LastValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            short s = arg.getDecimal16(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putShort(startOffset + Long.BYTES, s);
                size = 1;
                lastValue = Decimals.DECIMAL16_NULL;
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getShort(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL16_NULL;
                }

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

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal16LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final MemoryARW memory;
        private final long rowLo;
        private final int type;
        private short lastValue = Decimals.DECIMAL16_NULL;

        public Decimal16LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.type = type;
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
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            lastValue = memory.getShort(startOffset + loIdx % bufferSize * Short.BYTES);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putShort(startOffset + loIdx % bufferSize * Short.BYTES, s);
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal16LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        protected static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected short lastValue = Decimals.DECIMAL16_NULL;
        protected long size;
        protected long startOffset;

        public Decimal16LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getShort(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL16_NULL;
            }

            if (size == capacity) {
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                long oldAddress = memory.getPageAddress(0) + startOffset;
                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }
                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, s);
            size++;
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    public static class Decimal16LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        private final int type;
        private short lastValue = Decimals.DECIMAL16_NULL;
        private int loIdx = 0;

        public Decimal16LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getShort((long) loIdx * Short.BYTES);
            buffer.putShort((long) loIdx * Short.BYTES, arg.getDecimal16(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public short getDecimal16(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
        }
    }

    public static class Decimal16LastValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private short value = Decimals.DECIMAL16_NULL;

        public Decimal16LastValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                found = true;
                value = arg.getDecimal16(record);
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL16_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
            found = false;
        }
    }

    public static class Decimal256LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 lastValue = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LastNotNullOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            lastValue.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                lastValue.copyRaw(scratch);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reset() {
            super.reset();
            lastValue.ofRawNull();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
        }
    }

    static class Decimal256LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private final Decimal256 value = new Decimal256();

        Decimal256LastNotNullValueOverCurrentRowFunction(Function arg, int type) {
            super(arg);
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
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
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

    static class Decimal256LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                arg.getDecimal256(record, scratch);
                if (!scratch.isNull()) {
                    MapValue value = key.createValue();
                    value.putDecimal256(0, scratch);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (value != null) {
                value.getDecimal256(0, scratch);
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
    }

    public static class Decimal256LastNotNullValueOverPartitionRangeFrameFunction extends Decimal256LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public Decimal256LastNotNullValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, type);
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal256(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!scratch.isNull()) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal256(startOffset + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                    size = 1;
                    if (frameIncludesCurrentValue) {
                        lastValue.copyRaw(scratch);
                    } else {
                        lastValue.ofRawNull();
                    }
                } else {
                    lastValue.ofRawNull();
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    memory.getDecimal256(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
                } else {
                    lastValue.ofRawNull();
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal256LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal256 cacheValue = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal256 last = new Decimal256();
        private final Decimal256 lastValue = new Decimal256();
        private final MemoryARW memory;
        private final Decimal256 nullScratch = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final Decimal256 slotValue = new Decimal256();
        private final int type;

        public Decimal256LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            lastValue.ofRawNull();
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

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * 32L) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !scratch.isNull()) {
                    lastValue.copyRaw(scratch);
                } else {
                    lastValue.ofRawNull();
                }
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal256(startOffset + (long) i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
                }
            } else {
                value.getDecimal256(0, lastValue);
                loIdx = value.getLong(2);
                startOffset = value.getLong(3);
                if (!scratch.isNull() && frameIncludesCurrentValue) {
                    lastValue.copyRaw(scratch);
                } else if (frameLoBounded) {
                    memory.getDecimal256(startOffset + (loIdx + frameSize - 1) % bufferSize * 32L, last);
                    if (!last.isNull()) {
                        lastValue.copyRaw(last);
                    } else if (lastValue.isNull()) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            memory.getDecimal256(startOffset + (loIdx + i) % bufferSize * 32L, last);
                            if (!last.isNull()) {
                                lastValue.copyRaw(last);
                                break;
                            }
                        }
                    }
                } else {
                    memory.getDecimal256(startOffset + loIdx % bufferSize * 32L, last);
                    if (!last.isNull()) {
                        lastValue.copyRaw(last);
                    } else {
                        value.getDecimal256(0, lastValue);
                    }
                }
            }

            cacheValue.copyRaw(lastValue);
            if (frameLoBounded) {
                memory.getDecimal256(startOffset + loIdx % bufferSize * 32L, slotValue);
                if (!slotValue.isNull()
                        && slotValue.getHh() == lastValue.getHh()
                        && slotValue.getHl() == lastValue.getHl()
                        && slotValue.getLh() == lastValue.getLh()
                        && slotValue.getLl() == lastValue.getLl()) {
                    cacheValue.ofRawNull();
                }
            }
            value.putDecimal256(0, cacheValue);
            value.putLong(2, (loIdx + 1) % bufferSize);
            value.putLong(3, startOffset);
            memory.putDecimal256(startOffset + loIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue.ofRawNull();
        }
    }

    public static class Decimal256LastNotNullValueOverRangeFrameFunction extends Decimal256LastValueOverRangeFrameFunction {

        public Decimal256LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, type);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
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
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDecimal256(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                size++;
            }

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                memory.getDecimal256(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
            } else {
                lastValue.ofRawNull();
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal256LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 cacheValue = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final Decimal256 lastValue = new Decimal256();
        private final Decimal256 nullScratch = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final Decimal256 slotValue = new Decimal256();
        private final int type;
        private int loIdx = 0;

        public Decimal256LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            this.type = type;
            cacheValue.ofRawNull();
            lastValue.ofRawNull();
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            lastValue.copyRaw(cacheValue);
            if (!scratch.isNull() && frameIncludesCurrentValue) {
                lastValue.copyRaw(scratch);
            } else if (frameLoBounded) {
                buffer.getDecimal256((long) (loIdx + frameSize - 1) % bufferSize * 32L, slotValue);
                if (!slotValue.isNull()) {
                    lastValue.copyRaw(slotValue);
                } else if (lastValue.isNull()) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        buffer.getDecimal256((long) (loIdx + i) % bufferSize * 32L, slotValue);
                        if (!slotValue.isNull()) {
                            lastValue.copyRaw(slotValue);
                            break;
                        }
                    }
                }
            } else {
                buffer.getDecimal256((long) loIdx % bufferSize * 32L, slotValue);
                if (!slotValue.isNull()) {
                    lastValue.copyRaw(slotValue);
                } else {
                    lastValue.copyRaw(cacheValue);
                }
            }

            cacheValue.copyRaw(lastValue);
            if (frameLoBounded) {
                buffer.getDecimal256((long) loIdx % bufferSize * 32L, slotValue);
                if (!slotValue.isNull()
                        && slotValue.getHh() == lastValue.getHh()
                        && slotValue.getHl() == lastValue.getHl()
                        && slotValue.getLh() == lastValue.getLh()
                        && slotValue.getLl() == lastValue.getLl()) {
                    cacheValue.ofRawNull();
                }
            }
            buffer.putDecimal256((long) loIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reopen() {
            lastValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue.ofRawNull();
            cacheValue.ofRawNull();
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
            cacheValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal256((long) i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
            }
        }
    }

    public static class Decimal256LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            arg.getDecimal256(record, scratch);
            if (mapValue.isNew()) {
                mapValue.putDecimal256(0, scratch);
                value.copyRaw(scratch);
            } else {
                if (!scratch.isNull()) {
                    mapValue.putDecimal256(0, scratch);
                    value.copyRaw(scratch);
                } else {
                    mapValue.getDecimal256(0, value);
                }
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
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
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    public static class Decimal256LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private boolean found;

        public Decimal256LastNotNullValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                arg.getDecimal256(record, scratch);
                if (!scratch.isNull()) {
                    found = true;
                    value.copyRaw(scratch);
                }
            }
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
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            found = false;
        }
    }

    public static class Decimal256LastValueIncludeCurrentFrameFunction extends BaseWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg, int type) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
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
            return NAME;
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

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
        }
    }

    public static class Decimal256LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
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
            return NAME;
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

    static class Decimal256LastValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                arg.getDecimal256(record, scratch);
                value.putDecimal256(0, scratch);
            } else {
                value.getDecimal256(0, scratch);
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, scratch.getHh());
            Unsafe.putLong(addr + Long.BYTES, scratch.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, scratch.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, scratch.getLl());
        }
    }

    public static class Decimal256LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + 32;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final Decimal256 lastValue = new Decimal256();
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final Decimal256 scratch = new Decimal256();
        protected final int timestampIndex;
        protected final int type;

        public Decimal256LastValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.type = type;
            lastValue.ofRawNull();
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal256(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putDecimal256(startOffset + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                size = 1;
                lastValue.ofRawNull();
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    memory.getDecimal256(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
                } else {
                    lastValue.ofRawNull();
                }

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

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue.ofRawNull();
        }
    }

    public static class Decimal256LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal256 lastValue = new Decimal256();
        private final MemoryARW memory;
        private final Decimal256 nullScratch = new Decimal256();
        private final long rowLo;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.type = type;
            lastValue.ofRawNull();
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
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * 32L) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal256(startOffset + (long) i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            memory.getDecimal256(startOffset + loIdx % bufferSize * 32L, lastValue);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putDecimal256(startOffset + loIdx % bufferSize * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue.ofRawNull();
        }
    }

    public static class Decimal256LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        protected static final int RECORD_SIZE = Long.BYTES + 32;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final Decimal256 lastValue = new Decimal256();
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final Decimal256 scratch = new Decimal256();
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long size;
        protected long startOffset;

        public Decimal256LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
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
            this.type = type;
            lastValue.ofRawNull();
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                memory.getDecimal256(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, lastValue);
            } else {
                lastValue.ofRawNull();
            }

            if (size == capacity) {
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                long oldAddress = memory.getPageAddress(0) + startOffset;
                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }
                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putDecimal256(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            size++;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reopen() {
            lastValue.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    public static class Decimal256LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 lastValue = new Decimal256();
        private final Decimal256 nullScratch = new Decimal256();
        private final long rowsLo;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private int loIdx = 0;

        public Decimal256LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            this.type = type;
            lastValue.ofRawNull();
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            buffer.getDecimal256((long) loIdx * 32L, lastValue);
            arg.getDecimal256(record, scratch);
            buffer.putDecimal256((long) loIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lastValue);
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(addr, lastValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lastValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lastValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lastValue.getLl());
        }

        @Override
        public void reopen() {
            lastValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue.ofRawNull();
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue.ofRawNull();
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal256((long) i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
            }
        }
    }

    public static class Decimal256LastValueOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private boolean found;

        public Decimal256LastValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                found = true;
                arg.getDecimal256(record, scratch);
                value.copyRaw(scratch);
            }
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
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            found = false;
        }
    }

    public static class Decimal32LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final int type;
        private int lastValue = Decimals.DECIMAL32_NULL;

        public Decimal32LastNotNullOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                lastValue = i;
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reset() {
            super.reset();
            lastValue = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL32_NULL;
        }
    }

    static class Decimal32LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private int value = Decimals.DECIMAL32_NULL;

        Decimal32LastNotNullValueOverCurrentRowFunction(Function arg, int type) {
            super(arg);
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
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal32LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal32LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                int i = arg.getDecimal32(record);
                if (i != Decimals.DECIMAL32_NULL) {
                    MapValue value = key.createValue();
                    value.putLong(0, i);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            int val = value != null ? (int) value.getLong(0) : Decimals.DECIMAL32_NULL;
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal32LastNotNullValueOverPartitionRangeFrameFunction extends Decimal32LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public Decimal32LastNotNullValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, type);
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            int i = arg.getDecimal32(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (i != Decimals.DECIMAL32_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putInt(startOffset + Long.BYTES, i);
                    size = 1;
                    lastValue = frameIncludesCurrentValue ? i : Decimals.DECIMAL32_NULL;
                } else {
                    lastValue = Decimals.DECIMAL32_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
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

                long lastIndex = -1;
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        lastIndex = (int) (idx % capacity);
                        size--;
                    } else {
                        break;
                    }
                }

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getInt(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL32_NULL;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal32LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int type;
        private int lastValue = Decimals.DECIMAL32_NULL;

        public Decimal32LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
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

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && i != Decimals.DECIMAL32_NULL) {
                    lastValue = i;
                } else {
                    lastValue = Decimals.DECIMAL32_NULL;
                }
                for (int j = 0; j < bufferSize; j++) {
                    memory.putInt(startOffset + (long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                lastValue = (int) value.getLong(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (i != Decimals.DECIMAL32_NULL && frameIncludesCurrentValue) {
                    lastValue = i;
                } else if (frameLoBounded) {
                    int last = memory.getInt(startOffset + (loIdx + frameSize - 1) % bufferSize * Integer.BYTES);
                    if (last != Decimals.DECIMAL32_NULL) {
                        lastValue = last;
                    } else if (lastValue == Decimals.DECIMAL32_NULL) {
                        for (int j = frameSize - 2; 0 <= j; j--) {
                            int v = memory.getInt(startOffset + (loIdx + j) % bufferSize * Integer.BYTES);
                            if (v != Decimals.DECIMAL32_NULL) {
                                lastValue = v;
                                break;
                            }
                        }
                    }
                } else {
                    int last = memory.getInt(startOffset + loIdx % bufferSize * Integer.BYTES);
                    if (last != Decimals.DECIMAL32_NULL) {
                        lastValue = last;
                    } else {
                        lastValue = (int) value.getLong(0);
                    }
                }
            }

            int nextLastValue = lastValue;
            if (frameLoBounded && memory.getInt(startOffset + loIdx % bufferSize * Integer.BYTES) == lastValue) {
                nextLastValue = Decimals.DECIMAL32_NULL;
            }
            value.putLong(0, nextLastValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);
            memory.putInt(startOffset + loIdx * Integer.BYTES, i);
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal32LastNotNullValueOverRangeFrameFunction extends Decimal32LastValueOverRangeFrameFunction {

        public Decimal32LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, type);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            if (frameLoBounded) {
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, i);
                size++;
            }

            long lastIndex = -1;
            for (long j = 0, n = size; j < n; j++) {
                long idx = (firstIdx + j) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) >= minDiff) {
                    lastIndex = (int) (idx % capacity);
                    size--;
                } else {
                    break;
                }
            }

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getInt(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL32_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal32LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int type;
        private int cacheValue = Decimals.DECIMAL32_NULL;
        private int lastValue = Decimals.DECIMAL32_NULL;
        private int loIdx = 0;

        public Decimal32LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            lastValue = cacheValue;
            if (i != Decimals.DECIMAL32_NULL && frameIncludesCurrentValue) {
                lastValue = i;
            } else if (frameLoBounded) {
                int last = buffer.getInt((long) (loIdx + frameSize - 1) % bufferSize * Integer.BYTES);
                if (last != Decimals.DECIMAL32_NULL) {
                    lastValue = last;
                } else if (lastValue == Decimals.DECIMAL32_NULL) {
                    for (int j = frameSize - 2; 0 <= j; j--) {
                        int v = buffer.getInt((long) (loIdx + j) % bufferSize * Integer.BYTES);
                        if (v != Decimals.DECIMAL32_NULL) {
                            lastValue = v;
                            break;
                        }
                    }
                }
            } else {
                int last = buffer.getInt((long) loIdx % bufferSize * Integer.BYTES);
                if (last != Decimals.DECIMAL32_NULL) {
                    lastValue = last;
                } else {
                    lastValue = cacheValue;
                }
            }

            cacheValue = lastValue;
            if (frameLoBounded && buffer.getInt((long) loIdx % bufferSize * Integer.BYTES) == lastValue) {
                cacheValue = Decimals.DECIMAL32_NULL;
            }
            buffer.putInt((long) loIdx * Integer.BYTES, i);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL32_NULL;
            cacheValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL32_NULL;
            cacheValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int j = 0; j < bufferSize; j++) {
                buffer.putInt((long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
            }
        }
    }

    public static class Decimal32LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int type;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            int i = arg.getDecimal32(record);
            if (mapValue.isNew()) {
                mapValue.putLong(0, i);
                value = i;
            } else {
                if (i != Decimals.DECIMAL32_NULL) {
                    mapValue.putLong(0, i);
                    value = i;
                } else {
                    value = (int) mapValue.getLong(0);
                }
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

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

    public static class Decimal32LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32LastNotNullValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                int i = arg.getDecimal32(record);
                if (i != Decimals.DECIMAL32_NULL) {
                    found = true;
                    value = i;
                }
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL32_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
            found = false;
        }
    }

    public static class Decimal32LastValueIncludeCurrentFrameFunction extends BaseWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg, int type) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
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
            return NAME;
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

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal32LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
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
            return NAME;
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

    static class Decimal32LastValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal32LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            int val;
            if (value.isNew()) {
                int i = arg.getDecimal32(record);
                value.putLong(0, i);
                val = i;
            } else {
                val = (int) value.getLong(0);
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal32LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected int lastValue = Decimals.DECIMAL32_NULL;

        public Decimal32LastValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            int i = arg.getDecimal32(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putInt(startOffset + Long.BYTES, i);
                size = 1;
                lastValue = Decimals.DECIMAL32_NULL;
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
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

                long lastIndex = -1;
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        lastIndex = (int) (idx % capacity);
                        size--;
                    } else {
                        break;
                    }
                }

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getInt(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL32_NULL;
                }

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

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal32LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final MemoryARW memory;
        private final long rowLo;
        private final int type;
        private int lastValue = Decimals.DECIMAL32_NULL;

        public Decimal32LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.type = type;
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
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int j = 0; j < bufferSize; j++) {
                    memory.putInt(startOffset + (long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            lastValue = memory.getInt(startOffset + loIdx % bufferSize * Integer.BYTES);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putInt(startOffset + loIdx % bufferSize * Integer.BYTES, i);
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal32LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        protected static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected int lastValue = Decimals.DECIMAL32_NULL;
        protected long size;
        protected long startOffset;

        public Decimal32LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            long lastIndex = -1;
            for (long j = 0, n = size; j < n; j++) {
                long idx = (firstIdx + j) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) >= minDiff) {
                    lastIndex = (int) (idx % capacity);
                    size--;
                } else {
                    break;
                }
            }

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getInt(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL32_NULL;
            }

            if (size == capacity) {
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                long oldAddress = memory.getPageAddress(0) + startOffset;
                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }
                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, i);
            size++;
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    public static class Decimal32LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        private final int type;
        private int lastValue = Decimals.DECIMAL32_NULL;
        private int loIdx = 0;

        public Decimal32LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getInt((long) loIdx * Integer.BYTES);
            buffer.putInt((long) loIdx * Integer.BYTES, arg.getDecimal32(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public int getDecimal32(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int j = 0; j < bufferSize; j++) {
                buffer.putInt((long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
            }
        }
    }

    public static class Decimal32LastValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private int value = Decimals.DECIMAL32_NULL;

        public Decimal32LastValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                found = true;
                value = arg.getDecimal32(record);
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL32_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
            found = false;
        }
    }

    public static class Decimal64LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final int type;
        private long lastValue = Decimals.DECIMAL64_NULL;

        public Decimal64LastNotNullOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                lastValue = d;
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reset() {
            super.reset();
            lastValue = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL64_NULL;
        }
    }

    static class Decimal64LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private long value = Decimals.DECIMAL64_NULL;

        Decimal64LastNotNullValueOverCurrentRowFunction(Function arg, int type) {
            super(arg);
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
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal64LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal64LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                long d = arg.getDecimal64(record);
                if (d != Decimals.DECIMAL64_NULL) {
                    MapValue value = key.createValue();
                    value.putLong(0, d);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            long val = value != null ? value.getLong(0) : Decimals.DECIMAL64_NULL;
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal64LastNotNullValueOverPartitionRangeFrameFunction extends Decimal64LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public Decimal64LastNotNullValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, type);
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getDecimal64(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Decimals.DECIMAL64_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    size = 1;
                    lastValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
                } else {
                    lastValue = Decimals.DECIMAL64_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL64_NULL;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal64LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int type;
        private long lastValue = Decimals.DECIMAL64_NULL;

        public Decimal64LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
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

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && d != Decimals.DECIMAL64_NULL) {
                    lastValue = d;
                } else {
                    lastValue = Decimals.DECIMAL64_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                lastValue = value.getLong(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (d != Decimals.DECIMAL64_NULL && frameIncludesCurrentValue) {
                    lastValue = d;
                } else if (frameLoBounded) {
                    long last = memory.getLong(startOffset + (loIdx + frameSize - 1) % bufferSize * Long.BYTES);
                    if (last != Decimals.DECIMAL64_NULL) {
                        lastValue = last;
                    } else if (lastValue == Decimals.DECIMAL64_NULL) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            long v = memory.getLong(startOffset + (loIdx + i) % bufferSize * Long.BYTES);
                            if (v != Decimals.DECIMAL64_NULL) {
                                lastValue = v;
                                break;
                            }
                        }
                    }
                } else {
                    long last = memory.getLong(startOffset + loIdx % bufferSize * Long.BYTES);
                    if (last != Decimals.DECIMAL64_NULL) {
                        lastValue = last;
                    } else {
                        lastValue = value.getLong(0);
                    }
                }
            }

            long nextLastValue = lastValue;
            if (frameLoBounded && memory.getLong(startOffset + loIdx % bufferSize * Long.BYTES) == lastValue) {
                nextLastValue = Decimals.DECIMAL64_NULL;
            }
            value.putLong(0, nextLastValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal64LastNotNullValueOverRangeFrameFunction extends Decimal64LastValueOverRangeFrameFunction {

        public Decimal64LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, type);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
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
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal64LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int type;
        private long cacheValue = Decimals.DECIMAL64_NULL;
        private long lastValue = Decimals.DECIMAL64_NULL;
        private int loIdx = 0;

        public Decimal64LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            lastValue = cacheValue;
            if (d != Decimals.DECIMAL64_NULL && frameIncludesCurrentValue) {
                lastValue = d;
            } else if (frameLoBounded) {
                long last = buffer.getLong((long) (loIdx + frameSize - 1) % bufferSize * Long.BYTES);
                if (last != Decimals.DECIMAL64_NULL) {
                    lastValue = last;
                } else if (lastValue == Decimals.DECIMAL64_NULL) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        long v = buffer.getLong((long) (loIdx + i) % bufferSize * Long.BYTES);
                        if (v != Decimals.DECIMAL64_NULL) {
                            lastValue = v;
                            break;
                        }
                    }
                }
            } else {
                long last = buffer.getLong((long) loIdx % bufferSize * Long.BYTES);
                if (last != Decimals.DECIMAL64_NULL) {
                    lastValue = last;
                } else {
                    lastValue = cacheValue;
                }
            }

            cacheValue = lastValue;
            if (frameLoBounded && buffer.getLong((long) loIdx % bufferSize * Long.BYTES) == lastValue) {
                cacheValue = Decimals.DECIMAL64_NULL;
            }
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL64_NULL;
            cacheValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL64_NULL;
            cacheValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
        }
    }

    public static class Decimal64LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int type;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long d = arg.getDecimal64(record);
            if (mapValue.isNew()) {
                mapValue.putLong(0, d);
                value = d;
            } else {
                if (d != Decimals.DECIMAL64_NULL) {
                    mapValue.putLong(0, d);
                    value = d;
                } else {
                    value = mapValue.getLong(0);
                }
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

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

    public static class Decimal64LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64LastNotNullValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                long d = arg.getDecimal64(record);
                if (d != Decimals.DECIMAL64_NULL) {
                    found = true;
                    value = d;
                }
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL64_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
            found = false;
        }
    }

    public static class Decimal64LastValueIncludeCurrentFrameFunction extends BaseWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg, int type) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
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
            return NAME;
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

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal64LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
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
            return NAME;
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

    static class Decimal64LastValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal64LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long val;
            if (value.isNew()) {
                long d = arg.getDecimal64(record);
                value.putLong(0, d);
                val = d;
            } else {
                val = value.getLong(0);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal64LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long lastValue = Decimals.DECIMAL64_NULL;

        public Decimal64LastValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getDecimal64(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putLong(startOffset + Long.BYTES, d);
                size = 1;
                lastValue = Decimals.DECIMAL64_NULL;
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL64_NULL;
                }

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

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal64LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final MemoryARW memory;
        private final long rowLo;
        private final int type;
        private long lastValue = Decimals.DECIMAL64_NULL;

        public Decimal64LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.type = type;
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
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            lastValue = memory.getLong(startOffset + loIdx % bufferSize * Long.BYTES);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putLong(startOffset + loIdx % bufferSize * Long.BYTES, d);
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal64LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long lastValue = Decimals.DECIMAL64_NULL;
        protected long size;
        protected long startOffset;

        public Decimal64LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getLong(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL64_NULL;
            }

            if (size == capacity) {
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                long oldAddress = memory.getPageAddress(0) + startOffset;
                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }
                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    public static class Decimal64LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        private final int type;
        private long lastValue = Decimals.DECIMAL64_NULL;
        private int loIdx = 0;

        public Decimal64LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getLong((long) loIdx * Long.BYTES);
            buffer.putLong((long) loIdx * Long.BYTES, arg.getDecimal64(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getDecimal64(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
        }
    }

    public static class Decimal64LastValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private long value = Decimals.DECIMAL64_NULL;

        public Decimal64LastValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                found = true;
                value = arg.getDecimal64(record);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL64_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
            found = false;
        }
    }

    public static class Decimal8LastNotNullOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final int type;
        private byte lastValue = Decimals.DECIMAL8_NULL;

        public Decimal8LastNotNullOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                lastValue = b;
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reset() {
            super.reset();
            lastValue = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(") ignore nulls");
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL8_NULL;
        }
    }

    static class Decimal8LastNotNullValueOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private byte value = Decimals.DECIMAL8_NULL;

        Decimal8LastNotNullValueOverCurrentRowFunction(Function arg, int type) {
            super(arg);
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
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal8LastNotNullValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal8LastNotNullValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (key.findValue() == null) {
                byte b = arg.getDecimal8(record);
                if (b != Decimals.DECIMAL8_NULL) {
                    MapValue value = key.createValue();
                    value.putLong(0, b);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            byte val = value != null ? (byte) value.getLong(0) : Decimals.DECIMAL8_NULL;
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal8LastNotNullValueOverPartitionRangeFrameFunction extends Decimal8LastValueOverPartitionRangeFrameFunction {

        private final boolean frameIncludesCurrentValue;

        public Decimal8LastNotNullValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, type);
            frameIncludesCurrentValue = rangeHi == 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size = 0;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            byte b = arg.getDecimal8(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (b != Decimals.DECIMAL8_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putByte(startOffset + Long.BYTES, b);
                    size = 1;
                    lastValue = frameIncludesCurrentValue ? b : Decimals.DECIMAL8_NULL;
                } else {
                    lastValue = Decimals.DECIMAL8_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getByte(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL8_NULL;
                }
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal8LastNotNullValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int type;
        private byte lastValue = Decimals.DECIMAL8_NULL;

        public Decimal8LastNotNullValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
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

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && b != Decimals.DECIMAL8_NULL) {
                    lastValue = b;
                } else {
                    lastValue = Decimals.DECIMAL8_NULL;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
            } else {
                lastValue = (byte) value.getLong(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (b != Decimals.DECIMAL8_NULL && frameIncludesCurrentValue) {
                    lastValue = b;
                } else if (frameLoBounded) {
                    byte last = memory.getByte(startOffset + (loIdx + frameSize - 1) % bufferSize * Byte.BYTES);
                    if (last != Decimals.DECIMAL8_NULL) {
                        lastValue = last;
                    } else if (lastValue == Decimals.DECIMAL8_NULL) {
                        for (int i = frameSize - 2; 0 <= i; i--) {
                            byte v = memory.getByte(startOffset + (loIdx + i) % bufferSize * Byte.BYTES);
                            if (v != Decimals.DECIMAL8_NULL) {
                                lastValue = v;
                                break;
                            }
                        }
                    }
                } else {
                    byte last = memory.getByte(startOffset + loIdx % bufferSize * Byte.BYTES);
                    if (last != Decimals.DECIMAL8_NULL) {
                        lastValue = last;
                    } else {
                        lastValue = (byte) value.getLong(0);
                    }
                }
            }

            byte nextLastValue = lastValue;
            if (frameLoBounded && memory.getByte(startOffset + loIdx % bufferSize * Byte.BYTES) == lastValue) {
                nextLastValue = Decimals.DECIMAL8_NULL;
            }
            value.putLong(0, nextLastValue);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);
            memory.putByte(startOffset + loIdx * Byte.BYTES, b);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL8_NULL;
        }
    }

    public static class Decimal8LastNotNullValueOverRangeFrameFunction extends Decimal8LastValueOverRangeFrameFunction {

        public Decimal8LastNotNullValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, type);
        }

        @Override
        public void computeNext(Record record) {
            long newFirstIdx = firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
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
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;
                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                        firstIdx = 0;
                    }
                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, b);
                size++;
            }

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getByte(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL8_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal8LastNotNullValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int type;
        private byte cacheValue = Decimals.DECIMAL8_NULL;
        private byte lastValue = Decimals.DECIMAL8_NULL;
        private int loIdx = 0;

        public Decimal8LastNotNullValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            lastValue = cacheValue;
            if (b != Decimals.DECIMAL8_NULL && frameIncludesCurrentValue) {
                lastValue = b;
            } else if (frameLoBounded) {
                byte last = buffer.getByte((long) (loIdx + frameSize - 1) % bufferSize * Byte.BYTES);
                if (last != Decimals.DECIMAL8_NULL) {
                    lastValue = last;
                } else if (lastValue == Decimals.DECIMAL8_NULL) {
                    for (int i = frameSize - 2; 0 <= i; i--) {
                        byte v = buffer.getByte((long) (loIdx + i) % bufferSize * Byte.BYTES);
                        if (v != Decimals.DECIMAL8_NULL) {
                            lastValue = v;
                            break;
                        }
                    }
                }
            } else {
                byte last = buffer.getByte((long) loIdx % bufferSize * Byte.BYTES);
                if (last != Decimals.DECIMAL8_NULL) {
                    lastValue = last;
                } else {
                    lastValue = cacheValue;
                }
            }

            cacheValue = lastValue;
            if (frameLoBounded && buffer.getByte((long) loIdx % bufferSize * Byte.BYTES) == lastValue) {
                cacheValue = Decimals.DECIMAL8_NULL;
            }
            buffer.putByte((long) loIdx * Byte.BYTES, b);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL8_NULL;
            cacheValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL8_NULL;
            cacheValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
        }
    }

    public static class Decimal8LastNotNullValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int type;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8LastNotNullValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            byte b = arg.getDecimal8(record);
            if (mapValue.isNew()) {
                mapValue.putLong(0, b);
                value = b;
            } else {
                if (b != Decimals.DECIMAL8_NULL) {
                    mapValue.putLong(0, b);
                    value = b;
                } else {
                    value = (byte) mapValue.getLong(0);
                }
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

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

    public static class Decimal8LastNotNullValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8LastNotNullValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                byte b = arg.getDecimal8(record);
                if (b != Decimals.DECIMAL8_NULL) {
                    found = true;
                    value = b;
                }
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL8_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
            found = false;
        }
    }

    public static class Decimal8LastValueIncludeCurrentFrameFunction extends BaseWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8LastValueIncludeCurrentFrameFunction(long rowsLo, boolean isRange, Function arg, int type) {
            super(arg);
            this.rowsLo = rowsLo;
            this.isRange = isRange;
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
            return NAME;
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

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
        }
    }

    public static class Decimal8LastValueIncludeCurrentPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final boolean isRange;
        private final long rowsLo;
        private final int type;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8LastValueIncludeCurrentPartitionRowsFrameFunction(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(null, partitionByRecord, partitionBySink, arg);
            this.isRange = isRange;
            this.rowsLo = rowsLo;
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
            return NAME;
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

    static class Decimal8LastValueOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal8LastValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            byte val;
            if (value.isNew()) {
                byte b = arg.getDecimal8(record);
                value.putLong(0, b);
                val = b;
            } else {
                val = (byte) value.getLong(0);
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal8LastValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected byte lastValue = Decimals.DECIMAL8_NULL;

        public Decimal8LastValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            byte b = arg.getDecimal8(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putByte(startOffset + Long.BYTES, b);
                size = 1;
                lastValue = Decimals.DECIMAL8_NULL;
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);

                long newFirstIdx = firstIdx;
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

                if (lastIndex != -1) {
                    firstIdx = lastIndex;
                    size++;
                }
                if (lastIndex != -1 && size != 0) {
                    lastValue = memory.getByte(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    lastValue = Decimals.DECIMAL8_NULL;
                }

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

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, size);
            mapValue.putLong(2, capacity);
            mapValue.putLong(3, firstIdx);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            lastValue = Decimals.DECIMAL8_NULL;
        }
    }

    public static class Decimal8LastValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final MemoryARW memory;
        private final long rowLo;
        private final int type;
        private byte lastValue = Decimals.DECIMAL8_NULL;

        public Decimal8LastValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.rowLo = rowsLo;
            this.memory = memory;
            this.type = type;
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
            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
            }

            lastValue = memory.getByte(startOffset + loIdx % bufferSize * Byte.BYTES);
            value.putLong(0, (loIdx + 1) % bufferSize);
            memory.putByte(startOffset + loIdx % bufferSize * Byte.BYTES, b);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            lastValue = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            lastValue = Decimals.DECIMAL8_NULL;
        }
    }

    public static class Decimal8LastValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        protected static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected byte lastValue = Decimals.DECIMAL8_NULL;
        protected long size;
        protected long startOffset;

        public Decimal8LastValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
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
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
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
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

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

            if (lastIndex != -1) {
                firstIdx = lastIndex;
                size++;
            }
            if (lastIndex != -1 && size != 0) {
                lastValue = memory.getByte(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
            } else {
                lastValue = Decimals.DECIMAL8_NULL;
            }

            if (size == capacity) {
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                long oldAddress = memory.getPageAddress(0) + startOffset;
                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
                    long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                    Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                    Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                    firstIdx = 0;
                }
                startOffset = newAddress - memory.getPageAddress(0);
                capacity <<= 1;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, b);
            size++;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    public static class Decimal8LastValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final long rowsLo;
        private final int type;
        private byte lastValue = Decimals.DECIMAL8_NULL;
        private int loIdx = 0;

        public Decimal8LastValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
            bufferSize = (int) Math.abs(rowsHi);
            this.buffer = memory;
            this.rowsLo = rowsLo;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            lastValue = buffer.getByte((long) loIdx * Byte.BYTES);
            buffer.putByte((long) loIdx * Byte.BYTES, arg.getDecimal8(record));
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lastValue;
        }

        @Override
        public String getName() {
            return NAME;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lastValue);
        }

        @Override
        public void reopen() {
            lastValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            lastValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
        }

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

        @Override
        public void toTop() {
            super.toTop();
            lastValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
        }
    }

    public static class Decimal8LastValueOverWholeResultSetFunction extends BaseWindowFunction {
        private final int type;
        private boolean found;
        private byte value = Decimals.DECIMAL8_NULL;

        public Decimal8LastValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                found = true;
                value = arg.getDecimal8(record);
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL8_NULL;
            found = false;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
            found = false;
        }
    }

    static {
        LAST_VALUE_DECIMAL64_TYPES = new ArrayColumnTypes();
        LAST_VALUE_DECIMAL64_TYPES.add(ColumnType.LONG);

        LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        LAST_VALUE_DECIMAL64_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_VALUE_DECIMAL64_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_DECIMAL64_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        LAST_VALUE_DECIMAL128_TYPES = new ArrayColumnTypes();
        LAST_VALUE_DECIMAL128_TYPES.add(ColumnType.DECIMAL128);

        LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL128);
        LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_DECIMAL128_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        LAST_VALUE_DECIMAL256_TYPES = new ArrayColumnTypes();
        LAST_VALUE_DECIMAL256_TYPES.add(ColumnType.DECIMAL256);

        LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL256);
        LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        LAST_NOT_NULL_VALUE_DECIMAL256_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
    }
}
