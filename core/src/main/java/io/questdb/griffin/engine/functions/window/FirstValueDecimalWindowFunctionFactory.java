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

public class FirstValueDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "first_value";
    private static final ArrayColumnTypes FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL128_TYPES;
    // Live-view value layouts for the unbounded-preceding accumulator. Each keeps
    // the captured value at its native width in slot 0, an "initialized" flag in
    // slot 1 (distinguishes a captured value from a post-anchor-reset partition
    // awaiting recapture), and an anchor-driven tombstone in slot 2.
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL128_TYPES_LV;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL16_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL16_TYPES_LV;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL256_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL256_TYPES_LV;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL32_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL32_TYPES_LV;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL64_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL64_TYPES_LV;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL8_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_DECIMAL8_TYPES_LV;
    private static final ArrayColumnTypes FIRST_VALUE_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes FIRST_VALUE_OVER_PARTITION_ROWS_TYPES;
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
            case ColumnType.DECIMAL64 -> windowContext.isIgnoreNulls()
                    ? generateDecimal64IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal64RespectNulls(position, args, configuration, windowContext, argType);
            case ColumnType.DECIMAL128 -> windowContext.isIgnoreNulls()
                    ? generateDecimal128IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal128RespectNulls(position, args, configuration, windowContext, argType);
            default -> windowContext.isIgnoreNulls()
                    ? generateDecimal256IgnoreNulls(position, args, configuration, windowContext, argType)
                    : generateDecimal256RespectNulls(position, args, configuration, windowContext, argType);
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL128_TYPES_LV : FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal128FirstNotNullOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL128_TYPES_LV : FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal128FirstNotNullOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal128FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128FirstNotNullOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128FirstNotNullOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL128_TYPES_LV : FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal128FirstValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL128_TYPES_LV : FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal128FirstValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal128FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128FirstValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new Decimal128FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128FirstValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL16_TYPES_LV : FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal16FirstNotNullOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL16_TYPES_LV : FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal16FirstNotNullOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal16FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16FirstNotNullOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16FirstNotNullOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL16_TYPES_LV : FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal16FirstValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL16_TYPES_LV : FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL16_TYPES);
                    try {
                        return new Decimal16FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal16FirstValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal16FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16FirstValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new Decimal16FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16FirstValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL256_TYPES_LV : FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal256FirstNotNullOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL256_TYPES_LV : FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal256FirstNotNullOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal256FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256FirstNotNullOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256FirstNotNullOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL256_TYPES_LV : FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal256FirstValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL256_TYPES_LV : FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal256FirstValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal256FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256FirstValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new Decimal256FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256FirstValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL32_TYPES_LV : FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal32FirstNotNullOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL32_TYPES_LV : FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal32FirstNotNullOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal32FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32FirstNotNullOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32FirstNotNullOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL32_TYPES_LV : FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal32FirstValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL32_TYPES_LV : FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL32_TYPES);
                    try {
                        return new Decimal32FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal32FirstValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal32FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32FirstValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new Decimal32FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32FirstValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL64_TYPES_LV : FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal64FirstNotNullOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL64_TYPES_LV : FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal64FirstNotNullOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal64FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64FirstNotNullOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64FirstNotNullOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL64_TYPES_LV : FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal64FirstValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL64_TYPES_LV : FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal64FirstValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal64FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64FirstValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new Decimal64FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64FirstValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL8_TYPES_LV : FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal8FirstNotNullOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL8_TYPES_LV : FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstNotNullOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstNotNullOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal8FirstNotNullOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal8FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8FirstNotNullOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8FirstNotNullOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8FirstValueOverCurrentRowFunction(args.get(0), true, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8FirstNotNullOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL8_TYPES_LV : FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_RANGE_TYPES);
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
                        return new Decimal8FirstValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes,
                            liveView ? FIRST_VALUE_DECIMAL8_TYPES_LV : FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstValueOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), argType, partitionByKeyTypes, liveView, configuration);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_DECIMAL8_TYPES);
                    try {
                        return new Decimal8FirstValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), argType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, FIRST_VALUE_OVER_PARTITION_ROWS_TYPES);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                    try {
                        return new Decimal8FirstValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
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
                    return new Decimal8FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8FirstValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new Decimal8FirstValueOverWholeResultSetFunction(args.get(0), argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8FirstValueOverCurrentRowFunction(args.get(0), false, argType);
                } else {
                    MemoryARW mem = null;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8FirstValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, argType);
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    static class Decimal128FirstNotNullOverPartitionFunction extends Decimal128FirstValueOverPartitionFunction {

        public Decimal128FirstNotNullOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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

    public static class Decimal128FirstNotNullOverPartitionRangeFrameFunction extends Decimal128FirstValueOverPartitionRangeFrameFunction {

        public Decimal128FirstNotNullOverPartitionRangeFrameFunction(
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

            if (mapValue.isNew()) {
                arg.getDecimal128(record, scratch);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!scratch.isNull()) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    size = 1;
                    if (frameIncludesCurrentValue) {
                        firstValue.copyFrom(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    size = 0;
                    firstValue.ofRawNull();
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) {
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            memory.getDecimal128(startOffset + Long.BYTES, firstValue);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue.ofRawNull();
                        }
                    } else {
                        memory.getDecimal128(startOffset + Long.BYTES, firstValue);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                arg.getDecimal128(record, scratch);
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

                if (!findNewFirstValue) {
                    if (frameIncludesCurrentValue) {
                        firstValue.copyFrom(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
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

    public static class Decimal128FirstNotNullOverPartitionRowsFrameFunction extends Decimal128FirstValueOverPartitionRowsFrameFunction {

        public Decimal128FirstNotNullOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal128.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal128(startOffset + (long) i * Decimal128.BYTES, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            arg.getDecimal128(record, scratch);
            boolean dIsNull = scratch.isNull();

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    memory.getDecimal128(startOffset, firstValue);
                    return;
                }
                if (firstNotNullIdx == -1 && !dIsNull) {
                    firstNotNullIdx = count;
                    memory.putDecimal128(startOffset, scratch.getHigh(), scratch.getLow());
                    if (frameIncludesCurrentValue) {
                        firstValue.copyFrom(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                Decimal128 tmp = scratchAlt;
                memory.getDecimal128(startOffset + loIdx * Decimal128.BYTES, tmp);
                if (firstNotNullIdx != -1 && !tmp.isNull()) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    memory.getDecimal128(startOffset + firstNotNullIdx * Decimal128.BYTES, firstValue);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        memory.getDecimal128(startOffset + (loIdx + i) % bufferSize * Decimal128.BYTES, tmp);
                        if (!tmp.isNull()) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue.copyFrom(tmp);
                            break;
                        }
                    }
                    if (!find) {
                        if (frameIncludesCurrentValue) {
                            firstValue.copyFrom(scratch);
                        } else {
                            firstValue.ofRawNull();
                        }
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putDecimal128(startOffset + loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal128FirstNotNullOverRangeFrameFunction extends Decimal128FirstValueOverRangeFrameFunction implements Reopenable {

        public Decimal128FirstNotNullOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) {
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        memory.getDecimal128(startOffset + Long.BYTES, firstValue);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    memory.getDecimal128(startOffset + Long.BYTES, firstValue);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        findNewFirstValue = true;
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                    }
                    break;
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

            if (!findNewFirstValue) {
                if (frameIncludesCurrentValue) {
                    firstValue.copyFrom(scratch);
                } else {
                    firstValue.ofRawNull();
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal128FirstNotNullOverRowsFrameFunction extends Decimal128FirstValueOverRowsFrameFunction implements Reopenable {
        private long firstNotNullIdx = -1;

        public Decimal128FirstNotNullOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg, rowsLo, rowsHi, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            boolean dIsNull = scratch.isNull();
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    buffer.getDecimal128(0, firstValue);
                    return;
                }
                if (firstNotNullIdx == -1 && !dIsNull) {
                    firstNotNullIdx = count;
                    buffer.putDecimal128(0, scratch.getHigh(), scratch.getLow());
                    if (frameIncludesCurrentValue) {
                        firstValue.copyFrom(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
                count++;
            } else {
                buffer.getDecimal128((long) loIdx * Decimal128.BYTES, scratchAlt);
                if (firstNotNullIdx != -1 && !scratchAlt.isNull()) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    buffer.getDecimal128(firstNotNullIdx * Decimal128.BYTES, firstValue);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        buffer.getDecimal128((long) (loIdx + i) % bufferSize * Decimal128.BYTES, scratchAlt);
                        if (!scratchAlt.isNull()) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue.copyFrom(scratchAlt);
                            break;
                        }
                    }
                    if (!find) {
                        if (frameIncludesCurrentValue) {
                            firstValue.copyFrom(scratch);
                        } else {
                            firstValue.ofRawNull();
                        }
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putDecimal128((long) loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
                loIdx = (loIdx + 1) % bufferSize;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    static class Decimal128FirstNotNullOverUnboundedPartitionRowsFrameFunction extends Decimal128FirstValueOverUnboundedPartitionRowsFrameFunction {

        public Decimal128FirstNotNullOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg, type, partitionByKeyTypes, liveView, configuration);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (liveView) {
                MapValue mapValue = key.createValue();
                boolean isNew = mapValue.isNew();
                if (isNew) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (isNew || mapValue.getByte(1) == 0) {
                    arg.getDecimal128(record, scratch);
                    if (!scratch.isNull()) {
                        mapValue.putDecimal128(0, scratch);
                        mapValue.putByte(1, (byte) 1);
                        value.copyFrom(scratch);
                    } else {
                        value.ofRawNull();
                        mapValue.putDecimal128(0, value);
                        mapValue.putByte(1, (byte) 0);
                    }
                } else {
                    mapValue.getDecimal128(0, value);
                }
            } else {
                MapValue mapValue = key.findValue();
                if (mapValue != null) {
                    mapValue.getDecimal128(0, value);
                } else {
                    arg.getDecimal128(record, scratch);
                    if (!scratch.isNull()) {
                        mapValue = key.createValue();
                        mapValue.putDecimal128(0, scratch);
                        value.copyFrom(scratch);
                    } else {
                        value.ofRawNull();
                    }
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal128FirstNotNullOverWholeResultSetFunction extends Decimal128FirstValueOverWholeResultSetFunction {

        public Decimal128FirstNotNullOverWholeResultSetFunction(Function arg, int type) {
            super(arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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
                    value.copyFrom(scratch);
                    found = true;
                }
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
            found = false;
            value.ofRawNull();
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value.ofRawNull();
        }
    }

    static class Decimal128FirstValueOverCurrentRowFunction extends BaseWindowFunction {

        protected final Decimal128 scratch = new Decimal128();
        protected final Decimal128 value = new Decimal128();
        private final boolean ignoreNulls;
        private final int type;

        Decimal128FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls, int type) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
            this.type = type;
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
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }
    }

    static class Decimal128FirstValueOverPartitionFunction extends BasePartitionedWindowFunction {

        protected final Decimal128 firstValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;

        public Decimal128FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                arg.getDecimal128(record, firstValue);
                value.putDecimal128(0, firstValue);
            } else {
                value.getDecimal128(0, firstValue);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getLow());
        }
    }

    public static class Decimal128FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + 16;
        protected final Decimal128 firstValue = new Decimal128();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final Decimal128 scratch = new Decimal128();
        protected final int timestampIndex;
        protected final int type;

        public Decimal128FirstValueOverPartitionRangeFrameFunction(
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
            this.frameIncludesCurrentValue = rangeHi == 0;
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

            long frameSize;
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
                if (frameIncludesCurrentValue) {
                    firstValue.copyFrom(scratch);
                    frameSize = 1;
                } else {
                    firstValue.ofRawNull();
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                if (!frameLoBounded && frameSize > 0) {
                    memory.getDecimal128(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, firstValue);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
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
                    memory.getDecimal128(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, firstValue);
                } else {
                    firstValue.ofRawNull();
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
            firstValue.ofRawNull();
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
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" range between ").val(maxDiff).val(" preceding and ");
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
        }
    }

    public static class Decimal128FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final int bufferSize;
        protected final Decimal128 firstValue = new Decimal128();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final Decimal128 scratch = new Decimal128();
        protected final Decimal128 scratchAlt = new Decimal128();
        protected final int type;

        public Decimal128FirstValueOverPartitionRowsFrameFunction(
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
            long count;
            arg.getDecimal128(record, scratch);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal128.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal128(startOffset + (long) i * Decimal128.BYTES, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);
                if (!frameLoBounded && count == bufferSize) {
                    memory.getDecimal128(startOffset + loIdx * Decimal128.BYTES, firstValue);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue.copyFrom(scratch);
            } else if (count > bufferSize - frameSize) {
                memory.getDecimal128(startOffset + (loIdx + bufferSize - count) % bufferSize * Decimal128.BYTES, firstValue);
            } else {
                firstValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);
            memory.putDecimal128(startOffset + loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getLow());
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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
        }
    }

    static class Decimal128FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        protected static final int RECORD_SIZE = Long.BYTES + 16;
        protected final Decimal128 firstValue = new Decimal128();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
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

        public Decimal128FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            this(rangeLo, rangeHi, arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER),
                    timestampIdx, type);
        }

        public Decimal128FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
            firstValue.ofRawNull();
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

            if (frameLoBounded) {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                    } else if (frameIncludesCurrentValue) {
                        firstValue.copyFrom(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                    } else if (frameIncludesCurrentValue) {
                        firstValue.copyFrom(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getLow());
        }

        @Override
        public void reopen() {
            firstValue.ofRawNull();
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
            firstValue.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    static class Decimal128FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final Decimal128 firstValue = new Decimal128();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final Decimal128 scratch = new Decimal128();
        protected final Decimal128 scratchAlt = new Decimal128();
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;

        public Decimal128FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
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
            this.buffer = memory;
            this.type = type;
            firstValue.ofRawNull();
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
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (!frameLoBounded && count == bufferSize) {
                buffer.getDecimal128((long) loIdx * Decimal128.BYTES, firstValue);
                return;
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue.copyFrom(scratch);
            } else if (count > bufferSize - frameSize) {
                buffer.getDecimal128((loIdx + bufferSize - count) % bufferSize * Decimal128.BYTES, firstValue);
            } else {
                firstValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putDecimal128((long) loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getLow());
        }

        @Override
        public void reopen() {
            firstValue.ofRawNull();
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue.ofRawNull();
            count = 0;
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
            firstValue.ofRawNull();
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal128((long) i * Decimal128.BYTES, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    static class Decimal128FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;
        protected final Decimal128 value = new Decimal128();

        public Decimal128FirstValueOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_DECIMAL128_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_DECIMAL128_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            boolean isNew = mapValue.isNew();
            if (isNew && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (isNew || (liveView && mapValue.getByte(1) == 0)) {
                arg.getDecimal128(record, value);
                mapValue.putDecimal128(0, value);
                if (liveView) {
                    mapValue.putByte(1, (byte) 1);
                }
            } else {
                mapValue.getDecimal128(0, value);
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
        public Map getPartitionMap() {
            return map;
        }

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
                    : FIRST_VALUE_DECIMAL128_TYPES.getColumnCount();
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Mark the partition uninitialized so the next
            // computeNext recaptures the first value on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv != null) {
                mv.putByte(1, (byte) 0);
                if (!mv.isNew() && tombstoneValueIndex >= 0 && mv.getByte(tombstoneValueIndex) != 1) {
                    mv.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            source.getDecimal128(offset, scratch);
            value.putDecimal128(0, scratch);
            offset += 2 * Long.BYTES;
            value.putByte(1, source.getByte(offset));
            offset += Byte.BYTES;
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
            value.getDecimal128(0, scratch);
            sink.putDecimal128(scratch.getHigh(), scratch.getLow());
            sink.putByte(value.getByte(1));
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    static class Decimal128FirstValueOverWholeResultSetFunction extends BaseWindowFunction {

        protected final Decimal128 scratch = new Decimal128();
        protected final int type;
        protected final Decimal128 value = new Decimal128();
        protected boolean found;

        public Decimal128FirstValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                arg.getDecimal128(record, value);
                found = true;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                arg.getDecimal128(record, value);
                found = true;
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value.ofRawNull();
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value.ofRawNull();
        }
    }

    static class Decimal16FirstNotNullOverPartitionFunction extends Decimal16FirstValueOverPartitionFunction {

        public Decimal16FirstNotNullOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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
                short d = arg.getDecimal16(record);
                if (d != Decimals.DECIMAL16_NULL) {
                    MapValue value = key.createValue();
                    value.putShort(0, d);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            short val = value != null ? value.getShort(0) : Decimals.DECIMAL16_NULL;
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal16FirstNotNullOverPartitionRangeFrameFunction extends Decimal16FirstValueOverPartitionRangeFrameFunction {

        public Decimal16FirstNotNullOverPartitionRangeFrameFunction(
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

            if (mapValue.isNew()) {
                short d = arg.getDecimal16(record);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Decimals.DECIMAL16_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putShort(startOffset + Long.BYTES, d);
                    size = 1;
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                } else {
                    size = 0;
                    firstValue = Decimals.DECIMAL16_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) {
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            firstValue = memory.getShort(startOffset + Long.BYTES);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue = Decimals.DECIMAL16_NULL;
                        }
                    } else {
                        firstValue = memory.getShort(startOffset + Long.BYTES);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            firstValue = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                short d = arg.getDecimal16(record);
                if (d != Decimals.DECIMAL16_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                if (!findNewFirstValue) {
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
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

    public static class Decimal16FirstNotNullOverPartitionRowsFrameFunction extends Decimal16FirstValueOverPartitionRowsFrameFunction {

        public Decimal16FirstNotNullOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

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
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = memory.getShort(startOffset);
                    return;
                }
                short d = arg.getDecimal16(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL16_NULL) {
                    firstNotNullIdx = count;
                    memory.putShort(startOffset, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                } else {
                    firstValue = Decimals.DECIMAL16_NULL;
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                short d = arg.getDecimal16(record);
                if (firstNotNullIdx != -1 && memory.getShort(startOffset + loIdx * Short.BYTES) != Decimals.DECIMAL16_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = memory.getShort(startOffset + firstNotNullIdx * Short.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        short res = memory.getShort(startOffset + (loIdx + i) % bufferSize * Short.BYTES);
                        if (res != Decimals.DECIMAL16_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putShort(startOffset + loIdx * Short.BYTES, d);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal16FirstNotNullOverRangeFrameFunction extends Decimal16FirstValueOverRangeFrameFunction implements Reopenable {

        public Decimal16FirstNotNullOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) {
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        firstValue = memory.getShort(startOffset + Long.BYTES);
                    } else {
                        firstValue = Decimals.DECIMAL16_NULL;
                    }
                } else {
                    firstValue = memory.getShort(startOffset + Long.BYTES);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        findNewFirstValue = true;
                        firstValue = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    }
                    break;
                }
            }
            firstIdx = newFirstIdx;
            short d = arg.getDecimal16(record);
            if (d != Decimals.DECIMAL16_NULL) {
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
                memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            if (!findNewFirstValue) {
                firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal16FirstNotNullOverRowsFrameFunction extends Decimal16FirstValueOverRowsFrameFunction implements Reopenable {
        private long firstNotNullIdx = -1;

        public Decimal16FirstNotNullOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg, rowsLo, rowsHi, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getShort(0);
                    return;
                }
                short d = arg.getDecimal16(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL16_NULL) {
                    firstNotNullIdx = count;
                    buffer.putShort(0, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                } else {
                    firstValue = Decimals.DECIMAL16_NULL;
                }
                count++;
            } else {
                short d = arg.getDecimal16(record);
                if (firstNotNullIdx != -1 && buffer.getShort((long) loIdx * Short.BYTES) != Decimals.DECIMAL16_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = buffer.getShort(firstNotNullIdx * Short.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        short res = buffer.getShort((long) (loIdx + i) % bufferSize * Short.BYTES);
                        if (res != Decimals.DECIMAL16_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putShort((long) loIdx * Short.BYTES, d);
                loIdx = (loIdx + 1) % bufferSize;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    static class Decimal16FirstNotNullOverUnboundedPartitionRowsFrameFunction extends Decimal16FirstValueOverUnboundedPartitionRowsFrameFunction {

        public Decimal16FirstNotNullOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg, type, partitionByKeyTypes, liveView, configuration);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (liveView) {
                MapValue mapValue = key.createValue();
                boolean isNew = mapValue.isNew();
                if (isNew) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (isNew || mapValue.getByte(1) == 0) {
                    short d = arg.getDecimal16(record);
                    if (d != Decimals.DECIMAL16_NULL) {
                        mapValue.putShort(0, d);
                        mapValue.putByte(1, (byte) 1);
                        value = d;
                    } else {
                        mapValue.putShort(0, Decimals.DECIMAL16_NULL);
                        mapValue.putByte(1, (byte) 0);
                        value = Decimals.DECIMAL16_NULL;
                    }
                } else {
                    value = mapValue.getShort(0);
                }
            } else {
                MapValue mapValue = key.findValue();
                if (mapValue != null) {
                    value = mapValue.getShort(0);
                } else {
                    short d = arg.getDecimal16(record);
                    if (d != Decimals.DECIMAL16_NULL) {
                        mapValue = key.createValue();
                        mapValue.putShort(0, d);
                        value = d;
                    } else {
                        value = Decimals.DECIMAL16_NULL;
                    }
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal16FirstNotNullOverWholeResultSetFunction extends Decimal16FirstValueOverWholeResultSetFunction {

        public Decimal16FirstNotNullOverWholeResultSetFunction(Function arg, int type) {
            super(arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                short d = arg.getDecimal16(record);
                if (d != Decimals.DECIMAL16_NULL) {
                    value = d;
                    found = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL16_NULL;
        }
    }

    static class Decimal16FirstValueOverCurrentRowFunction extends BaseWindowFunction {

        private final boolean ignoreNulls;
        private final int type;
        private short value;

        Decimal16FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls, int type) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
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
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal16FirstValueOverPartitionFunction extends BasePartitionedWindowFunction {

        protected final int type;
        protected short firstValue;

        public Decimal16FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                firstValue = arg.getDecimal16(record);
                value.putShort(0, firstValue);
            } else {
                firstValue = value.getShort(0);
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return firstValue;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    public static class Decimal16FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected short firstValue;

        public Decimal16FirstValueOverPartitionRangeFrameFunction(
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
            this.frameIncludesCurrentValue = rangeHi == 0;
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

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            short d = arg.getDecimal16(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putShort(startOffset + Long.BYTES, d);
                size = 1;
                if (frameIncludesCurrentValue) {
                    firstValue = d;
                    frameSize = 1;
                } else {
                    firstValue = Decimals.DECIMAL16_NULL;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                if (!frameLoBounded && frameSize > 0) {
                    firstValue = memory.getShort(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
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

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;

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
                    firstValue = memory.getShort(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    firstValue = Decimals.DECIMAL16_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public short getDecimal16(Record rec) {
            return firstValue;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            firstValue = Decimals.DECIMAL16_NULL;
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
            sink.val(maxDiff);
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
        }
    }

    public static class Decimal16FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int type;
        protected short firstValue;

        public Decimal16FirstValueOverPartitionRowsFrameFunction(
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
            long count;
            short d = arg.getDecimal16(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);
                if (!frameLoBounded && count == bufferSize) {
                    firstValue = memory.getShort(startOffset + loIdx * Short.BYTES);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = memory.getShort(startOffset + (loIdx + bufferSize - count) % bufferSize * Short.BYTES);
            } else {
                firstValue = Decimals.DECIMAL16_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);
            memory.putShort(startOffset + loIdx * Short.BYTES, d);
        }

        @Override
        public short getDecimal16(Record rec) {
            return firstValue;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), firstValue);
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal16FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        protected static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected short firstValue;
        protected long size;
        protected long startOffset;

        public Decimal16FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            this(rangeLo, rangeHi, arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER),
                    timestampIdx,
                    type);
        }

        public Decimal16FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
            firstValue = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            short d = arg.getDecimal16(record);

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
            memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;

            if (frameLoBounded) {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        firstValue = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL16_NULL;
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstValue = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL16_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL16_NULL;
                }
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return firstValue;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL16_NULL;
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
            firstValue = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    static class Decimal16FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final int type;
        protected long count = 0;
        protected short firstValue;
        protected int loIdx = 0;

        public Decimal16FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
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
            this.buffer = memory;
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
        }

        @Override
        public void computeNext(Record record) {
            short d = arg.getDecimal16(record);
            if (!frameLoBounded && count == bufferSize) {
                firstValue = buffer.getShort((long) loIdx * Short.BYTES);
                return;
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = buffer.getShort((loIdx + bufferSize - count) % bufferSize * Short.BYTES);
            } else {
                firstValue = Decimals.DECIMAL16_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putShort((long) loIdx * Short.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public short getDecimal16(Record rec) {
            return firstValue;
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
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue = Decimals.DECIMAL16_NULL;
            count = 0;
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
            firstValue = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
        }
    }

    static class Decimal16FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
        protected final int type;
        protected short value = Decimals.DECIMAL16_NULL;

        public Decimal16FirstValueOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_DECIMAL16_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_DECIMAL16_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            boolean isNew = mapValue.isNew();
            if (isNew && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (isNew || (liveView && mapValue.getByte(1) == 0)) {
                value = arg.getDecimal16(record);
                mapValue.putShort(0, value);
                if (liveView) {
                    mapValue.putByte(1, (byte) 1);
                }
            } else {
                value = mapValue.getShort(0);
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
        public Map getPartitionMap() {
            return map;
        }

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
                    : FIRST_VALUE_DECIMAL16_TYPES.getColumnCount();
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Mark the partition uninitialized so the next
            // computeNext recaptures the first value on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv != null) {
                mv.putByte(1, (byte) 0);
                if (!mv.isNew() && tombstoneValueIndex >= 0 && mv.getByte(tombstoneValueIndex) != 1) {
                    mv.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            value.putShort(0, source.getShort(offset));
            offset += Short.BYTES;
            value.putByte(1, source.getByte(offset));
            offset += Byte.BYTES;
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
            sink.putShort(value.getShort(0));
            sink.putByte(value.getByte(1));
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    static class Decimal16FirstValueOverWholeResultSetFunction extends BaseWindowFunction {

        protected final int type;
        protected boolean found;
        protected short value = Decimals.DECIMAL16_NULL;

        public Decimal16FirstValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                value = arg.getDecimal16(record);
                found = true;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = arg.getDecimal16(record);
                found = true;
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL16_NULL;
        }
    }

    static class Decimal256FirstNotNullOverPartitionFunction extends Decimal256FirstValueOverPartitionFunction {

        public Decimal256FirstNotNullOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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

    public static class Decimal256FirstNotNullOverPartitionRangeFrameFunction extends Decimal256FirstValueOverPartitionRangeFrameFunction {

        public Decimal256FirstNotNullOverPartitionRangeFrameFunction(
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

            if (mapValue.isNew()) {
                arg.getDecimal256(record, scratch);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!scratch.isNull()) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal256(startOffset + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                    size = 1;
                    if (frameIncludesCurrentValue) {
                        firstValue.copyRaw(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    size = 0;
                    firstValue.ofRawNull();
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) {
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            memory.getDecimal256(startOffset + Long.BYTES, firstValue);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue.ofRawNull();
                        }
                    } else {
                        memory.getDecimal256(startOffset + Long.BYTES, firstValue);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                arg.getDecimal256(record, scratch);
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

                if (!findNewFirstValue) {
                    if (frameIncludesCurrentValue) {
                        firstValue.copyRaw(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
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

    public static class Decimal256FirstNotNullOverPartitionRowsFrameFunction extends Decimal256FirstValueOverPartitionRowsFrameFunction {

        public Decimal256FirstNotNullOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal256.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal256(startOffset + (long) i * Decimal256.BYTES,
                            Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            arg.getDecimal256(record, scratch);
            boolean dIsNull = scratch.isNull();

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    memory.getDecimal256(startOffset, firstValue);
                    return;
                }
                if (firstNotNullIdx == -1 && !dIsNull) {
                    firstNotNullIdx = count;
                    memory.putDecimal256(startOffset, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                    if (frameIncludesCurrentValue) {
                        firstValue.copyRaw(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                memory.getDecimal256(startOffset + loIdx * Decimal256.BYTES, scratchAlt);
                if (firstNotNullIdx != -1 && !scratchAlt.isNull()) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    memory.getDecimal256(startOffset + firstNotNullIdx * Decimal256.BYTES, firstValue);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        memory.getDecimal256(startOffset + (loIdx + i) % bufferSize * Decimal256.BYTES, scratchAlt);
                        if (!scratchAlt.isNull()) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue.copyRaw(scratchAlt);
                            break;
                        }
                    }
                    if (!find) {
                        if (frameIncludesCurrentValue) {
                            firstValue.copyRaw(scratch);
                        } else {
                            firstValue.ofRawNull();
                        }
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putDecimal256(startOffset + loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal256FirstNotNullOverRangeFrameFunction extends Decimal256FirstValueOverRangeFrameFunction implements Reopenable {

        public Decimal256FirstNotNullOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) {
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        memory.getDecimal256(startOffset + Long.BYTES, firstValue);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    memory.getDecimal256(startOffset + Long.BYTES, firstValue);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        findNewFirstValue = true;
                        memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                    }
                    break;
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

            if (!findNewFirstValue) {
                if (frameIncludesCurrentValue) {
                    firstValue.copyRaw(scratch);
                } else {
                    firstValue.ofRawNull();
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal256FirstNotNullOverRowsFrameFunction extends Decimal256FirstValueOverRowsFrameFunction implements Reopenable {
        private long firstNotNullIdx = -1;

        public Decimal256FirstNotNullOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg, rowsLo, rowsHi, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            boolean dIsNull = scratch.isNull();
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    buffer.getDecimal256(0, firstValue);
                    return;
                }
                if (firstNotNullIdx == -1 && !dIsNull) {
                    firstNotNullIdx = count;
                    buffer.putDecimal256(0, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                    if (frameIncludesCurrentValue) {
                        firstValue.copyRaw(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
                count++;
            } else {
                buffer.getDecimal256((long) loIdx * Decimal256.BYTES, scratchAlt);
                if (firstNotNullIdx != -1 && !scratchAlt.isNull()) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    buffer.getDecimal256(firstNotNullIdx * Decimal256.BYTES, firstValue);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        buffer.getDecimal256((long) (loIdx + i) % bufferSize * Decimal256.BYTES, scratchAlt);
                        if (!scratchAlt.isNull()) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue.copyRaw(scratchAlt);
                            break;
                        }
                    }
                    if (!find) {
                        if (frameIncludesCurrentValue) {
                            firstValue.copyRaw(scratch);
                        } else {
                            firstValue.ofRawNull();
                        }
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putDecimal256((long) loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                loIdx = (loIdx + 1) % bufferSize;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    static class Decimal256FirstNotNullOverUnboundedPartitionRowsFrameFunction extends Decimal256FirstValueOverUnboundedPartitionRowsFrameFunction {

        public Decimal256FirstNotNullOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg, type, partitionByKeyTypes, liveView, configuration);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (liveView) {
                MapValue mapValue = key.createValue();
                boolean isNew = mapValue.isNew();
                if (isNew) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (isNew || mapValue.getByte(1) == 0) {
                    arg.getDecimal256(record, scratch);
                    if (!scratch.isNull()) {
                        mapValue.putDecimal256(0, scratch);
                        mapValue.putByte(1, (byte) 1);
                        value.copyRaw(scratch);
                    } else {
                        value.ofRawNull();
                        mapValue.putDecimal256(0, value);
                        mapValue.putByte(1, (byte) 0);
                    }
                } else {
                    mapValue.getDecimal256(0, value);
                }
            } else {
                MapValue mapValue = key.findValue();
                if (mapValue != null) {
                    mapValue.getDecimal256(0, value);
                } else {
                    arg.getDecimal256(record, scratch);
                    if (!scratch.isNull()) {
                        mapValue = key.createValue();
                        mapValue.putDecimal256(0, scratch);
                        value.copyRaw(scratch);
                    } else {
                        value.ofRawNull();
                    }
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal256FirstNotNullOverWholeResultSetFunction extends Decimal256FirstValueOverWholeResultSetFunction {

        public Decimal256FirstNotNullOverWholeResultSetFunction(Function arg, int type) {
            super(arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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
                    value.copyRaw(scratch);
                    found = true;
                }
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
            found = false;
            value.ofRawNull();
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value.ofRawNull();
        }
    }

    static class Decimal256FirstValueOverCurrentRowFunction extends BaseWindowFunction {

        protected final Decimal256 scratch = new Decimal256();
        protected final Decimal256 value = new Decimal256();
        private final boolean ignoreNulls;
        private final int type;

        Decimal256FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls, int type) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
            this.type = type;
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
            return ignoreNulls;
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

    static class Decimal256FirstValueOverPartitionFunction extends BasePartitionedWindowFunction {

        protected final Decimal256 firstValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;

        public Decimal256FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                arg.getDecimal256(record, firstValue);
                value.putDecimal256(0, firstValue);
            } else {
                value.getDecimal256(0, firstValue);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, firstValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, firstValue.getLl());
        }
    }

    public static class Decimal256FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + 32;
        protected final Decimal256 firstValue = new Decimal256();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final Decimal256 scratch = new Decimal256();
        protected final int timestampIndex;
        protected final int type;

        public Decimal256FirstValueOverPartitionRangeFrameFunction(
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
            this.frameIncludesCurrentValue = rangeHi == 0;
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

            long frameSize;
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
                if (frameIncludesCurrentValue) {
                    firstValue.copyRaw(scratch);
                    frameSize = 1;
                } else {
                    firstValue.ofRawNull();
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                if (!frameLoBounded && frameSize > 0) {
                    memory.getDecimal256(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, firstValue);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
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
                    memory.getDecimal256(startOffset + firstIdx * RECORD_SIZE + Long.BYTES, firstValue);
                } else {
                    firstValue.ofRawNull();
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, firstValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, firstValue.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
            firstValue.ofRawNull();
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
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" range between ").val(maxDiff).val(" preceding and ");
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
        }
    }

    public static class Decimal256FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final int bufferSize;
        protected final Decimal256 firstValue = new Decimal256();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final Decimal256 scratch = new Decimal256();
        protected final Decimal256 scratchAlt = new Decimal256();
        protected final int type;

        public Decimal256FirstValueOverPartitionRowsFrameFunction(
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
            long count;
            arg.getDecimal256(record, scratch);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal256.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal256(startOffset + (long) i * Decimal256.BYTES,
                            Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);
                if (!frameLoBounded && count == bufferSize) {
                    memory.getDecimal256(startOffset + loIdx * Decimal256.BYTES, firstValue);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue.copyRaw(scratch);
            } else if (count > bufferSize - frameSize) {
                memory.getDecimal256(startOffset + (loIdx + bufferSize - count) % bufferSize * Decimal256.BYTES, firstValue);
            } else {
                firstValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);
            memory.putDecimal256(startOffset + loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, firstValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, firstValue.getLl());
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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
        }
    }

    static class Decimal256FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        protected static final int RECORD_SIZE = Long.BYTES + 32;
        protected final Decimal256 firstValue = new Decimal256();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
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

        public Decimal256FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            this(rangeLo, rangeHi, arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER),
                    timestampIdx, type);
        }

        public Decimal256FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
            firstValue.ofRawNull();
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

            if (frameLoBounded) {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                    } else if (frameIncludesCurrentValue) {
                        firstValue.copyRaw(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        memory.getDecimal256(startOffset + idx * RECORD_SIZE + Long.BYTES, firstValue);
                    } else if (frameIncludesCurrentValue) {
                        firstValue.copyRaw(scratch);
                    } else {
                        firstValue.ofRawNull();
                    }
                } else {
                    firstValue.ofRawNull();
                }
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, firstValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, firstValue.getLl());
        }

        @Override
        public void reopen() {
            firstValue.ofRawNull();
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
            firstValue.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    static class Decimal256FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final Decimal256 firstValue = new Decimal256();
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final Decimal256 scratch = new Decimal256();
        protected final Decimal256 scratchAlt = new Decimal256();
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;

        public Decimal256FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
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
            this.buffer = memory;
            this.type = type;
            firstValue.ofRawNull();
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
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (!frameLoBounded && count == bufferSize) {
                buffer.getDecimal256((long) loIdx * Decimal256.BYTES, firstValue);
                return;
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue.copyRaw(scratch);
            } else if (count > bufferSize - frameSize) {
                buffer.getDecimal256((loIdx + bufferSize - count) % bufferSize * Decimal256.BYTES, firstValue);
            } else {
                firstValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putDecimal256((long) loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(firstValue);
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
            Unsafe.putLong(addr, firstValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, firstValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, firstValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, firstValue.getLl());
        }

        @Override
        public void reopen() {
            firstValue.ofRawNull();
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue.ofRawNull();
            count = 0;
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
            firstValue.ofRawNull();
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal256((long) i * Decimal256.BYTES,
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
            }
        }
    }

    static class Decimal256FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;
        protected final Decimal256 value = new Decimal256();

        public Decimal256FirstValueOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_DECIMAL256_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_DECIMAL256_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            boolean isNew = mapValue.isNew();
            if (isNew && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (isNew || (liveView && mapValue.getByte(1) == 0)) {
                arg.getDecimal256(record, value);
                mapValue.putDecimal256(0, value);
                if (liveView) {
                    mapValue.putByte(1, (byte) 1);
                }
            } else {
                mapValue.getDecimal256(0, value);
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
        public Map getPartitionMap() {
            return map;
        }

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
                    : FIRST_VALUE_DECIMAL256_TYPES.getColumnCount();
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Mark the partition uninitialized so the next
            // computeNext recaptures the first value on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv != null) {
                mv.putByte(1, (byte) 0);
                if (!mv.isNew() && tombstoneValueIndex >= 0 && mv.getByte(tombstoneValueIndex) != 1) {
                    mv.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            source.getDecimal256(offset, scratch);
            value.putDecimal256(0, scratch);
            offset += 4 * Long.BYTES;
            value.putByte(1, source.getByte(offset));
            offset += Byte.BYTES;
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
            value.getDecimal256(0, scratch);
            sink.putDecimal256(scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            sink.putByte(value.getByte(1));
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    static class Decimal256FirstValueOverWholeResultSetFunction extends BaseWindowFunction {

        protected final Decimal256 scratch = new Decimal256();
        protected final int type;
        protected final Decimal256 value = new Decimal256();
        protected boolean found;

        public Decimal256FirstValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                arg.getDecimal256(record, value);
                found = true;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                arg.getDecimal256(record, value);
                found = true;
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
            found = false;
            value.ofRawNull();
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value.ofRawNull();
        }
    }

    static class Decimal32FirstNotNullOverPartitionFunction extends Decimal32FirstValueOverPartitionFunction {

        public Decimal32FirstNotNullOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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
                int d = arg.getDecimal32(record);
                if (d != Decimals.DECIMAL32_NULL) {
                    MapValue value = key.createValue();
                    value.putInt(0, d);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            int val = value != null ? value.getInt(0) : Decimals.DECIMAL32_NULL;
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal32FirstNotNullOverPartitionRangeFrameFunction extends Decimal32FirstValueOverPartitionRangeFrameFunction {

        public Decimal32FirstNotNullOverPartitionRangeFrameFunction(
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

            if (mapValue.isNew()) {
                int d = arg.getDecimal32(record);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Decimals.DECIMAL32_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putInt(startOffset + Long.BYTES, d);
                    size = 1;
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                } else {
                    size = 0;
                    firstValue = Decimals.DECIMAL32_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) {
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            firstValue = memory.getInt(startOffset + Long.BYTES);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue = Decimals.DECIMAL32_NULL;
                        }
                    } else {
                        firstValue = memory.getInt(startOffset + Long.BYTES);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            firstValue = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                int d = arg.getDecimal32(record);
                if (d != Decimals.DECIMAL32_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                if (!findNewFirstValue) {
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
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

    public static class Decimal32FirstNotNullOverPartitionRowsFrameFunction extends Decimal32FirstValueOverPartitionRowsFrameFunction {

        public Decimal32FirstNotNullOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putInt(startOffset + (long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = memory.getInt(startOffset);
                    return;
                }
                int d = arg.getDecimal32(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL32_NULL) {
                    firstNotNullIdx = count;
                    memory.putInt(startOffset, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                } else {
                    firstValue = Decimals.DECIMAL32_NULL;
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                int d = arg.getDecimal32(record);
                if (firstNotNullIdx != -1 && memory.getInt(startOffset + loIdx * Integer.BYTES) != Decimals.DECIMAL32_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = memory.getInt(startOffset + firstNotNullIdx * Integer.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        int res = memory.getInt(startOffset + (loIdx + i) % bufferSize * Integer.BYTES);
                        if (res != Decimals.DECIMAL32_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putInt(startOffset + loIdx * Integer.BYTES, d);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal32FirstNotNullOverRangeFrameFunction extends Decimal32FirstValueOverRangeFrameFunction implements Reopenable {

        public Decimal32FirstNotNullOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) {
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        firstValue = memory.getInt(startOffset + Long.BYTES);
                    } else {
                        firstValue = Decimals.DECIMAL32_NULL;
                    }
                } else {
                    firstValue = memory.getInt(startOffset + Long.BYTES);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        findNewFirstValue = true;
                        firstValue = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    }
                    break;
                }
            }
            firstIdx = newFirstIdx;
            int d = arg.getDecimal32(record);
            if (d != Decimals.DECIMAL32_NULL) {
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
                memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            if (!findNewFirstValue) {
                firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal32FirstNotNullOverRowsFrameFunction extends Decimal32FirstValueOverRowsFrameFunction implements Reopenable {
        private long firstNotNullIdx = -1;

        public Decimal32FirstNotNullOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg, rowsLo, rowsHi, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getInt(0);
                    return;
                }
                int d = arg.getDecimal32(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL32_NULL) {
                    firstNotNullIdx = count;
                    buffer.putInt(0, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                } else {
                    firstValue = Decimals.DECIMAL32_NULL;
                }
                count++;
            } else {
                int d = arg.getDecimal32(record);
                if (firstNotNullIdx != -1 && buffer.getInt((long) loIdx * Integer.BYTES) != Decimals.DECIMAL32_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = buffer.getInt(firstNotNullIdx * Integer.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        int res = buffer.getInt((long) (loIdx + i) % bufferSize * Integer.BYTES);
                        if (res != Decimals.DECIMAL32_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putInt((long) loIdx * Integer.BYTES, d);
                loIdx = (loIdx + 1) % bufferSize;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    static class Decimal32FirstNotNullOverUnboundedPartitionRowsFrameFunction extends Decimal32FirstValueOverUnboundedPartitionRowsFrameFunction {

        public Decimal32FirstNotNullOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg, type, partitionByKeyTypes, liveView, configuration);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (liveView) {
                MapValue mapValue = key.createValue();
                boolean isNew = mapValue.isNew();
                if (isNew) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (isNew || mapValue.getByte(1) == 0) {
                    int d = arg.getDecimal32(record);
                    if (d != Decimals.DECIMAL32_NULL) {
                        mapValue.putInt(0, d);
                        mapValue.putByte(1, (byte) 1);
                        value = d;
                    } else {
                        mapValue.putInt(0, Decimals.DECIMAL32_NULL);
                        mapValue.putByte(1, (byte) 0);
                        value = Decimals.DECIMAL32_NULL;
                    }
                } else {
                    value = mapValue.getInt(0);
                }
            } else {
                MapValue mapValue = key.findValue();
                if (mapValue != null) {
                    value = mapValue.getInt(0);
                } else {
                    int d = arg.getDecimal32(record);
                    if (d != Decimals.DECIMAL32_NULL) {
                        mapValue = key.createValue();
                        mapValue.putInt(0, d);
                        value = d;
                    } else {
                        value = Decimals.DECIMAL32_NULL;
                    }
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal32FirstNotNullOverWholeResultSetFunction extends Decimal32FirstValueOverWholeResultSetFunction {

        public Decimal32FirstNotNullOverWholeResultSetFunction(Function arg, int type) {
            super(arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                int d = arg.getDecimal32(record);
                if (d != Decimals.DECIMAL32_NULL) {
                    value = d;
                    found = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL32_NULL;
        }
    }

    static class Decimal32FirstValueOverCurrentRowFunction extends BaseWindowFunction {

        private final boolean ignoreNulls;
        private final int type;
        private int value;

        Decimal32FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls, int type) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
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
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal32FirstValueOverPartitionFunction extends BasePartitionedWindowFunction {

        protected final int type;
        protected int firstValue;

        public Decimal32FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                firstValue = arg.getDecimal32(record);
                value.putInt(0, firstValue);
            } else {
                firstValue = value.getInt(0);
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return firstValue;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    public static class Decimal32FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected int firstValue;

        public Decimal32FirstValueOverPartitionRangeFrameFunction(
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
            this.frameIncludesCurrentValue = rangeHi == 0;
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

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            int d = arg.getDecimal32(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putInt(startOffset + Long.BYTES, d);
                size = 1;
                if (frameIncludesCurrentValue) {
                    firstValue = d;
                    frameSize = 1;
                } else {
                    firstValue = Decimals.DECIMAL32_NULL;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                if (!frameLoBounded && frameSize > 0) {
                    firstValue = memory.getInt(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
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

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;

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
                    firstValue = memory.getInt(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    firstValue = Decimals.DECIMAL32_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public int getDecimal32(Record rec) {
            return firstValue;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            firstValue = Decimals.DECIMAL32_NULL;
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
            sink.val(maxDiff);
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
        }
    }

    public static class Decimal32FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int type;
        protected int firstValue;

        public Decimal32FirstValueOverPartitionRowsFrameFunction(
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
            long count;
            int d = arg.getDecimal32(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putInt(startOffset + (long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);
                if (!frameLoBounded && count == bufferSize) {
                    firstValue = memory.getInt(startOffset + loIdx * Integer.BYTES);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = memory.getInt(startOffset + (loIdx + bufferSize - count) % bufferSize * Integer.BYTES);
            } else {
                firstValue = Decimals.DECIMAL32_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);
            memory.putInt(startOffset + loIdx * Integer.BYTES, d);
        }

        @Override
        public int getDecimal32(Record rec) {
            return firstValue;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), firstValue);
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal32FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        protected static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected int firstValue;
        protected long size;
        protected long startOffset;

        public Decimal32FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            this(rangeLo, rangeHi, arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER),
                    timestampIdx,
                    type);
        }

        public Decimal32FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
            firstValue = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            int d = arg.getDecimal32(record);

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
            memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;

            if (frameLoBounded) {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        firstValue = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL32_NULL;
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstValue = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL32_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL32_NULL;
                }
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return firstValue;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL32_NULL;
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
            firstValue = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    static class Decimal32FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final int type;
        protected long count = 0;
        protected int firstValue;
        protected int loIdx = 0;

        public Decimal32FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
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
            this.buffer = memory;
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
        }

        @Override
        public void computeNext(Record record) {
            int d = arg.getDecimal32(record);
            if (!frameLoBounded && count == bufferSize) {
                firstValue = buffer.getInt((long) loIdx * Integer.BYTES);
                return;
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = buffer.getInt((loIdx + bufferSize - count) % bufferSize * Integer.BYTES);
            } else {
                firstValue = Decimals.DECIMAL32_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putInt((long) loIdx * Integer.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public int getDecimal32(Record rec) {
            return firstValue;
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
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue = Decimals.DECIMAL32_NULL;
            count = 0;
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
            firstValue = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putInt((long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
            }
        }
    }

    static class Decimal32FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
        protected final int type;
        protected int value = Decimals.DECIMAL32_NULL;

        public Decimal32FirstValueOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_DECIMAL32_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_DECIMAL32_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            boolean isNew = mapValue.isNew();
            if (isNew && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (isNew || (liveView && mapValue.getByte(1) == 0)) {
                value = arg.getDecimal32(record);
                mapValue.putInt(0, value);
                if (liveView) {
                    mapValue.putByte(1, (byte) 1);
                }
            } else {
                value = mapValue.getInt(0);
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
        public Map getPartitionMap() {
            return map;
        }

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
                    : FIRST_VALUE_DECIMAL32_TYPES.getColumnCount();
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Mark the partition uninitialized so the next
            // computeNext recaptures the first value on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv != null) {
                mv.putByte(1, (byte) 0);
                if (!mv.isNew() && tombstoneValueIndex >= 0 && mv.getByte(tombstoneValueIndex) != 1) {
                    mv.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            value.putInt(0, source.getInt(offset));
            offset += Integer.BYTES;
            value.putByte(1, source.getByte(offset));
            offset += Byte.BYTES;
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
            sink.putInt(value.getInt(0));
            sink.putByte(value.getByte(1));
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    static class Decimal32FirstValueOverWholeResultSetFunction extends BaseWindowFunction {

        protected final int type;
        protected boolean found;
        protected int value = Decimals.DECIMAL32_NULL;

        public Decimal32FirstValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                value = arg.getDecimal32(record);
                found = true;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = arg.getDecimal32(record);
                found = true;
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL32_NULL;
        }
    }

    static class Decimal64FirstNotNullOverPartitionFunction extends Decimal64FirstValueOverPartitionFunction {

        public Decimal64FirstNotNullOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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

    public static class Decimal64FirstNotNullOverPartitionRangeFrameFunction extends Decimal64FirstValueOverPartitionRangeFrameFunction {

        public Decimal64FirstNotNullOverPartitionRangeFrameFunction(
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

            if (mapValue.isNew()) {
                long d = arg.getDecimal64(record);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Decimals.DECIMAL64_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    size = 1;
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
                } else {
                    size = 0;
                    firstValue = Decimals.DECIMAL64_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) {
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            firstValue = memory.getLong(startOffset + Long.BYTES);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue = Decimals.DECIMAL64_NULL;
                        }
                    } else {
                        firstValue = memory.getLong(startOffset + Long.BYTES);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            firstValue = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                long d = arg.getDecimal64(record);
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

                if (!findNewFirstValue) {
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
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

    public static class Decimal64FirstNotNullOverPartitionRowsFrameFunction extends Decimal64FirstValueOverPartitionRowsFrameFunction {

        public Decimal64FirstNotNullOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

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
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = memory.getLong(startOffset);
                    return;
                }
                long d = arg.getDecimal64(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL64_NULL) {
                    firstNotNullIdx = count;
                    memory.putLong(startOffset, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
                } else {
                    firstValue = Decimals.DECIMAL64_NULL;
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                long d = arg.getDecimal64(record);
                if (firstNotNullIdx != -1 && memory.getLong(startOffset + loIdx * Long.BYTES) != Decimals.DECIMAL64_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = memory.getLong(startOffset + firstNotNullIdx * Long.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        long res = memory.getLong(startOffset + (loIdx + i) % bufferSize * Long.BYTES);
                        if (res != Decimals.DECIMAL64_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
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

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal64FirstNotNullOverRangeFrameFunction extends Decimal64FirstValueOverRangeFrameFunction implements Reopenable {

        public Decimal64FirstNotNullOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) {
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        firstValue = memory.getLong(startOffset + Long.BYTES);
                    } else {
                        firstValue = Decimals.DECIMAL64_NULL;
                    }
                } else {
                    firstValue = memory.getLong(startOffset + Long.BYTES);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        findNewFirstValue = true;
                        firstValue = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    }
                    break;
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

            if (!findNewFirstValue) {
                firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal64FirstNotNullOverRowsFrameFunction extends Decimal64FirstValueOverRowsFrameFunction implements Reopenable {
        private long firstNotNullIdx = -1;

        public Decimal64FirstNotNullOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg, rowsLo, rowsHi, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getLong(0);
                    return;
                }
                long d = arg.getDecimal64(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL64_NULL) {
                    firstNotNullIdx = count;
                    buffer.putLong(0, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
                } else {
                    firstValue = Decimals.DECIMAL64_NULL;
                }
                count++;
            } else {
                long d = arg.getDecimal64(record);
                if (firstNotNullIdx != -1 && buffer.getLong((long) loIdx * Long.BYTES) != Decimals.DECIMAL64_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = buffer.getLong(firstNotNullIdx * Long.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        long res = buffer.getLong((long) (loIdx + i) % bufferSize * Long.BYTES);
                        if (res != Decimals.DECIMAL64_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
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
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    static class Decimal64FirstNotNullOverUnboundedPartitionRowsFrameFunction extends Decimal64FirstValueOverUnboundedPartitionRowsFrameFunction {

        public Decimal64FirstNotNullOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg, type, partitionByKeyTypes, liveView, configuration);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (liveView) {
                MapValue mapValue = key.createValue();
                boolean isNew = mapValue.isNew();
                if (isNew) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (isNew || mapValue.getByte(1) == 0) {
                    long d = arg.getDecimal64(record);
                    if (d != Decimals.DECIMAL64_NULL) {
                        mapValue.putLong(0, d);
                        mapValue.putByte(1, (byte) 1);
                        value = d;
                    } else {
                        mapValue.putLong(0, Decimals.DECIMAL64_NULL);
                        mapValue.putByte(1, (byte) 0);
                        value = Decimals.DECIMAL64_NULL;
                    }
                } else {
                    value = mapValue.getLong(0);
                }
            } else {
                MapValue mapValue = key.findValue();
                if (mapValue != null) {
                    value = mapValue.getLong(0);
                } else {
                    long d = arg.getDecimal64(record);
                    if (d != Decimals.DECIMAL64_NULL) {
                        mapValue = key.createValue();
                        mapValue.putLong(0, d);
                        value = d;
                    } else {
                        value = Decimals.DECIMAL64_NULL;
                    }
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal64FirstNotNullOverWholeResultSetFunction extends Decimal64FirstValueOverWholeResultSetFunction {

        public Decimal64FirstNotNullOverWholeResultSetFunction(Function arg, int type) {
            super(arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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
                    value = d;
                    found = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL64_NULL;
        }
    }

    static class Decimal64FirstValueOverCurrentRowFunction extends BaseWindowFunction {

        private final boolean ignoreNulls;
        private final int type;
        private long value;

        Decimal64FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls, int type) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
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
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal64FirstValueOverPartitionFunction extends BasePartitionedWindowFunction {

        protected final int type;
        protected long firstValue;

        public Decimal64FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                firstValue = arg.getDecimal64(record);
                value.putLong(0, firstValue);
            } else {
                firstValue = value.getLong(0);
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return firstValue;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    public static class Decimal64FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long firstValue;

        public Decimal64FirstValueOverPartitionRangeFrameFunction(
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
            this.frameIncludesCurrentValue = rangeHi == 0;
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

            long frameSize;
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
                if (frameIncludesCurrentValue) {
                    firstValue = d;
                    frameSize = 1;
                } else {
                    firstValue = Decimals.DECIMAL64_NULL;
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
                    firstValue = Decimals.DECIMAL64_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public long getDecimal64(Record rec) {
            return firstValue;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            firstValue = Decimals.DECIMAL64_NULL;
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
            sink.val(maxDiff);
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
        }
    }

    public static class Decimal64FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int type;
        protected long firstValue;

        public Decimal64FirstValueOverPartitionRowsFrameFunction(
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
            long count;
            long d = arg.getDecimal64(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);
                if (!frameLoBounded && count == bufferSize) {
                    firstValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = memory.getLong(startOffset + (loIdx + bufferSize - count) % bufferSize * Long.BYTES);
            } else {
                firstValue = Decimals.DECIMAL64_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        @Override
        public long getDecimal64(Record rec) {
            return firstValue;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal64FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        protected static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long firstValue;
        protected long size;
        protected long startOffset;

        public Decimal64FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            this(rangeLo, rangeHi, arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER),
                    timestampIdx,
                    type);
        }

        public Decimal64FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
            firstValue = Decimals.DECIMAL64_NULL;
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

            if (frameLoBounded) {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        firstValue = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL64_NULL;
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstValue = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL64_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL64_NULL;
                }
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return firstValue;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL64_NULL;
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
            firstValue = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    static class Decimal64FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final int type;
        protected long count = 0;
        protected long firstValue;
        protected int loIdx = 0;

        public Decimal64FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
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
            this.buffer = memory;
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
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (!frameLoBounded && count == bufferSize) {
                firstValue = buffer.getLong((long) loIdx * Long.BYTES);
                return;
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = buffer.getLong((loIdx + bufferSize - count) % bufferSize * Long.BYTES);
            } else {
                firstValue = Decimals.DECIMAL64_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getDecimal64(Record rec) {
            return firstValue;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue = Decimals.DECIMAL64_NULL;
            count = 0;
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
            firstValue = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
        }
    }

    static class Decimal64FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
        protected final int type;
        protected long value = Decimals.DECIMAL64_NULL;

        public Decimal64FirstValueOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_DECIMAL64_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_DECIMAL64_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            boolean isNew = mapValue.isNew();
            if (isNew && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (isNew || (liveView && mapValue.getByte(1) == 0)) {
                value = arg.getDecimal64(record);
                mapValue.putLong(0, value);
                if (liveView) {
                    mapValue.putByte(1, (byte) 1);
                }
            } else {
                value = mapValue.getLong(0);
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
        public Map getPartitionMap() {
            return map;
        }

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
                    : FIRST_VALUE_DECIMAL64_TYPES.getColumnCount();
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Mark the partition uninitialized so the next
            // computeNext recaptures the first value on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv != null) {
                mv.putByte(1, (byte) 0);
                if (!mv.isNew() && tombstoneValueIndex >= 0 && mv.getByte(tombstoneValueIndex) != 1) {
                    mv.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            value.putLong(0, source.getLong(offset));
            offset += Long.BYTES;
            value.putByte(1, source.getByte(offset));
            offset += Byte.BYTES;
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
            sink.putByte(value.getByte(1));
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    static class Decimal64FirstValueOverWholeResultSetFunction extends BaseWindowFunction {

        protected final int type;
        protected boolean found;
        protected long value = Decimals.DECIMAL64_NULL;

        public Decimal64FirstValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                value = arg.getDecimal64(record);
                found = true;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = arg.getDecimal64(record);
                found = true;
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL64_NULL;
        }
    }

    static class Decimal8FirstNotNullOverPartitionFunction extends Decimal8FirstValueOverPartitionFunction {

        public Decimal8FirstNotNullOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
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
                byte d = arg.getDecimal8(record);
                if (d != Decimals.DECIMAL8_NULL) {
                    MapValue value = key.createValue();
                    value.putByte(0, d);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            byte val = value != null ? value.getByte(0) : Decimals.DECIMAL8_NULL;
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    public static class Decimal8FirstNotNullOverPartitionRangeFrameFunction extends Decimal8FirstValueOverPartitionRangeFrameFunction {

        public Decimal8FirstNotNullOverPartitionRangeFrameFunction(
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

            if (mapValue.isNew()) {
                byte d = arg.getDecimal8(record);
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Decimals.DECIMAL8_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putByte(startOffset + Long.BYTES, d);
                    size = 1;
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                } else {
                    size = 0;
                    firstValue = Decimals.DECIMAL8_NULL;
                }
            } else {
                startOffset = mapValue.getLong(0);
                size = mapValue.getLong(1);
                capacity = mapValue.getLong(2);
                firstIdx = mapValue.getLong(3);
                if (!frameLoBounded && size > 0) {
                    if (firstIdx == 0) {
                        long ts = memory.getLong(startOffset);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            firstIdx = 1;
                            firstValue = memory.getByte(startOffset + Long.BYTES);
                            mapValue.putLong(3, firstIdx);
                        } else {
                            firstValue = Decimals.DECIMAL8_NULL;
                        }
                    } else {
                        firstValue = memory.getByte(startOffset + Long.BYTES);
                    }
                    return;
                }

                long newFirstIdx = firstIdx;
                boolean findNewFirstValue = false;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            findNewFirstValue = true;
                            firstValue = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        }
                        break;
                    }
                }
                firstIdx = newFirstIdx;
                byte d = arg.getDecimal8(record);
                if (d != Decimals.DECIMAL8_NULL) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                if (!findNewFirstValue) {
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
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

    public static class Decimal8FirstNotNullOverPartitionRowsFrameFunction extends Decimal8FirstValueOverPartitionRowsFrameFunction {

        public Decimal8FirstNotNullOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long firstNotNullIdx = -1;
            long count = 0;

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
                firstNotNullIdx = value.getLong(2);
                count = value.getLong(3);
            }

            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = memory.getByte(startOffset);
                    return;
                }
                byte d = arg.getDecimal8(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL8_NULL) {
                    firstNotNullIdx = count;
                    memory.putByte(startOffset, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                } else {
                    firstValue = Decimals.DECIMAL8_NULL;
                }
                value.putLong(2, firstNotNullIdx);
                value.putLong(3, count + 1);
            } else {
                byte d = arg.getDecimal8(record);
                if (firstNotNullIdx != -1 && memory.getByte(startOffset + loIdx * Byte.BYTES) != Decimals.DECIMAL8_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = memory.getByte(startOffset + firstNotNullIdx * Byte.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        byte res = memory.getByte(startOffset + (loIdx + i) % bufferSize * Byte.BYTES);
                        if (res != Decimals.DECIMAL8_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                value.putLong(0, (loIdx + 1) % bufferSize);
                value.putLong(2, firstNotNullIdx);
                memory.putByte(startOffset + loIdx * Byte.BYTES, d);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal8FirstNotNullOverRangeFrameFunction extends Decimal8FirstValueOverRangeFrameFunction implements Reopenable {

        public Decimal8FirstNotNullOverRangeFrameFunction(
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
            long timestamp = record.getTimestamp(timestampIndex);
            if (!frameLoBounded && size > 0) {
                if (firstIdx == 0) {
                    long ts = memory.getLong(startOffset);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstIdx = 1;
                        firstValue = memory.getByte(startOffset + Long.BYTES);
                    } else {
                        firstValue = Decimals.DECIMAL8_NULL;
                    }
                } else {
                    firstValue = memory.getByte(startOffset + Long.BYTES);
                }
                return;
            }

            long newFirstIdx = firstIdx;
            boolean findNewFirstValue = false;
            for (long i = 0, n = size; i < n; i++) {
                long idx = (firstIdx + i) % capacity;
                long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                if (Math.abs(timestamp - ts) > maxDiff) {
                    newFirstIdx = (idx + 1) % capacity;
                    size--;
                } else {
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        findNewFirstValue = true;
                        firstValue = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    }
                    break;
                }
            }
            firstIdx = newFirstIdx;
            byte d = arg.getDecimal8(record);
            if (d != Decimals.DECIMAL8_NULL) {
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
                memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            if (!findNewFirstValue) {
                firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal8FirstNotNullOverRowsFrameFunction extends Decimal8FirstValueOverRowsFrameFunction implements Reopenable {
        private long firstNotNullIdx = -1;

        public Decimal8FirstNotNullOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg, rowsLo, rowsHi, memory, type);
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded) {
                if (firstNotNullIdx != -1 && count - bufferSize >= firstNotNullIdx) {
                    firstValue = buffer.getByte(0);
                    return;
                }
                byte d = arg.getDecimal8(record);
                if (firstNotNullIdx == -1 && d != Decimals.DECIMAL8_NULL) {
                    firstNotNullIdx = count;
                    buffer.putByte(0, d);
                    firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                } else {
                    firstValue = Decimals.DECIMAL8_NULL;
                }
                count++;
            } else {
                byte d = arg.getDecimal8(record);
                if (firstNotNullIdx != -1 && buffer.getByte((long) loIdx * Byte.BYTES) != Decimals.DECIMAL8_NULL) {
                    firstNotNullIdx = -1;
                }
                if (firstNotNullIdx != -1) {
                    firstValue = buffer.getByte(firstNotNullIdx * Byte.BYTES);
                } else {
                    boolean find = false;
                    for (int i = 0; i < frameSize; i++) {
                        byte res = buffer.getByte((long) (loIdx + i) % bufferSize * Byte.BYTES);
                        if (res != Decimals.DECIMAL8_NULL) {
                            find = true;
                            firstNotNullIdx = (loIdx + i) % bufferSize;
                            firstValue = res;
                            break;
                        }
                    }
                    if (!find) {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                    }
                }
                if (firstNotNullIdx == loIdx) {
                    firstNotNullIdx = -1;
                }
                buffer.putByte((long) loIdx * Byte.BYTES, d);
                loIdx = (loIdx + 1) % bufferSize;
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void reopen() {
            super.reopen();
            firstNotNullIdx = -1;
        }

        @Override
        public void reset() {
            super.reset();
            firstNotNullIdx = -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            firstNotNullIdx = -1;
        }
    }

    static class Decimal8FirstNotNullOverUnboundedPartitionRowsFrameFunction extends Decimal8FirstValueOverUnboundedPartitionRowsFrameFunction {

        public Decimal8FirstNotNullOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg, type, partitionByKeyTypes, liveView, configuration);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            if (liveView) {
                MapValue mapValue = key.createValue();
                boolean isNew = mapValue.isNew();
                if (isNew) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
                if (isNew || mapValue.getByte(1) == 0) {
                    byte d = arg.getDecimal8(record);
                    if (d != Decimals.DECIMAL8_NULL) {
                        mapValue.putByte(0, d);
                        mapValue.putByte(1, (byte) 1);
                        value = d;
                    } else {
                        mapValue.putByte(0, Decimals.DECIMAL8_NULL);
                        mapValue.putByte(1, (byte) 0);
                        value = Decimals.DECIMAL8_NULL;
                    }
                } else {
                    value = mapValue.getByte(0);
                }
            } else {
                MapValue mapValue = key.findValue();
                if (mapValue != null) {
                    value = mapValue.getByte(0);
                } else {
                    byte d = arg.getDecimal8(record);
                    if (d != Decimals.DECIMAL8_NULL) {
                        mapValue = key.createValue();
                        mapValue.putByte(0, d);
                        value = d;
                    } else {
                        value = Decimals.DECIMAL8_NULL;
                    }
                }
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }
    }

    public static class Decimal8FirstNotNullOverWholeResultSetFunction extends Decimal8FirstValueOverWholeResultSetFunction {

        public Decimal8FirstNotNullOverWholeResultSetFunction(Function arg, int type) {
            super(arg, type);
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public boolean isIgnoreNulls() {
            return true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                byte d = arg.getDecimal8(record);
                if (d != Decimals.DECIMAL8_NULL) {
                    value = d;
                    found = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL8_NULL;
        }
    }

    static class Decimal8FirstValueOverCurrentRowFunction extends BaseWindowFunction {

        private final boolean ignoreNulls;
        private final int type;
        private byte value;

        Decimal8FirstValueOverCurrentRowFunction(Function arg, boolean ignoreNulls, int type) {
            super(arg);
            this.ignoreNulls = ignoreNulls;
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
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal8FirstValueOverPartitionFunction extends BasePartitionedWindowFunction {

        protected final int type;
        protected byte firstValue;

        public Decimal8FirstValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                firstValue = arg.getDecimal8(record);
                value.putByte(0, firstValue);
            } else {
                firstValue = value.getByte(0);
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return firstValue;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), firstValue);
        }
    }

    public static class Decimal8FirstValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected byte firstValue;

        public Decimal8FirstValueOverPartitionRangeFrameFunction(
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
            this.frameIncludesCurrentValue = rangeHi == 0;
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

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            byte d = arg.getDecimal8(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putByte(startOffset + Long.BYTES, d);
                size = 1;
                if (frameIncludesCurrentValue) {
                    firstValue = d;
                    frameSize = 1;
                } else {
                    firstValue = Decimals.DECIMAL8_NULL;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                if (!frameLoBounded && frameSize > 0) {
                    firstValue = memory.getByte(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
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

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;

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
                    firstValue = memory.getByte(startOffset + firstIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    firstValue = Decimals.DECIMAL8_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return firstValue;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            firstValue = Decimals.DECIMAL8_NULL;
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
            sink.val(maxDiff);
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
        }
    }

    public static class Decimal8FirstValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int type;
        protected byte firstValue;

        public Decimal8FirstValueOverPartitionRowsFrameFunction(
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
            long count;
            byte d = arg.getDecimal8(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);
                if (!frameLoBounded && count == bufferSize) {
                    firstValue = memory.getByte(startOffset + loIdx * Byte.BYTES);
                    return;
                }
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = memory.getByte(startOffset + (loIdx + bufferSize - count) % bufferSize * Byte.BYTES);
            } else {
                firstValue = Decimals.DECIMAL8_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);
            memory.putByte(startOffset + loIdx * Byte.BYTES, d);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return firstValue;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), firstValue);
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
            sink.val('(').val(arg).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal8FirstValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        protected static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected byte firstValue;
        protected long size;
        protected long startOffset;

        public Decimal8FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            this(rangeLo, rangeHi, arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER),
                    timestampIdx,
                    type);
        }

        public Decimal8FirstValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
            firstValue = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            byte d = arg.getDecimal8(record);

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
            memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;

            if (frameLoBounded) {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        firstValue = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL8_NULL;
                }
            } else {
                if (size > 0) {
                    long idx = firstIdx;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        firstValue = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                    } else {
                        firstValue = frameIncludesCurrentValue ? d : Decimals.DECIMAL8_NULL;
                    }
                } else {
                    firstValue = Decimals.DECIMAL8_NULL;
                }
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return firstValue;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL8_NULL;
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
            firstValue = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }
    }

    static class Decimal8FirstValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final int type;
        protected long count = 0;
        protected byte firstValue;
        protected int loIdx = 0;

        public Decimal8FirstValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
            super(arg);
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
            this.buffer = memory;
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
        }

        @Override
        public void computeNext(Record record) {
            byte d = arg.getDecimal8(record);
            if (!frameLoBounded && count == bufferSize) {
                firstValue = buffer.getByte((long) loIdx * Byte.BYTES);
                return;
            }

            if (count == 0 && frameIncludesCurrentValue) {
                firstValue = d;
            } else if (count > bufferSize - frameSize) {
                firstValue = buffer.getByte((loIdx + bufferSize - count) % bufferSize * Byte.BYTES);
            } else {
                firstValue = Decimals.DECIMAL8_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putByte((long) loIdx * Byte.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return firstValue;
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
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), firstValue);
        }

        @Override
        public void reopen() {
            firstValue = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            firstValue = Decimals.DECIMAL8_NULL;
            count = 0;
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
            firstValue = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
        }
    }

    static class Decimal8FirstValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        protected final CairoConfiguration configuration;
        protected final ArrayColumnTypes keyColumnTypes;
        protected final boolean liveView;
        protected final ArrayColumnTypes mapValueTypes;
        protected final int type;
        protected byte value = Decimals.DECIMAL8_NULL;

        public Decimal8FirstValueOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int type,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = FIRST_VALUE_DECIMAL8_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(FIRST_VALUE_DECIMAL8_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            boolean isNew = mapValue.isNew();
            if (isNew && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            if (isNew || (liveView && mapValue.getByte(1) == 0)) {
                value = arg.getDecimal8(record);
                mapValue.putByte(0, value);
                if (liveView) {
                    mapValue.putByte(1, (byte) 1);
                }
            } else {
                value = mapValue.getByte(0);
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
        public Map getPartitionMap() {
            return map;
        }

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
                    : FIRST_VALUE_DECIMAL8_TYPES.getColumnCount();
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Mark the partition uninitialized so the next
            // computeNext recaptures the first value on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv != null) {
                mv.putByte(1, (byte) 0);
                if (!mv.isNew() && tombstoneValueIndex >= 0 && mv.getByte(tombstoneValueIndex) != 1) {
                    mv.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            value.putByte(0, source.getByte(offset));
            offset += Byte.BYTES;
            value.putByte(1, source.getByte(offset));
            offset += Byte.BYTES;
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
            sink.putByte(value.getByte(0));
            sink.putByte(value.getByte(1));
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    static class Decimal8FirstValueOverWholeResultSetFunction extends BaseWindowFunction {

        protected final int type;
        protected boolean found;
        protected byte value = Decimals.DECIMAL8_NULL;

        public Decimal8FirstValueOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                value = arg.getDecimal8(record);
                found = true;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                value = arg.getDecimal8(record);
                found = true;
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            found = false;
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toTop() {
            super.toTop();
            found = false;
            value = Decimals.DECIMAL8_NULL;
        }
    }

    static {
        FIRST_VALUE_DECIMAL64_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL64_TYPES.add(ColumnType.LONG);

        FIRST_VALUE_DECIMAL64_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL64_TYPES_LV.add(ColumnType.LONG); // slot 0: captured value
        FIRST_VALUE_DECIMAL64_TYPES_LV.add(ColumnType.BYTE); // slot 1: initialized flag
        FIRST_VALUE_DECIMAL64_TYPES_LV.add(ColumnType.BYTE); // slot 2: tombstone (anchor-driven compaction)

        FIRST_VALUE_DECIMAL8_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL8_TYPES.add(ColumnType.BYTE);

        FIRST_VALUE_DECIMAL8_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL8_TYPES_LV.add(ColumnType.BYTE); // slot 0: captured value
        FIRST_VALUE_DECIMAL8_TYPES_LV.add(ColumnType.BYTE); // slot 1: initialized flag
        FIRST_VALUE_DECIMAL8_TYPES_LV.add(ColumnType.BYTE); // slot 2: tombstone (anchor-driven compaction)

        FIRST_VALUE_DECIMAL16_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL16_TYPES.add(ColumnType.SHORT);

        FIRST_VALUE_DECIMAL16_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL16_TYPES_LV.add(ColumnType.SHORT); // slot 0: captured value
        FIRST_VALUE_DECIMAL16_TYPES_LV.add(ColumnType.BYTE);  // slot 1: initialized flag
        FIRST_VALUE_DECIMAL16_TYPES_LV.add(ColumnType.BYTE);  // slot 2: tombstone (anchor-driven compaction)

        FIRST_VALUE_DECIMAL32_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL32_TYPES.add(ColumnType.INT);

        FIRST_VALUE_DECIMAL32_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL32_TYPES_LV.add(ColumnType.INT);  // slot 0: captured value
        FIRST_VALUE_DECIMAL32_TYPES_LV.add(ColumnType.BYTE); // slot 1: initialized flag
        FIRST_VALUE_DECIMAL32_TYPES_LV.add(ColumnType.BYTE); // slot 2: tombstone (anchor-driven compaction)

        FIRST_VALUE_DECIMAL128_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL128_TYPES.add(ColumnType.DECIMAL128);

        FIRST_VALUE_DECIMAL128_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL128_TYPES_LV.add(ColumnType.DECIMAL128); // slot 0: captured value
        FIRST_VALUE_DECIMAL128_TYPES_LV.add(ColumnType.BYTE);       // slot 1: initialized flag
        FIRST_VALUE_DECIMAL128_TYPES_LV.add(ColumnType.BYTE);       // slot 2: tombstone (anchor-driven compaction)

        FIRST_VALUE_DECIMAL256_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL256_TYPES.add(ColumnType.DECIMAL256);

        FIRST_VALUE_DECIMAL256_TYPES_LV = new ArrayColumnTypes();
        FIRST_VALUE_DECIMAL256_TYPES_LV.add(ColumnType.DECIMAL256); // slot 0: captured value
        FIRST_VALUE_DECIMAL256_TYPES_LV.add(ColumnType.BYTE);       // slot 1: initialized flag
        FIRST_VALUE_DECIMAL256_TYPES_LV.add(ColumnType.BYTE);       // slot 2: tombstone (anchor-driven compaction)

        FIRST_VALUE_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        FIRST_VALUE_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        FIRST_VALUE_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        FIRST_VALUE_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        FIRST_VALUE_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        FIRST_NOT_NULL_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        FIRST_VALUE_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        FIRST_VALUE_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        FIRST_VALUE_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        FIRST_VALUE_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
    }
}
