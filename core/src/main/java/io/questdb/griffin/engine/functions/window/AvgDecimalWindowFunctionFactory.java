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
import io.questdb.cairo.CairoException;
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
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.math.RoundingMode;

public class AvgDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final ArrayColumnTypes AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL128_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL64_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes AVG_DECIMAL_NARROW_TYPES;
    private static final String NAME = "avg";
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
        int framingMode = windowContext.getFramingMode();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);
        int argType = arg.getType();
        int tag = ColumnType.tagOf(argType);
        int argPos = argPositions.getQuick(0);

        if (rowsHi < rowsLo) {
            boolean isRange = framingMode == WindowExpression.FRAMING_RANGE;
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
            case ColumnType.DECIMAL8 ->
                    newInstanceDecimal8(position, args, configuration, sqlExecutionContext, argType, argPos);
            case ColumnType.DECIMAL16 ->
                    newInstanceDecimal16(position, args, configuration, sqlExecutionContext, argType, argPos);
            case ColumnType.DECIMAL32 ->
                    newInstanceDecimal32(position, args, configuration, sqlExecutionContext, argType, argPos);
            case ColumnType.DECIMAL64 ->
                    newInstanceDecimal64(position, args, configuration, sqlExecutionContext, argType, argPos);
            case ColumnType.DECIMAL128 ->
                    newInstanceDecimal128(position, args, configuration, sqlExecutionContext, argType, argPos);
            case ColumnType.DECIMAL256 ->
                    newInstanceDecimal256(position, args, configuration, sqlExecutionContext, argType, argPos);
            default -> throw SqlException.$(position, "avg is not yet implemented for ").put(ColumnType.nameOf(tag));
        };
    }

    private static void readD256(MemoryARW mem, long offset, Decimal256 sink) {
        sink.ofRaw(
                mem.getLong(offset),
                mem.getLong(offset + Long.BYTES),
                mem.getLong(offset + 2 * Long.BYTES),
                mem.getLong(offset + 3 * Long.BYTES)
        );
    }

    private Function newInstanceDecimal128(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int argType,
            int argPos
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal128AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal128AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal128AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal128AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, argPos);
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
                    return new Decimal128AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal128AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function newInstanceDecimal16(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int argType,
            int argPos
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal16AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal16AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal16AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal16AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, argPos);
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
                    return new Decimal16AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal16AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function newInstanceDecimal256(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int argType,
            int argPos
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256AvgOverCurrentRowFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_TYPES);
                    try {
                        return new Decimal256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, argPos);
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
                    return new Decimal256AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256AvgOverCurrentRowFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function newInstanceDecimal32(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int argType,
            int argPos
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal32AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal32AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal32AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal32AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, argPos);
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
                    return new Decimal32AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal32AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function newInstanceDecimal64(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int argType,
            int argPos
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL64_TYPES);
                    try {
                        return new Decimal64AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL64_TYPES);
                    try {
                        return new Decimal64AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL64_TYPES);
                    try {
                        return new Decimal64AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL64_TYPES);
                    try {
                        return new Decimal64AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, argPos);
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
                    return new Decimal64AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal64AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private Function newInstanceDecimal8(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int argType,
            int argPos
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        Function arg = args.get(0);

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal8AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal8AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal8AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_TYPES);
                    try {
                        return new Decimal8AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, argPos);
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
                    return new Decimal8AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8AvgOverUnboundedRowsFrameFunction(arg, argType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8AvgOverCurrentRowFunction(arg, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8AvgOverWholeResultSetFunction(arg, argType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal8AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal128AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        Decimal128AvgOverCurrentRowFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            value.copyFrom(scratch);
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }
    }

    static class Decimal128AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public String getName() {
            return NAME;
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
            if (!scratch.isNull()) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    acc.ofRaw(scratch.getHigh(), scratch.getLow());
                    value.putDecimal256(0, acc);
                    value.putLong(1, 1);
                } else {
                    value.getDecimal256(0, acc);
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                    value.putDecimal256(0, acc);
                    value.addLong(1, 1);
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
            if (value == null) {
                Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            } else {
                long count = value.getLong(1);
                if (count <= 0) {
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                } else {
                    value.getDecimal256(0, divScratch);
                    if (!divScratch.isNull() && divScratch.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                    divScratch.setScale(scale);
                    try {
                        divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    Unsafe.putLong(addr, divScratch.getLh());
                    Unsafe.putLong(addr + Long.BYTES, divScratch.getLl());
                }
            }
        }
    }

    public static class Decimal128AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int timestampIndex;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type,
                int position
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            boolean isNull = scratch.isNull();

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    if (frameIncludesCurrentValue) {
                        acc.ofRaw(scratch.getHigh(), scratch.getLow());
                        value.copyFrom(scratch);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        value.ofRawNull();
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
                    value.ofRawNull();
                    frameSize = 0;
                }
            } else {
                mapValue.getDecimal256(0, acc);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                                long h = scratch.getHigh();
                                acc.subtract(h < 0 ? -1L : 0L, h < 0 ? -1L : 0L, h, scratch.getLow(), 0);
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

                if (!isNull) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    long slotOffset = startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE;
                    memory.putLong(slotOffset, timestamp);
                    scratch.ofRaw(value.isNull() ? 0L : value.getHigh(), value.isNull() ? 0L : value.getLow());
                    arg.getDecimal128(record, scratch);
                    memory.putDecimal128(slotOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                            try {
                                Decimal256.uncheckedAdd(acc, scratch);
                            } catch (NumericException e) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                            }
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                            try {
                                Decimal256.uncheckedAdd(acc, scratch);
                            } catch (NumericException e) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
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

                mapValue.putDecimal256(0, acc);

                if (frameSize != 0) {
                    divScratch.copyRaw(acc);
                    divScratch.setScale(scale);
                    try {
                        divScratch.divide(0, 0, 0, frameSize, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value.ofRaw(divScratch.getLh(), divScratch.getLl());
                } else {
                    value.ofRawNull();
                }
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
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
        public void reopen() {
            acc.ofZero();
            super.reopen();
            value.ofRawNull();
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
            freeList.clear();
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
            acc.ofZero();
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal128AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type,
                int position
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
            frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            MapValue mv = key.createValue();

            long count;
            long loIdx;
            long startOffset;
            arg.getDecimal128(record, scratch);
            boolean isNull = scratch.isNull();
            long inHigh = scratch.getHigh();
            long inLow = scratch.getLow();

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal128.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !isNull) {
                    acc.ofRaw(inHigh, inLow);
                    count = 1;
                    value.ofRaw(inHigh, inLow);
                } else {
                    acc.ofRaw(0);
                    value.ofRawNull();
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Decimal128.BYTES, Decimals.DECIMAL128_HI_NULL);
                    memory.putLong(startOffset + (long) i * Decimal128.BYTES + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                }
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                long hiH;
                long hiL;
                if (frameIncludesCurrentValue) {
                    hiH = inHigh;
                    hiL = inLow;
                } else {
                    long hiOff = startOffset + ((loIdx + frameSize - 1) % bufferSize) * Decimal128.BYTES;
                    hiH = memory.getLong(hiOff);
                    hiL = memory.getLong(hiOff + Long.BYTES);
                }
                if (!Decimal128.isNull(hiH, hiL)) {
                    count++;
                    scratch.ofRaw(hiH, hiL);
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                }
                if (count != 0) {
                    divScratch.copyRaw(acc);
                    divScratch.setScale(scale);
                    try {
                        divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value.ofRaw(divScratch.getLh(), divScratch.getLl());
                } else {
                    value.ofRawNull();
                }
                if (frameLoBounded) {
                    long loH = memory.getLong(startOffset + loIdx * Decimal128.BYTES);
                    long loL = memory.getLong(startOffset + loIdx * Decimal128.BYTES + Long.BYTES);
                    if (!Decimal128.isNull(loH, loL)) {
                        acc.subtract(loH < 0 ? -1L : 0L, loH < 0 ? -1L : 0L, loH, loL, 0);
                        count--;
                    }
                }
            }

            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putLong(startOffset + loIdx * Decimal128.BYTES, inHigh);
            memory.putLong(startOffset + loIdx * Decimal128.BYTES + Long.BYTES, inLow);
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
        public void reopen() {
            acc.ofZero();
            super.reopen();
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
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
            acc.ofZero();
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal128AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int timestampIndex;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal128AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type,
                int position
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            acc.ofRaw(0);
            value.ofRawNull();
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
            boolean isNull = scratch.isNull();
            long inHigh = scratch.getHigh();
            long inLow = scratch.getLow();
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                            long h = scratch.getHigh();
                            acc.subtract(h < 0 ? -1L : 0L, h < 0 ? -1L : 0L, h, scratch.getLow(), 0);
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

            if (!isNull) {
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
                long slotOffset = startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE;
                memory.putLong(slotOffset, timestamp);
                memory.putLong(slotOffset + Long.BYTES, inHigh);
                memory.putLong(slotOffset + Long.BYTES + Long.BYTES, inLow);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                        }
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        memory.getDecimal128(startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
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
            if (frameSize != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, frameSize, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                value.ofRawNull();
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
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reopen() {
            value.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0);
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
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
            value.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal128AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long count = 0;
        private int loIdx = 0;

        public Decimal128AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
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
            long inHigh = scratch.getHigh();
            long inLow = scratch.getLow();
            long hiH;
            long hiL;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                long off = (long) ((loIdx + frameSize - 1) % bufferSize) * Decimal128.BYTES;
                hiH = buffer.getLong(off);
                hiL = buffer.getLong(off + Long.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                long off = (long) (loIdx % bufferSize) * Decimal128.BYTES;
                hiH = buffer.getLong(off);
                hiL = buffer.getLong(off + Long.BYTES);
            } else {
                hiH = inHigh;
                hiL = inLow;
            }
            if (!Decimal128.isNull(hiH, hiL)) {
                scratch.ofRaw(hiH, hiL);
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                long off = (long) loIdx * Decimal128.BYTES;
                long loH = buffer.getLong(off);
                long loL = buffer.getLong(off + Long.BYTES);
                if (!Decimal128.isNull(loH, loL)) {
                    acc.subtract(loH < 0 ? -1L : 0L, loH < 0 ? -1L : 0L, loH, loL, 0);
                    count--;
                }
            }
            long writeOff = (long) loIdx * Decimal128.BYTES;
            buffer.putLong(writeOff, inHigh);
            buffer.putLong(writeOff + Long.BYTES, inLow);
            loIdx = (loIdx + 1) % bufferSize;
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
        public void reopen() {
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
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
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Decimal128.BYTES, Decimals.DECIMAL128_HI_NULL);
                buffer.putLong((long) i * Decimal128.BYTES + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    static class Decimal128AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long count;
            if (mv.isNew()) {
                acc.ofRaw(0);
                count = 0;
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
            }

            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            if (count != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                value.ofRawNull();
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
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal128AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long count = 0;

        public Decimal128AvgOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                value.ofRawNull();
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
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal128AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long count = 0;

        public Decimal128AvgOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
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
            if (!scratch.isNull()) {
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.ofRaw(divScratch.getLh(), divScratch.getLl());
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal16AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private short value;

        Decimal16AvgOverCurrentRowFunction(Function arg, int type) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal16AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;

        public Decimal16AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public String getName() {
            return NAME;
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
            if (s != Decimals.DECIMAL16_NULL) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(0, s);
                    value.putLong(1, 1);
                } else {
                    value.putLong(0, value.getLong(0) + s);
                    value.addLong(1, 1);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            short result;
            if (value == null) {
                result = Decimals.DECIMAL16_NULL;
            } else {
                long count = value.getLong(1);
                if (count <= 0) {
                    result = Decimals.DECIMAL16_NULL;
                } else {
                    divResult.of(value.getLong(0), scale);
                    try {
                        divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    result = (short) divResult.getValue();
                }
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), result);
        }
    }

    public static class Decimal16AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {
        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private short value;

        public Decimal16AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type,
                int position
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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

            long acc;
            long frameSize;
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
                if (s != Decimals.DECIMAL16_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putShort(startOffset + Long.BYTES, s);
                    if (frameIncludesCurrentValue) {
                        acc = s;
                        value = s;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc = 0;
                        value = Decimals.DECIMAL16_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc = 0;
                    value = Decimals.DECIMAL16_NULL;
                    frameSize = 0;
                }
            } else {
                acc = mapValue.getLong(0);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc -= val;
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
                            acc += val;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc += val;
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                    firstIdx = newFirstIdx;
                }

                mapValue.putLong(0, acc);

                if (frameSize != 0) {
                    divResult.of(acc, scale);
                    try {
                        divResult.divide(frameSize, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value = (short) divResult.getValue();
                } else {
                    value = Decimals.DECIMAL16_NULL;
                }
            }

            mapValue.putLong(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
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
        public void reopen() {
            super.reopen();
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
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
        }
    }

    public static class Decimal16AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int scale;
        private final int type;
        private short value;

        public Decimal16AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type,
                int position
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
            frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            MapValue mv = key.createValue();

            long acc;
            long count;
            long loIdx;
            long startOffset;
            short s = arg.getDecimal16(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && s != Decimals.DECIMAL16_NULL) {
                    acc = s;
                    count = 1;
                    value = s;
                } else {
                    acc = 0;
                    value = Decimals.DECIMAL16_NULL;
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
            } else {
                acc = mv.getLong(0);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                short hiValue = frameIncludesCurrentValue ? s : memory.getShort(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Short.BYTES);
                if (hiValue != Decimals.DECIMAL16_NULL) {
                    count++;
                    acc += hiValue;
                }
                if (count != 0) {
                    divResult.of(acc, scale);
                    try {
                        divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value = (short) divResult.getValue();
                } else {
                    value = Decimals.DECIMAL16_NULL;
                }
                if (frameLoBounded) {
                    short loValue = memory.getShort(startOffset + loIdx * Short.BYTES);
                    if (loValue != Decimals.DECIMAL16_NULL) {
                        acc -= loValue;
                        count--;
                    }
                }
            }

            mv.putLong(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putShort(startOffset + loIdx * Short.BYTES, s);
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
        }
    }

    static class Decimal16AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private long acc = 0L;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private short value;

        public Decimal16AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type,
                int position
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            value = Decimals.DECIMAL16_NULL;
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
                        if (frameSize > 0) {
                            short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc -= val;
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
                        acc += val;
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        short val = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        acc += val;
                        frameSize++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }
            if (frameSize != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(frameSize, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (short) divResult.getValue();
            } else {
                value = Decimals.DECIMAL16_NULL;
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
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reopen() {
            acc = 0L;
            value = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            memory.close();
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
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }
    }

    static class Decimal16AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private int loIdx = 0;
        private short value;

        public Decimal16AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL16_NULL;
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
            short s = arg.getDecimal16(record);
            short hiValue = s;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getShort((long) ((loIdx + frameSize - 1) % bufferSize) * Short.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getShort((long) (loIdx % bufferSize) * Short.BYTES);
            }
            if (hiValue != Decimals.DECIMAL16_NULL) {
                acc += hiValue;
                count++;
            }
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (short) divResult.getValue();
            } else {
                value = Decimals.DECIMAL16_NULL;
            }

            if (frameLoBounded) {
                short loValue = buffer.getShort((long) loIdx * Short.BYTES);
                if (loValue != Decimals.DECIMAL16_NULL) {
                    acc -= loValue;
                    count--;
                }
            }
            buffer.putShort((long) loIdx * Short.BYTES, s);
            loIdx = (loIdx + 1) % bufferSize;
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
        public void reopen() {
            acc = 0L;
            value = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            buffer.close();
            value = Decimals.DECIMAL16_NULL;
            count = 0;
            loIdx = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over ( rows between ");
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
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
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

    static class Decimal16AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private short value;

        public Decimal16AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long acc;
            long count;
            if (mv.isNew()) {
                acc = 0L;
                count = 0;
            } else {
                acc = mv.getLong(0);
                count = mv.getLong(1);
            }

            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                acc += s;
                count++;
            }
            mv.putLong(0, acc);
            mv.putLong(1, count);
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (short) divResult.getValue();
            } else {
                value = Decimals.DECIMAL16_NULL;
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
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal16AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private short value;

        public Decimal16AvgOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                acc += s;
                count++;
            }
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (short) divResult.getValue();
            } else {
                value = Decimals.DECIMAL16_NULL;
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
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            value = Decimals.DECIMAL16_NULL;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
            count = 0;
        }
    }

    static class Decimal16AvgOverWholeResultSetFunction extends BaseWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private short value;

        public Decimal16AvgOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL16_NULL;
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
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                acc += s;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value = Decimals.DECIMAL16_NULL;
            } else {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (short) divResult.getValue();
            }
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            value = Decimals.DECIMAL16_NULL;
            count = 0;
        }

        @Override
        public void toTop() {
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL16_NULL;
            count = 0;
        }
    }

    static class Decimal256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        Decimal256AvgOverCurrentRowFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            value.copyRaw(scratch);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }
    }

    static class Decimal256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public String getName() {
            return NAME;
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
            if (!scratch.isNull()) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    acc.copyRaw(scratch);
                    value.putDecimal256(0, acc);
                    value.putLong(1, 1);
                } else {
                    value.getDecimal256(0, acc);
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                    value.putDecimal256(0, acc);
                    value.addLong(1, 1);
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
            if (value == null) {
                Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            } else {
                long count = value.getLong(1);
                if (count <= 0) {
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                } else {
                    value.getDecimal256(0, divScratch);
                    if (!divScratch.isNull() && divScratch.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                    divScratch.setScale(scale);
                    try {
                        divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    Unsafe.putLong(addr, divScratch.getHh());
                    Unsafe.putLong(addr + Long.BYTES, divScratch.getHl());
                    Unsafe.putLong(addr + 2 * Long.BYTES, divScratch.getLh());
                    Unsafe.putLong(addr + 3 * Long.BYTES, divScratch.getLl());
                }
            }
        }
    }

    public static class Decimal256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type,
                int position
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            boolean isNull = scratch.isNull();
            long inHh = scratch.getHh();
            long inHl = scratch.getHl();
            long inLh = scratch.getLh();
            long inLl = scratch.getLl();

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, inHh);
                    memory.putLong(startOffset + Long.BYTES + Long.BYTES, inHl);
                    memory.putLong(startOffset + Long.BYTES + 2 * Long.BYTES, inLh);
                    memory.putLong(startOffset + Long.BYTES + 3 * Long.BYTES, inLl);
                    if (frameIncludesCurrentValue) {
                        acc.copyRaw(scratch);
                        value.copyRaw(scratch);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        value.ofRawNull();
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
                    value.ofRawNull();
                    frameSize = 0;
                }
            } else {
                mapValue.getDecimal256(0, acc);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                readD256(memory, startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                                acc.subtract(scratch);
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

                if (!isNull) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    long slotOffset = startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE;
                    memory.putLong(slotOffset, timestamp);
                    memory.putLong(slotOffset + Long.BYTES, inHh);
                    memory.putLong(slotOffset + Long.BYTES + Long.BYTES, inHl);
                    memory.putLong(slotOffset + Long.BYTES + 2 * Long.BYTES, inLh);
                    memory.putLong(slotOffset + Long.BYTES + 3 * Long.BYTES, inLl);
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            readD256(memory, startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                            try {
                                Decimal256.uncheckedAdd(acc, scratch);
                            } catch (NumericException e) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                            }
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            readD256(memory, startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                            try {
                                Decimal256.uncheckedAdd(acc, scratch);
                            } catch (NumericException e) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
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

                mapValue.putDecimal256(0, acc);

                if (frameSize != 0) {
                    divScratch.copyRaw(acc);
                    divScratch.setScale(scale);
                    try {
                        divScratch.divide(0, 0, 0, frameSize, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value.copyRaw(divScratch);
                } else {
                    value.ofRawNull();
                }
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
        public void reopen() {
            acc.ofZero();
            super.reopen();
            value.ofRawNull();
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
            freeList.clear();
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
            acc.ofZero();
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type,
                int position
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
            frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            MapValue mv = key.createValue();

            long count;
            long loIdx;
            long startOffset;
            arg.getDecimal256(record, scratch);
            long inHh = scratch.getHh();
            long inHl = scratch.getHl();
            long inLh = scratch.getLh();
            long inLl = scratch.getLl();
            boolean isNull = scratch.isNull();

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Decimal256.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !isNull) {
                    acc.copyRaw(scratch);
                    count = 1;
                    value.copyRaw(scratch);
                } else {
                    acc.ofRaw(0);
                    value.ofRawNull();
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    long off = startOffset + (long) i * Decimal256.BYTES;
                    memory.putLong(off, Decimals.DECIMAL256_HH_NULL);
                    memory.putLong(off + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    memory.putLong(off + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    memory.putLong(off + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                }
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                if (frameIncludesCurrentValue) {
                    if (!isNull) {
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                        }
                        count++;
                    }
                } else {
                    readD256(memory, startOffset + ((loIdx + frameSize - 1) % bufferSize) * Decimal256.BYTES, scratch);
                    if (!scratch.isNull()) {
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                        }
                        count++;
                    }
                }
                if (count != 0) {
                    divScratch.copyRaw(acc);
                    divScratch.setScale(scale);
                    try {
                        divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value.copyRaw(divScratch);
                } else {
                    value.ofRawNull();
                }
                if (frameLoBounded) {
                    readD256(memory, startOffset + loIdx * Decimal256.BYTES, scratch);
                    if (!scratch.isNull()) {
                        acc.subtract(scratch);
                        count--;
                    }
                }
            }

            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            long writeOff = startOffset + loIdx * Decimal256.BYTES;
            memory.putLong(writeOff, inHh);
            memory.putLong(writeOff + Long.BYTES, inHl);
            memory.putLong(writeOff + 2 * Long.BYTES, inLh);
            memory.putLong(writeOff + 3 * Long.BYTES, inLl);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
        public void reopen() {
            acc.ofZero();
            super.reopen();
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
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
            acc.ofZero();
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type,
                int position
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            acc.ofRaw(0);
            value.ofRawNull();
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
            boolean isNull = scratch.isNull();
            long inHh = scratch.getHh();
            long inHl = scratch.getHl();
            long inLh = scratch.getLh();
            long inLl = scratch.getLl();
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            readD256(memory, startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                            acc.subtract(scratch);
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

            if (!isNull) {
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
                long slotOffset = startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE;
                memory.putLong(slotOffset, timestamp);
                memory.putLong(slotOffset + Long.BYTES, inHh);
                memory.putLong(slotOffset + Long.BYTES + Long.BYTES, inHl);
                memory.putLong(slotOffset + Long.BYTES + 2 * Long.BYTES, inLh);
                memory.putLong(slotOffset + Long.BYTES + 3 * Long.BYTES, inLl);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        readD256(memory, startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                        }
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        readD256(memory, startOffset + idx * RECORD_SIZE + Long.BYTES, scratch);
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
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
            if (frameSize != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, frameSize, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
        public void reopen() {
            value.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0);
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
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
            value.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
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
            long inHh = scratch.getHh();
            long inHl = scratch.getHl();
            long inLh = scratch.getLh();
            long inLl = scratch.getLl();
            if (frameIncludesCurrentValue) {
                if (!scratch.isNull()) {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                    count++;
                }
            } else {
                long hiOff = frameLoBounded
                        ? (long) ((loIdx + frameSize - 1) % bufferSize) * Decimal256.BYTES
                        : (long) (loIdx % bufferSize) * Decimal256.BYTES;
                readD256(buffer, hiOff, scratch);
                if (!scratch.isNull()) {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                    count++;
                }
            }
            if (count != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                readD256(buffer, (long) loIdx * Decimal256.BYTES, scratch);
                if (!scratch.isNull()) {
                    acc.subtract(scratch);
                    count--;
                }
            }
            long writeOff = (long) loIdx * Decimal256.BYTES;
            buffer.putLong(writeOff, inHh);
            buffer.putLong(writeOff + Long.BYTES, inHl);
            buffer.putLong(writeOff + 2 * Long.BYTES, inLh);
            buffer.putLong(writeOff + 3 * Long.BYTES, inLl);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
        public void reopen() {
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
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
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                long off = (long) i * Decimal256.BYTES;
                buffer.putLong(off, Decimals.DECIMAL256_HH_NULL);
                buffer.putLong(off + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                buffer.putLong(off + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                buffer.putLong(off + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            }
        }
    }

    static class Decimal256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long count;
            if (mv.isNew()) {
                acc.ofRaw(0);
                count = 0;
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
            }

            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            if (count != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal256AvgOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
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
            count = 0;
            acc.ofRaw(0);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int scale;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal256AvgOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public String getName() {
            return NAME;
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
            if (!scratch.isNull()) {
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
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
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                divScratch.copyRaw(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value.copyRaw(divScratch);
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal32AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private int value;

        Decimal32AvgOverCurrentRowFunction(Function arg, int type) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal32AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;

        public Decimal32AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public String getName() {
            return NAME;
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
            if (i != Decimals.DECIMAL32_NULL) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(0, i);
                    value.putLong(1, 1);
                } else {
                    value.putLong(0, value.getLong(0) + i);
                    value.addLong(1, 1);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            int result;
            if (value == null) {
                result = Decimals.DECIMAL32_NULL;
            } else {
                long count = value.getLong(1);
                if (count <= 0) {
                    result = Decimals.DECIMAL32_NULL;
                } else {
                    divResult.of(value.getLong(0), scale);
                    try {
                        divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    result = (int) divResult.getValue();
                }
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), result);
        }
    }

    public static class Decimal32AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {
        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private int value;

        public Decimal32AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type,
                int position
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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

            long acc;
            long frameSize;
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
                if (i != Decimals.DECIMAL32_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putInt(startOffset + Long.BYTES, i);
                    if (frameIncludesCurrentValue) {
                        acc = i;
                        value = i;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc = 0;
                        value = Decimals.DECIMAL32_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc = 0;
                    value = Decimals.DECIMAL32_NULL;
                    frameSize = 0;
                }
            } else {
                acc = mapValue.getLong(0);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc -= val;
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
                            acc += val;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc += val;
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                    firstIdx = newFirstIdx;
                }

                mapValue.putLong(0, acc);

                if (frameSize != 0) {
                    divResult.of(acc, scale);
                    try {
                        divResult.divide(frameSize, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value = (int) divResult.getValue();
                } else {
                    value = Decimals.DECIMAL32_NULL;
                }
            }

            mapValue.putLong(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
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
        public void reopen() {
            super.reopen();
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
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
        }
    }

    public static class Decimal32AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int scale;
        private final int type;
        private int value;

        public Decimal32AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type,
                int position
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
            frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            MapValue mv = key.createValue();

            long acc;
            long count;
            long loIdx;
            long startOffset;
            int i = arg.getDecimal32(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && i != Decimals.DECIMAL32_NULL) {
                    acc = i;
                    count = 1;
                    value = i;
                } else {
                    acc = 0;
                    value = Decimals.DECIMAL32_NULL;
                    count = 0;
                }
                for (int j = 0; j < bufferSize; j++) {
                    memory.putInt(startOffset + (long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                acc = mv.getLong(0);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                int hiValue = frameIncludesCurrentValue ? i : memory.getInt(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
                if (hiValue != Decimals.DECIMAL32_NULL) {
                    count++;
                    acc += hiValue;
                }
                if (count != 0) {
                    divResult.of(acc, scale);
                    try {
                        divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value = (int) divResult.getValue();
                } else {
                    value = Decimals.DECIMAL32_NULL;
                }
                if (frameLoBounded) {
                    int loValue = memory.getInt(startOffset + loIdx * Integer.BYTES);
                    if (loValue != Decimals.DECIMAL32_NULL) {
                        acc -= loValue;
                        count--;
                    }
                }
            }

            mv.putLong(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putInt(startOffset + loIdx * Integer.BYTES, i);
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
        }
    }

    static class Decimal32AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private long acc = 0L;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private int value;

        public Decimal32AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type,
                int position
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            value = Decimals.DECIMAL32_NULL;
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
                        if (frameSize > 0) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc -= val;
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
                        acc += val;
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        acc += val;
                        frameSize++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }
            if (frameSize != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(frameSize, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (int) divResult.getValue();
            } else {
                value = Decimals.DECIMAL32_NULL;
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
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reopen() {
            acc = 0L;
            value = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            memory.close();
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
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }
    }

    static class Decimal32AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private int loIdx = 0;
        private int value;

        public Decimal32AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL32_NULL;
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
            int i = arg.getDecimal32(record);
            int hiValue = i;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getInt((long) ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getInt((long) (loIdx % bufferSize) * Integer.BYTES);
            }
            if (hiValue != Decimals.DECIMAL32_NULL) {
                acc += hiValue;
                count++;
            }
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (int) divResult.getValue();
            } else {
                value = Decimals.DECIMAL32_NULL;
            }

            if (frameLoBounded) {
                int loValue = buffer.getInt((long) loIdx * Integer.BYTES);
                if (loValue != Decimals.DECIMAL32_NULL) {
                    acc -= loValue;
                    count--;
                }
            }
            buffer.putInt((long) loIdx * Integer.BYTES, i);
            loIdx = (loIdx + 1) % bufferSize;
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
        public void reopen() {
            acc = 0L;
            value = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            buffer.close();
            value = Decimals.DECIMAL32_NULL;
            count = 0;
            loIdx = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over ( rows between ");
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
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
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

    static class Decimal32AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private int value;

        public Decimal32AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long acc;
            long count;
            if (mv.isNew()) {
                acc = 0L;
                count = 0;
            } else {
                acc = mv.getLong(0);
                count = mv.getLong(1);
            }

            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                acc += i;
                count++;
            }
            mv.putLong(0, acc);
            mv.putLong(1, count);
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (int) divResult.getValue();
            } else {
                value = Decimals.DECIMAL32_NULL;
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
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal32AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private int value;

        public Decimal32AvgOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                acc += i;
                count++;
            }
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (int) divResult.getValue();
            } else {
                value = Decimals.DECIMAL32_NULL;
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
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            value = Decimals.DECIMAL32_NULL;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
            count = 0;
        }
    }

    static class Decimal32AvgOverWholeResultSetFunction extends BaseWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private int value;

        public Decimal32AvgOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL32_NULL;
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
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                acc += i;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value = Decimals.DECIMAL32_NULL;
            } else {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (int) divResult.getValue();
            }
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            value = Decimals.DECIMAL32_NULL;
            count = 0;
        }

        @Override
        public void toTop() {
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL32_NULL;
            count = 0;
        }
    }

    static class Decimal64AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private long value;

        Decimal64AvgOverCurrentRowFunction(Function arg, int type) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal64AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal128 decimal128 = new Decimal128();
        private final int position;
        private final int scale;
        private final int type;

        public Decimal64AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public String getName() {
            return NAME;
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
            if (d != Decimals.DECIMAL64_NULL) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    decimal128.ofRaw(d);
                    value.putDecimal128(0, decimal128);
                    value.putLong(1, 1);
                } else {
                    value.getDecimal128(0, decimal128);
                    try {
                        Decimal128.uncheckedAdd(decimal128, d);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value.putDecimal128(0, decimal128);
                    value.addLong(1, 1);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            long result;
            if (value == null) {
                result = Decimals.DECIMAL64_NULL;
            } else {
                long count = value.getLong(1);
                if (count <= 0) {
                    result = Decimals.DECIMAL64_NULL;
                } else {
                    value.getDecimal128(0, decimal128);
                    decimal128.setScale(scale);
                    try {
                        decimal128.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    result = decimal128.getLow();
                }
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), result);
        }
    }

    public static class Decimal64AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {
        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal128 decimal128 = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final Decimal128 result = new Decimal128();
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private long avg;

        public Decimal64AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type,
                int position
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
                if (d != Decimals.DECIMAL64_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    if (frameIncludesCurrentValue) {
                        decimal128.ofRaw(d);
                        mapValue.putDecimal128(0, decimal128);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                        avg = d;
                    } else {
                        decimal128.ofRaw(0);
                        mapValue.putDecimal128(0, decimal128);
                        frameSize = 0;
                        size = 1;
                        avg = Decimals.DECIMAL64_NULL;
                    }
                } else {
                    size = 0;
                    decimal128.ofRaw(0);
                    mapValue.putDecimal128(0, decimal128);
                    frameSize = 0;
                    avg = Decimals.DECIMAL64_NULL;
                }
            } else {
                mapValue.getDecimal128(0, decimal128);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                decimal128.subtract(val < 0 ? -1L : 0L, val, 0);
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
                            try {
                                Decimal128.uncheckedAdd(decimal128, value);
                            } catch (NumericException e) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal128.uncheckedAdd(decimal128, val);
                            } catch (NumericException e) {
                                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
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

                if (frameSize != 0) {
                    result.copyFrom(decimal128);
                    result.setScale(scale);
                    try {
                        result.divide(0, frameSize, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    avg = result.getLow();
                } else {
                    avg = Decimals.DECIMAL64_NULL;
                }
            }

            mapValue.putDecimal128(0, decimal128);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
        }

        @Override
        public long getDecimal64(Record rec) {
            return avg;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            super.reopen();
            avg = Decimals.DECIMAL64_NULL;
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal64AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal128 decimal128 = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final Decimal128 result = new Decimal128();
        private final int scale;
        private final int type;
        private long avg;

        public Decimal64AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type,
                int position
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
            frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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

            long count;
            long loIdx;
            long startOffset;
            long d = arg.getDecimal64(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && d != Decimals.DECIMAL64_NULL) {
                    decimal128.ofRaw(d);
                    count = 1;
                    avg = d;
                } else {
                    decimal128.ofRaw(0);
                    avg = Decimals.DECIMAL64_NULL;
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                value.getDecimal128(0, decimal128);
                count = value.getLong(1);
                loIdx = value.getLong(2);
                startOffset = value.getLong(3);

                long hiValue = frameIncludesCurrentValue ? d : memory.getLong(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
                if (hiValue != Decimals.DECIMAL64_NULL) {
                    count++;
                    try {
                        Decimal128.uncheckedAdd(decimal128, hiValue);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                }

                if (count != 0) {
                    result.copyFrom(decimal128);
                    result.setScale(scale);
                    try {
                        result.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    avg = result.getLow();
                } else {
                    avg = Decimals.DECIMAL64_NULL;
                }

                if (frameLoBounded) {
                    long loValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    if (loValue != Decimals.DECIMAL64_NULL) {
                        decimal128.subtract(loValue < 0 ? -1L : 0L, loValue, 0);
                        count--;
                    }
                }
            }

            value.putDecimal128(0, decimal128);
            value.putLong(1, count);
            value.putLong(2, (loIdx + 1) % bufferSize);
            value.putLong(3, startOffset);
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        @Override
        public long getDecimal64(Record rec) {
            return avg;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
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

    static class Decimal64AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal128 acc = new Decimal128();
        private final Decimal128 divScratch = new Decimal128();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private long avg;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal64AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type,
                int position
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            acc.ofRaw(0);
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
                        if (frameSize > 0) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc.subtract(val < 0 ? -1L : 0L, val, 0);
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
                        try {
                            Decimal128.uncheckedAdd(acc, value);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal128.uncheckedAdd(acc, val);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
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
            if (frameSize != 0) {
                divScratch.copyFrom(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, frameSize, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                avg = divScratch.getLow();
            } else {
                avg = Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return avg;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            avg = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0);
        }

        @Override
        public void reset() {
            acc.ofZero();
            super.reset();
            memory.close();
        }

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

        @Override
        public void toTop() {
            super.toTop();
            avg = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal64AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal128 acc = new Decimal128();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal128 divScratch = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int scale;
        private final int type;
        private long avg = Decimals.DECIMAL64_NULL;
        private long count = 0;
        private int loIdx = 0;

        public Decimal64AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
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

            long hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) (loIdx % bufferSize) * Long.BYTES);
            }
            if (hiValue != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal128.uncheckedAdd(acc, hiValue);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                count++;
            }
            if (count != 0) {
                divScratch.copyFrom(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                avg = divScratch.getLow();
            } else {
                avg = Decimals.DECIMAL64_NULL;
            }

            if (frameLoBounded) {
                long loValue = buffer.getLong((long) loIdx * Long.BYTES);
                if (loValue != Decimals.DECIMAL64_NULL) {
                    acc.subtract(loValue < 0 ? -1L : 0L, loValue, 0);
                    count--;
                }
            }

            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getDecimal64(Record rec) {
            return avg;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
        }

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

        @Override
        public void toTop() {
            super.toTop();
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
        }
    }

    static class Decimal64AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final Decimal128 decimal128 = new Decimal128();
        private final int position;
        private final int scale;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private long avg;

        public Decimal64AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long count;
            if (value.isNew()) {
                decimal128.ofRaw(0);
                count = 0;
            } else {
                value.getDecimal128(0, decimal128);
                count = value.getLong(1);
            }

            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal128.uncheckedAdd(decimal128, d);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                count++;
            }
            value.putDecimal128(0, decimal128);
            value.putLong(1, count);
            if (count != 0) {
                scratch.copyFrom(decimal128);
                scratch.setScale(scale);
                try {
                    scratch.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                avg = scratch.getLow();
            } else {
                avg = Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return avg;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal64AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final Decimal128 divScratch = new Decimal128();
        private final int position;
        private final int scale;
        private final int type;
        private long avg;
        private long count = 0;

        public Decimal64AvgOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal128.uncheckedAdd(acc, d);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                count++;
            }

            if (count != 0) {
                divScratch.copyFrom(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                avg = divScratch.getLow();
            } else {
                avg = Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return avg;
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reset() {
            super.reset();
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            acc.ofRaw(0);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal64AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final Decimal128 divScratch = new Decimal128();
        private final int position;
        private final int scale;
        private final int type;
        private long avg;
        private long count;

        public Decimal64AvgOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            acc.ofRaw(0);
        }

        @Override
        public String getName() {
            return NAME;
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
            if (d != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal128.uncheckedAdd(acc, d);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void preparePass2() {
            if (count > 0) {
                divScratch.copyFrom(acc);
                divScratch.setScale(scale);
                try {
                    divScratch.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                avg = divScratch.getLow();
            } else {
                avg = Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public void reset() {
            super.reset();
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            avg = Decimals.DECIMAL64_NULL;
            count = 0;
            acc.ofRaw(0);
        }
    }

    static class Decimal8AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private byte value;

        Decimal8AvgOverCurrentRowFunction(Function arg, int type) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    static class Decimal8AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;

        public Decimal8AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public String getName() {
            return NAME;
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
            if (b != Decimals.DECIMAL8_NULL) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(0, b);
                    value.putLong(1, 1);
                } else {
                    value.putLong(0, value.getLong(0) + b);
                    value.addLong(1, 1);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            byte result;
            if (value == null) {
                result = Decimals.DECIMAL8_NULL;
            } else {
                long count = value.getLong(1);
                if (count <= 0) {
                    result = Decimals.DECIMAL8_NULL;
                } else {
                    divResult.of(value.getLong(0), scale);
                    try {
                        divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    result = (byte) divResult.getValue();
                }
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), result);
        }
    }

    public static class Decimal8AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {
        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private byte value;

        public Decimal8AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int type,
                int position
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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

            long acc;
            long frameSize;
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
                if (b != Decimals.DECIMAL8_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putByte(startOffset + Long.BYTES, b);
                    if (frameIncludesCurrentValue) {
                        acc = b;
                        value = b;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc = 0;
                        value = Decimals.DECIMAL8_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc = 0;
                    value = Decimals.DECIMAL8_NULL;
                    frameSize = 0;
                }
            } else {
                acc = mapValue.getLong(0);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc -= val;
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
                            byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc += val;
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc += val;
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                    firstIdx = newFirstIdx;
                }

                mapValue.putLong(0, acc);

                if (frameSize != 0) {
                    divResult.of(acc, scale);
                    try {
                        divResult.divide(frameSize, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value = (byte) divResult.getValue();
                } else {
                    value = Decimals.DECIMAL8_NULL;
                }
            }

            mapValue.putLong(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
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
        public void reopen() {
            super.reopen();
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
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
        }
    }

    public static class Decimal8AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final int bufferSize;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int scale;
        private final int type;
        private byte value;

        public Decimal8AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int type,
                int position
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
            frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
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
            MapValue mv = key.createValue();

            long acc;
            long count;
            long loIdx;
            long startOffset;
            byte b = arg.getDecimal8(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && b != Decimals.DECIMAL8_NULL) {
                    acc = b;
                    count = 1;
                    value = b;
                } else {
                    acc = 0;
                    value = Decimals.DECIMAL8_NULL;
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
            } else {
                acc = mv.getLong(0);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                byte hiValue = frameIncludesCurrentValue ? b : memory.getByte(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Byte.BYTES);
                if (hiValue != Decimals.DECIMAL8_NULL) {
                    count++;
                    acc += hiValue;
                }
                if (count != 0) {
                    divResult.of(acc, scale);
                    try {
                        divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    value = (byte) divResult.getValue();
                } else {
                    value = Decimals.DECIMAL8_NULL;
                }
                if (frameLoBounded) {
                    byte loValue = memory.getByte(startOffset + loIdx * Byte.BYTES);
                    if (loValue != Decimals.DECIMAL8_NULL) {
                        acc -= loValue;
                        count--;
                    }
                }
            }

            mv.putLong(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putByte(startOffset + loIdx * Byte.BYTES, b);
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
        }
    }

    static class Decimal8AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {
        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int scale;
        private final int timestampIndex;
        private final int type;
        private long acc = 0L;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private byte value;

        public Decimal8AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type,
                int position
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
            this.timestampIndex = timestampIdx;
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            value = Decimals.DECIMAL8_NULL;
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
                        if (frameSize > 0) {
                            byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc -= val;
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
                        byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        acc += val;
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        byte val = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        acc += val;
                        frameSize++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }
            if (frameSize != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(frameSize, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (byte) divResult.getValue();
            } else {
                value = Decimals.DECIMAL8_NULL;
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
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reopen() {
            acc = 0L;
            value = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            memory.close();
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
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }
    }

    static class Decimal8AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal64 divResult = new Decimal64();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private int loIdx = 0;
        private byte value;

        public Decimal8AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL8_NULL;
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
            byte b = arg.getDecimal8(record);
            byte hiValue = b;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getByte((long) ((loIdx + frameSize - 1) % bufferSize) * Byte.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getByte((long) (loIdx % bufferSize) * Byte.BYTES);
            }
            if (hiValue != Decimals.DECIMAL8_NULL) {
                acc += hiValue;
                count++;
            }
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (byte) divResult.getValue();
            } else {
                value = Decimals.DECIMAL8_NULL;
            }

            if (frameLoBounded) {
                byte loValue = buffer.getByte((long) loIdx * Byte.BYTES);
                if (loValue != Decimals.DECIMAL8_NULL) {
                    acc -= loValue;
                    count--;
                }
            }
            buffer.putByte((long) loIdx * Byte.BYTES, b);
            loIdx = (loIdx + 1) % bufferSize;
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
        public void reopen() {
            acc = 0L;
            value = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            buffer.close();
            value = Decimals.DECIMAL8_NULL;
            count = 0;
            loIdx = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(')');
            sink.val(" over ( rows between ");
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
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
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

    static class Decimal8AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private byte value;

        public Decimal8AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long acc;
            long count;
            if (mv.isNew()) {
                acc = 0L;
                count = 0;
            } else {
                acc = mv.getLong(0);
                count = mv.getLong(1);
            }

            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                acc += b;
                count++;
            }
            mv.putLong(0, acc);
            mv.putLong(1, count);
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (byte) divResult.getValue();
            } else {
                value = Decimals.DECIMAL8_NULL;
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
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal8AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private byte value;

        public Decimal8AvgOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                acc += b;
                count++;
            }
            if (count != 0) {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (byte) divResult.getValue();
            } else {
                value = Decimals.DECIMAL8_NULL;
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
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            acc = 0L;
            super.reset();
            value = Decimals.DECIMAL8_NULL;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            acc = 0L;
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
            count = 0;
        }
    }

    static class Decimal8AvgOverWholeResultSetFunction extends BaseWindowFunction {
        private final Decimal64 divResult = new Decimal64();
        private final int position;
        private final int scale;
        private final int type;
        private long acc = 0L;
        private long count = 0;
        private byte value;

        public Decimal8AvgOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.scale = ColumnType.getDecimalScale(type);
            this.position = position;
            value = Decimals.DECIMAL8_NULL;
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
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                acc += b;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value = Decimals.DECIMAL8_NULL;
            } else {
                divResult.of(acc, scale);
                try {
                    divResult.divide(count, 0, scale, RoundingMode.HALF_EVEN);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                value = (byte) divResult.getValue();
            }
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL8_NULL;
            count = 0;
            acc = 0L;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL8_NULL;
            count = 0;
            acc = 0L;
        }
    }

    static {
        AVG_DECIMAL64_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL64_TYPES.add(ColumnType.DECIMAL128); // slot 0: acc
        AVG_DECIMAL64_TYPES.add(ColumnType.LONG);       // slot 1: count

        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL128); // slot 0: acc
        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 1: frameSize
        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 2: startOffset
        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 3: size
        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 4: capacity
        AVG_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 5: firstIdx

        AVG_DECIMAL128_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL128_TYPES.add(ColumnType.DECIMAL256); // slot 0: acc
        AVG_DECIMAL128_TYPES.add(ColumnType.LONG);       // slot 1: count

        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL256); // slot 0: acc
        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 1: frameSize
        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 2: startOffset
        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 3: size
        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 4: capacity
        AVG_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);       // slot 5: firstIdx

        AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL256); // slot 0: acc
        AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);       // slot 1: count
        AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);       // slot 2: loIdx
        AVG_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);       // slot 3: startOffset

        AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL128); // slot 0: acc
        AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);       // slot 1: count
        AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);       // slot 2: loIdx
        AVG_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);       // slot 3: startOffset

        AVG_DECIMAL_NARROW_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL_NARROW_TYPES.add(ColumnType.LONG); // acc (raw long sum)
        AVG_DECIMAL_NARROW_TYPES.add(ColumnType.LONG); // count

        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG); // acc
        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG); // frameSize
        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG); // startOffset
        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG); // size
        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG); // capacity
        AVG_DECIMAL_NARROW_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG); // firstIdx

        AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG); // acc
        AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG); // count
        AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG); // loIdx
        AVG_DECIMAL_NARROW_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG); // startOffset
    }
}
