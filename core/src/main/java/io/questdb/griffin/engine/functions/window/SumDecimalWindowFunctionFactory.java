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
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

public class SumDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String NAME = "sum";
    private static final String SIGNATURE = NAME + "(Ξ)";
    private static final ArrayColumnTypes SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL128_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL16_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL64_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES;
    private static final ArrayColumnTypes SUM_DECIMAL8_TYPES;

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
        int argScale = ColumnType.getDecimalScale(argType);
        // sum widens: D8/D16 -> D64, D32/D64 -> D128, D128/D256 -> D256
        int outputType = switch (tag) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16 ->
                    ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL64), argScale);
            case ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128), argScale);
            case ColumnType.DECIMAL128, ColumnType.DECIMAL256 ->
                    ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL256), argScale);
            default -> throw SqlException.$(argPos, "sum is not yet implemented for ").put(ColumnType.nameOf(tag));
        };

        if (rowsHi < rowsLo) {
            boolean isRange = framingMode == WindowExpression.FRAMING_RANGE;
            return switch (tag) {
                case ColumnType.DECIMAL8, ColumnType.DECIMAL16 ->
                        new Decimal64NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, outputType);
                case ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                        new Decimal128NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, outputType);
                default ->
                        new Decimal256NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, outputType);
            };
        }

        return switch (tag) {
            case ColumnType.DECIMAL8 ->
                    newInstanceDecimal8(position, args, configuration, sqlExecutionContext, outputType);
            case ColumnType.DECIMAL16 ->
                    newInstanceDecimal16(position, args, configuration, sqlExecutionContext, outputType);
            case ColumnType.DECIMAL32 ->
                    newInstanceDecimal32(position, args, configuration, sqlExecutionContext, outputType, argPos);
            case ColumnType.DECIMAL64 ->
                    newInstanceDecimal64(position, args, configuration, sqlExecutionContext, outputType, argPos);
            case ColumnType.DECIMAL128 ->
                    newInstanceDecimal128(position, args, configuration, sqlExecutionContext, outputType, argPos);
            case ColumnType.DECIMAL256 ->
                    newInstanceDecimal256(position, args, configuration, sqlExecutionContext, argType, argPos);
            default -> throw SqlException.$(argPos, "sum is not yet implemented for ").put(ColumnType.nameOf(tag));
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

    private static void writeD256(MemoryARW mem, long offset, Decimal256 src) {
        mem.putLong(offset, src.getHh());
        mem.putLong(offset + Long.BYTES, src.getHl());
        mem.putLong(offset + 2 * Long.BYTES, src.getLh());
        mem.putLong(offset + 3 * Long.BYTES, src.getLl());
    }

    private Function newInstanceDecimal128(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            int outputType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal128SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal128SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
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
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128SumOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal128SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128SumOverCurrentRowFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal128SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128SumOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, outputType, argPos);
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
                    return new Decimal128SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128SumOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, outputType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128SumOverCurrentRowFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal128SumOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, outputType, argPos);
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
            int outputType
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL16_TYPES);
                    try {
                        return new Decimal16SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL16_TYPES);
                    try {
                        return new Decimal16SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType);
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
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16SumOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL16_TYPES);
                    try {
                        return new Decimal16SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL16_TYPES);
                    try {
                        return new Decimal16SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16SumOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, outputType);
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
                    return new Decimal16SumOverWholeResultSetFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16SumOverUnboundedRowsFrameFunction(arg, outputType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16SumOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, outputType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16SumOverUnboundedRowsFrameFunction(arg, outputType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16SumOverWholeResultSetFunction(arg, outputType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal16SumOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, outputType);
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
            int outputType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal256SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal256SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
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
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256SumOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal256SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256SumOverCurrentRowFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_TYPES);
                    try {
                        return new Decimal256SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256SumOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, outputType, argPos);
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
                    return new Decimal256SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256SumOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, outputType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256SumOverCurrentRowFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal256SumOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, outputType, argPos);
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
            int outputType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal32SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal32SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
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
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32SumOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal32SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal32SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32SumOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, outputType, argPos);
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
                    return new Decimal32SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32SumOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, outputType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal32SumOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, outputType, argPos);
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
            int outputType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal64SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal64SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
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
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64SumOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal64SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_TYPES);
                    try {
                        return new Decimal64SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64SumOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, outputType, argPos);
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
                    return new Decimal64SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64SumOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, outputType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64SumOverUnboundedRowsFrameFunction(arg, outputType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64SumOverWholeResultSetFunction(arg, outputType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal64SumOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, outputType, argPos);
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
            int outputType
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL8_TYPES);
                    try {
                        return new Decimal8SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL8_TYPES);
                    try {
                        return new Decimal8SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType);
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
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8SumOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL8_TYPES);
                    try {
                        return new Decimal8SumOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL8_TYPES);
                    try {
                        return new Decimal8SumOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, outputType);
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8SumOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, outputType);
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
                    return new Decimal8SumOverWholeResultSetFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8SumOverUnboundedRowsFrameFunction(arg, outputType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8SumOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, outputType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8SumOverUnboundedRowsFrameFunction(arg, outputType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8SumOverCurrentRowFunction(arg, outputType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8SumOverWholeResultSetFunction(arg, outputType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal8SumOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, outputType);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal128SumOverCurrentRowFunction extends BaseWindowFunction {

        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal256 value = new Decimal256();

        Decimal128SumOverCurrentRowFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (scratch.isNull()) {
                value.ofRawNull();
            } else {
                value.ofRaw(scratch.getHigh(), scratch.getLow());
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal128SumOverPartitionFunction extends BasePartitionedWindowFunction {
        private final Decimal256 acc = new Decimal256();
        private final Decimal256 nullVal = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                if (mv.isNew()) {
                    acc.ofRaw(scratch.getHigh(), scratch.getLow());
                    mv.putDecimal256(0, acc);
                    mv.putBool(1, false);
                } else if (mv.getBool(1)) {
                    acc.ofRaw(scratch.getHigh(), scratch.getLow());
                    mv.putDecimal256(0, acc);
                    mv.putBool(1, false);
                } else {
                    mv.getDecimal256(0, acc);
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                    mv.putDecimal256(0, acc);
                }
            } else if (mv.isNew()) {
                nullVal.ofRawNull();
                mv.putDecimal256(0, nullVal);
                mv.putBool(1, true);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null && !mv.getBool(1)) {
                mv.getDecimal256(0, acc);
                Unsafe.putLong(addr, acc.getHh());
                Unsafe.putLong(addr + Long.BYTES, acc.getHl());
                Unsafe.putLong(addr + 2 * Long.BYTES, acc.getLh());
                Unsafe.putLong(addr + 3 * Long.BYTES, acc.getLl());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            }
        }
    }

    public static class Decimal128SumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal256 acc = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal128SumOverPartitionRangeFrameFunction(
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
            boolean inputNull = scratch.isNull();
            long inHigh = scratch.getHigh();
            long inLow = scratch.getLow();

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!inputNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, inHigh);
                    memory.putLong(startOffset + Long.BYTES + Long.BYTES, inLow);
                    if (frameIncludesCurrentValue) {
                        long signExt = inHigh < 0 ? -1L : 0L;
                        acc.ofRaw(signExt, signExt, inHigh, inLow);
                        value.copyRaw(acc);
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

                if (!inputNull) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }
                    long slotOffset = startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE;
                    memory.putLong(slotOffset, timestamp);
                    memory.putLong(slotOffset + Long.BYTES, inHigh);
                    memory.putLong(slotOffset + Long.BYTES + Long.BYTES, inLow);
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
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                    value.copyRaw(acc);
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
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
            super.reopen();
            value.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
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

    public static class Decimal128SumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal128SumOverPartitionRowsFrameFunction(
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
                    value.copyRaw(acc);
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

                long hiOffset = frameIncludesCurrentValue
                        ? -1
                        : startOffset + ((loIdx + frameSize - 1) % bufferSize) * Decimal128.BYTES;
                long hiH;
                long hiL;
                if (frameIncludesCurrentValue) {
                    hiH = inHigh;
                    hiL = inLow;
                } else {
                    hiH = memory.getLong(hiOffset);
                    hiL = memory.getLong(hiOffset + Long.BYTES);
                }
                if (!Decimal128.isNull(hiH, hiL)) {
                    count++;
                    scratch.ofRaw(hiH, hiL);
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
                if (count != 0) {
                    value.copyRaw(acc);
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
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal128SumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal256 acc = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal128SumOverRangeFrameFunction(
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
            try {
                this.frameLoBounded = rangeLo != Long.MIN_VALUE;
                this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
                this.minDiff = Math.abs(rangeHi);
                this.timestampIndex = timestampIdx;
                this.type = type;
                this.position = position;
                capacity = initialCapacity;
                // memory allocates lazily on reopen(), under the tracker bound by the cursor
                firstIdx = 0;
                frameSize = 0;
                acc.ofRaw(0);
                value.ofRawNull();
            } catch (Throwable th) {
                memory.close();
                throw th;
            }
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
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                value.copyRaw(acc);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
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

    static class Decimal128SumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal128SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
                    throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                value.copyRaw(acc);
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
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                buffer.putLong((long) i * Decimal128.BYTES, Decimals.DECIMAL128_HI_NULL);
                buffer.putLong((long) i * Decimal128.BYTES + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    static class Decimal128SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal128SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            boolean wasNullState;
            if (mv.isNew()) {
                acc.ofRaw(0);
                wasNullState = true;
            } else {
                mv.getDecimal256(0, acc);
                wasNullState = mv.getBool(1);
            }

            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                if (wasNullState) {
                    acc.ofRaw(scratch.getHigh(), scratch.getLow());
                    wasNullState = false;
                } else {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            mv.putDecimal256(0, acc);
            mv.putBool(1, wasNullState);
            if (wasNullState) {
                value.ofRawNull();
            } else {
                value.copyRaw(acc);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal128SumOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private boolean isNullState = true;

        public Decimal128SumOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (!scratch.isNull()) {
                if (isNullState) {
                    acc.ofRaw(scratch.getHigh(), scratch.getLow());
                    isNullState = false;
                } else {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyRaw(acc);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
            isNullState = true;
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
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal128SumOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private boolean isNullState = true;

        public Decimal128SumOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
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
                if (isNullState) {
                    acc.ofRaw(scratch.getHigh(), scratch.getLow());
                    isNullState = false;
                } else {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
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
        public void preparePass2() {
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyRaw(acc);
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal16SumOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private long value;

        Decimal16SumOverCurrentRowFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            value = s == Decimals.DECIMAL16_NULL ? Decimals.DECIMAL64_NULL : s;
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

    static class Decimal16SumOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal16SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                if (mv.isNew()) {
                    mv.putLong(0, s);
                    mv.putBool(1, false);
                } else if (mv.getBool(1)) {
                    mv.putLong(0, s);
                    mv.putBool(1, false);
                } else {
                    mv.putLong(0, mv.getLong(0) + s);
                }
            } else if (mv.isNew()) {
                mv.putLong(0, 0L);
                mv.putBool(1, true);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null && !mv.getBool(1)) {
                Unsafe.putLong(addr, mv.getLong(0));
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
            }
        }
    }

    public static class Decimal16SumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int timestampIndex;
        private final int type;
        private long value;

        public Decimal16SumOverPartitionRangeFrameFunction(
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
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
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
                        value = acc;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc = 0L;
                        value = Decimals.DECIMAL64_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc = 0L;
                    value = Decimals.DECIMAL64_NULL;
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

                if (frameSize != 0) {
                    value = acc;
                } else {
                    value = Decimals.DECIMAL64_NULL;
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
        public void reopen() {
            super.reopen();
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
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

    public static class Decimal16SumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int type;
        private long value;

        public Decimal16SumOverPartitionRowsFrameFunction(
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
            frameIncludesCurrentValue = rowsHi == 0;
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
            MapValue mv = key.createValue();

            long count;
            long loIdx;
            long startOffset;
            long acc;
            short s = arg.getDecimal16(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && s != Decimals.DECIMAL16_NULL) {
                    acc = s;
                    count = 1;
                    value = acc;
                } else {
                    acc = 0L;
                    value = Decimals.DECIMAL64_NULL;
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
                    value = acc;
                } else {
                    value = Decimals.DECIMAL64_NULL;
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

    static class Decimal16SumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        private final int type;
        private long acc;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private long value;

        public Decimal16SumOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                this.frameLoBounded = rangeLo != Long.MIN_VALUE;
                this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
                this.minDiff = Math.abs(rangeHi);
                this.timestampIndex = timestampIdx;
                this.type = type;
                capacity = initialCapacity;
                // memory allocates lazily on reopen(), under the tracker bound by the cursor
                firstIdx = 0;
                frameSize = 0;
                acc = 0L;
                value = Decimals.DECIMAL64_NULL;
            } catch (Throwable th) {
                memory.close();
                throw th;
            }
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
            value = frameSize != 0 ? acc : Decimals.DECIMAL64_NULL;
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
        public void reopen() {
            value = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc = 0L;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
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
            value = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc = 0L;
        }
    }

    static class Decimal16SumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int type;
        private long acc;
        private long count = 0;
        private int loIdx = 0;
        private long value;

        public Decimal16SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            acc = 0L;
            value = Decimals.DECIMAL64_NULL;
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
            value = count != 0 ? acc : Decimals.DECIMAL64_NULL;

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
        public void reopen() {
            value = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc = 0L;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            value = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc = 0L;
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
            value = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc = 0L;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
        }
    }

    static class Decimal16SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int type;
        private long value;

        public Decimal16SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long acc;
            boolean wasNullState;
            if (mv.isNew()) {
                acc = 0L;
                wasNullState = true;
            } else {
                acc = mv.getLong(0);
                wasNullState = mv.getBool(1);
            }

            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                if (wasNullState) {
                    acc = s;
                    wasNullState = false;
                } else {
                    acc += s;
                }
            }
            mv.putLong(0, acc);
            mv.putBool(1, wasNullState);
            value = wasNullState ? Decimals.DECIMAL64_NULL : acc;
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
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal16SumOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final int type;
        private long acc;
        private boolean isNullState = true;
        private long value;

        public Decimal16SumOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            acc = 0L;
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                if (isNullState) {
                    acc = s;
                    isNullState = false;
                } else {
                    acc += s;
                }
            }
            value = isNullState ? Decimals.DECIMAL64_NULL : acc;
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
            isNullState = true;
            acc = 0L;
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
            value = Decimals.DECIMAL64_NULL;
            isNullState = true;
            acc = 0L;
        }
    }

    static class Decimal16SumOverWholeResultSetFunction extends BaseWindowFunction {

        private final int type;
        private long acc;
        private boolean isNullState = true;
        private long value;

        public Decimal16SumOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            acc = 0L;
            value = Decimals.DECIMAL64_NULL;
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
                if (isNullState) {
                    acc = s;
                    isNullState = false;
                } else {
                    acc += s;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void preparePass2() {
            value = isNullState ? Decimals.DECIMAL64_NULL : acc;
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL64_NULL;
            isNullState = true;
            acc = 0L;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
            isNullState = true;
            acc = 0L;
        }
    }

    static class Decimal256SumOverCurrentRowFunction extends BaseWindowFunction {

        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        Decimal256SumOverCurrentRowFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (scratch.isNull()) {
                value.ofRawNull();
            } else {
                value.copyRaw(scratch);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal256SumOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                if (mv.isNew()) {
                    acc.copyRaw(scratch);
                    mv.putDecimal256(0, acc);
                    mv.putBool(1, false);
                } else if (mv.getBool(1)) {
                    acc.copyRaw(scratch);
                    mv.putDecimal256(0, acc);
                    mv.putBool(1, false);
                } else {
                    mv.getDecimal256(0, acc);
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                    mv.putDecimal256(0, acc);
                }
            } else if (mv.isNew()) {
                acc.ofRawNull();
                mv.putDecimal256(0, acc);
                mv.putBool(1, true);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null && !mv.getBool(1)) {
                mv.getDecimal256(0, acc);
                Unsafe.putLong(addr, acc.getHh());
                Unsafe.putLong(addr + Long.BYTES, acc.getHl());
                Unsafe.putLong(addr + 2 * Long.BYTES, acc.getLh());
                Unsafe.putLong(addr + 3 * Long.BYTES, acc.getLl());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            }
        }
    }

    public static class Decimal256SumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256 acc = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final Decimal256 input = new Decimal256();
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256SumOverPartitionRangeFrameFunction(
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
            arg.getDecimal256(record, input);
            boolean isNull = input.isNull();

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    writeD256(memory, startOffset + Long.BYTES, input);
                    if (frameIncludesCurrentValue) {
                        acc.copyRaw(input);
                        value.copyRaw(acc);
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
                    writeD256(memory, slotOffset + Long.BYTES, input);
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
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                            }
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                    value.copyRaw(acc);
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
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
            super.reopen();
            value.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
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

    public static class Decimal256SumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256SumOverPartitionRowsFrameFunction(
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
                    value.copyRaw(acc);
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
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                        }
                        count++;
                    }
                } else {
                    readD256(memory, startOffset + ((loIdx + frameSize - 1) % bufferSize) * Decimal256.BYTES, scratch);
                    if (!scratch.isNull()) {
                        try {
                            Decimal256.uncheckedAdd(acc, scratch);
                        } catch (NumericException e) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                        }
                        count++;
                    }
                }
                if (count != 0) {
                    value.copyRaw(acc);
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
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal256SumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256 acc = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int timestampIndex;
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal256SumOverRangeFrameFunction(
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
            try {
                this.frameLoBounded = rangeLo != Long.MIN_VALUE;
                this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
                this.minDiff = Math.abs(rangeHi);
                this.timestampIndex = timestampIdx;
                this.type = type;
                this.position = position;
                capacity = initialCapacity;
                // memory allocates lazily on reopen(), under the tracker bound by the cursor
                firstIdx = 0;
                frameSize = 0;
                acc.ofRaw(0);
                value.ofRawNull();
            } catch (Throwable th) {
                memory.close();
                throw th;
            }
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
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                        }
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                value.copyRaw(acc);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
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

    static class Decimal256SumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal256SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                    count++;
                }
            }
            if (count != 0) {
                value.copyRaw(acc);
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
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal256SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            boolean wasNullState;
            if (mv.isNew()) {
                acc.ofRaw(0);
                wasNullState = true;
            } else {
                mv.getDecimal256(0, acc);
                wasNullState = mv.getBool(1);
            }

            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                if (wasNullState) {
                    acc.copyRaw(scratch);
                    wasNullState = false;
                } else {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            mv.putDecimal256(0, acc);
            mv.putBool(1, wasNullState);
            if (wasNullState) {
                value.ofRawNull();
            } else {
                value.copyRaw(acc);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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

    static class Decimal256SumOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private boolean isNullState = true;

        public Decimal256SumOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (!scratch.isNull()) {
                if (isNullState) {
                    acc.copyRaw(scratch);
                    isNullState = false;
                } else {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyRaw(acc);
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
            isNullState = true;
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
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal256SumOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int type;
        private final Decimal256 value = new Decimal256();
        private boolean isNullState = true;

        public Decimal256SumOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
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
                if (isNullState) {
                    acc.copyRaw(scratch);
                    isNullState = false;
                } else {
                    try {
                        Decimal256.uncheckedAdd(acc, scratch);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
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
        public void preparePass2() {
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyRaw(acc);
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal32SumOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private final Decimal128 value = new Decimal128();

        Decimal32SumOverCurrentRowFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            if (i == Decimals.DECIMAL32_NULL) {
                value.ofRawNull();
            } else {
                value.ofRaw(i);
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

    static class Decimal32SumOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;

        public Decimal32SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                if (mv.isNew()) {
                    acc.ofRaw(i);
                    mv.putDecimal128(0, acc);
                    mv.putBool(1, false);
                } else if (mv.getBool(1)) {
                    acc.ofRaw(i);
                    mv.putDecimal128(0, acc);
                    mv.putBool(1, false);
                } else {
                    mv.getDecimal128(0, acc);
                    Decimal128.uncheckedAdd(acc, i);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                    mv.putDecimal128(0, acc);
                }
            } else if (mv.isNew()) {
                mv.putDecimal128Null(0);
                mv.putBool(1, true);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null && !mv.getBool(1)) {
                mv.getDecimal128(0, acc);
                Unsafe.putLong(addr, acc.getHigh());
                Unsafe.putLong(addr + Long.BYTES, acc.getLow());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    public static class Decimal32SumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal128 acc = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int timestampIndex;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal32SumOverPartitionRangeFrameFunction(
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
            int d = arg.getDecimal32(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (d != Decimals.DECIMAL32_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putInt(startOffset + Long.BYTES, d);
                    if (frameIncludesCurrentValue) {
                        acc.ofRaw(d);
                        value.copyFrom(acc);
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
                mapValue.getDecimal128(0, acc);
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
                                int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
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

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            Decimal128.uncheckedAdd(acc, val);
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            Decimal128.uncheckedAdd(acc, val);
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                    value.copyFrom(acc);
                } else {
                    value.ofRawNull();
                }
            }

            mapValue.putDecimal128(0, acc);
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
            super.reopen();
            value.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
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

    public static class Decimal32SumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal32SumOverPartitionRowsFrameFunction(
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
            int d = arg.getDecimal32(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && d != Decimals.DECIMAL32_NULL) {
                    acc.ofRaw(d);
                    count = 1;
                    value.copyFrom(acc);
                } else {
                    acc.ofRaw(0);
                    value.ofRawNull();
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putInt(startOffset + (long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                mv.getDecimal128(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                int hiValue = frameIncludesCurrentValue ? d : memory.getInt(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
                if (hiValue != Decimals.DECIMAL32_NULL) {
                    count++;
                    Decimal128.uncheckedAdd(acc, hiValue);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
                if (count != 0) {
                    value.copyFrom(acc);
                } else {
                    value.ofRawNull();
                }
                if (frameLoBounded) {
                    int loValue = memory.getInt(startOffset + loIdx * Integer.BYTES);
                    if (loValue != Decimals.DECIMAL32_NULL) {
                        acc.subtract(loValue < 0 ? -1L : 0L, loValue, 0);
                        count--;
                    }
                }
            }

            mv.putDecimal128(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putInt(startOffset + loIdx * Integer.BYTES, d);
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

    static class Decimal32SumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal128 acc = new Decimal128();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int timestampIndex;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal32SumOverRangeFrameFunction(
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
            try {
                this.frameLoBounded = rangeLo != Long.MIN_VALUE;
                this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
                this.minDiff = Math.abs(rangeHi);
                this.timestampIndex = timestampIdx;
                this.type = type;
                this.position = position;
                capacity = initialCapacity;
                // memory allocates lazily on reopen(), under the tracker bound by the cursor
                firstIdx = 0;
                frameSize = 0;
                acc.ofRaw(0);
                value.ofRawNull();
            } catch (Throwable th) {
                memory.close();
                throw th;
            }
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
                        if (frameSize > 0) {
                            int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
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

            if (d != Decimals.DECIMAL32_NULL) {
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
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        Decimal128.uncheckedAdd(acc, val);
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                        int val = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        Decimal128.uncheckedAdd(acc, val);
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                value.copyFrom(acc);
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
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
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

    static class Decimal32SumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal128 acc = new Decimal128();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long count = 0;
        private int loIdx = 0;

        public Decimal32SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            int d = arg.getDecimal32(record);
            int hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getInt((long) ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getInt((long) (loIdx % bufferSize) * Integer.BYTES);
            }
            if (hiValue != Decimals.DECIMAL32_NULL) {
                Decimal128.uncheckedAdd(acc, hiValue);
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                value.copyFrom(acc);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                int loValue = buffer.getInt((long) loIdx * Integer.BYTES);
                if (loValue != Decimals.DECIMAL32_NULL) {
                    acc.subtract(loValue < 0 ? -1L : 0L, loValue, 0);
                    count--;
                }
            }
            buffer.putInt((long) loIdx * Integer.BYTES, d);
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
                buffer.putInt((long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
            }
        }
    }

    static class Decimal32SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal32SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            boolean wasNullState;
            if (mv.isNew()) {
                acc.ofRaw(0);
                wasNullState = true;
            } else {
                mv.getDecimal128(0, acc);
                wasNullState = mv.getBool(1);
            }

            int d = arg.getDecimal32(record);
            if (d != Decimals.DECIMAL32_NULL) {
                if (wasNullState) {
                    acc.ofRaw(d);
                    wasNullState = false;
                } else {
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            mv.putDecimal128(0, acc);
            mv.putBool(1, wasNullState);
            if (wasNullState) {
                value.ofRawNull();
            } else {
                value.copyFrom(acc);
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

    static class Decimal32SumOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private boolean isNullState = true;

        public Decimal32SumOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            int d = arg.getDecimal32(record);
            if (d != Decimals.DECIMAL32_NULL) {
                if (isNullState) {
                    acc.ofRaw(d);
                    isNullState = false;
                } else {
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyFrom(acc);
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
            isNullState = true;
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
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal32SumOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private boolean isNullState = true;

        public Decimal32SumOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
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
            int d = arg.getDecimal32(record);
            if (d != Decimals.DECIMAL32_NULL) {
                if (isNullState) {
                    acc.ofRaw(d);
                    isNullState = false;
                } else {
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
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
        public void preparePass2() {
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyFrom(acc);
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal64SumOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private final Decimal128 value = new Decimal128();

        Decimal64SumOverCurrentRowFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d == Decimals.DECIMAL64_NULL) {
                value.ofRawNull();
            } else {
                value.ofRaw(d);
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

    static class Decimal64SumOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;

        public Decimal64SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                if (mv.isNew()) {
                    acc.ofRaw(d);
                    mv.putDecimal128(0, acc);
                    mv.putBool(1, false);
                } else if (mv.getBool(1)) {
                    acc.ofRaw(d);
                    mv.putDecimal128(0, acc);
                    mv.putBool(1, false);
                } else {
                    mv.getDecimal128(0, acc);
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                    mv.putDecimal128(0, acc);
                }
            } else if (mv.isNew()) {
                mv.putDecimal128Null(0);
                mv.putBool(1, true);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null && !mv.getBool(1)) {
                mv.getDecimal128(0, acc);
                Unsafe.putLong(addr, acc.getHigh());
                Unsafe.putLong(addr + Long.BYTES, acc.getLow());
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    public static class Decimal64SumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal128 acc = new Decimal128();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int timestampIndex;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal64SumOverPartitionRangeFrameFunction(
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
                        acc.ofRaw(d);
                        value.copyFrom(acc);
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
                mapValue.getDecimal128(0, acc);
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
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            Decimal128.uncheckedAdd(acc, val);
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                            Decimal128.uncheckedAdd(acc, val);
                            if (acc.hasOverflowed()) {
                                throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                    value.copyFrom(acc);
                } else {
                    value.ofRawNull();
                }
            }

            mapValue.putDecimal128(0, acc);
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
            super.reopen();
            value.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
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

    public static class Decimal64SumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal64SumOverPartitionRowsFrameFunction(
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
            long d = arg.getDecimal64(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && d != Decimals.DECIMAL64_NULL) {
                    acc.ofRaw(d);
                    count = 1;
                    value.copyFrom(acc);
                } else {
                    acc.ofRaw(0);
                    value.ofRawNull();
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                mv.getDecimal128(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                long hiValue = frameIncludesCurrentValue ? d : memory.getLong(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
                if (hiValue != Decimals.DECIMAL64_NULL) {
                    count++;
                    Decimal128.uncheckedAdd(acc, hiValue);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
                if (count != 0) {
                    value.copyFrom(acc);
                } else {
                    value.ofRawNull();
                }
                if (frameLoBounded) {
                    long loValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    if (loValue != Decimals.DECIMAL64_NULL) {
                        acc.subtract(loValue < 0 ? -1L : 0L, loValue, 0);
                        count--;
                    }
                }
            }

            mv.putDecimal128(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
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

    static class Decimal64SumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal128 acc = new Decimal128();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int timestampIndex;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal64SumOverRangeFrameFunction(
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
            try {
                this.frameLoBounded = rangeLo != Long.MIN_VALUE;
                this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
                this.minDiff = Math.abs(rangeHi);
                this.timestampIndex = timestampIdx;
                this.type = type;
                this.position = position;
                capacity = initialCapacity;
                // memory allocates lazily on reopen(), under the tracker bound by the cursor
                firstIdx = 0;
                frameSize = 0;
                acc.ofRaw(0);
                value.ofRawNull();
            } catch (Throwable th) {
                memory.close();
                throw th;
            }
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
                        long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        Decimal128.uncheckedAdd(acc, val);
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                        Decimal128.uncheckedAdd(acc, val);
                        if (acc.hasOverflowed()) {
                            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
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
                value.copyFrom(acc);
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
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
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

    static class Decimal64SumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal128 acc = new Decimal128();
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private long count = 0;
        private int loIdx = 0;

        public Decimal64SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type, int position) {
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
            long d = arg.getDecimal64(record);
            long hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getLong((long) (loIdx % bufferSize) * Long.BYTES);
            }
            if (hiValue != Decimals.DECIMAL64_NULL) {
                Decimal128.uncheckedAdd(acc, hiValue);
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                value.copyFrom(acc);
            } else {
                value.ofRawNull();
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
            value.ofRawNull();
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

    static class Decimal64SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal64SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            boolean wasNullState;
            if (mv.isNew()) {
                acc.ofRaw(0);
                wasNullState = true;
            } else {
                mv.getDecimal128(0, acc);
                wasNullState = mv.getBool(1);
            }

            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                if (wasNullState) {
                    acc.ofRaw(d);
                    wasNullState = false;
                } else {
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            mv.putDecimal128(0, acc);
            mv.putBool(1, wasNullState);
            if (wasNullState) {
                value.ofRawNull();
            } else {
                value.copyFrom(acc);
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
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal64SumOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private boolean isNullState = true;

        public Decimal64SumOverUnboundedRowsFrameFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                if (isNullState) {
                    acc.ofRaw(d);
                    isNullState = false;
                } else {
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
                }
            }
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyFrom(acc);
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
            isNullState = true;
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
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal64SumOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal128 acc = new Decimal128();
        private final int position;
        private final int type;
        private final Decimal128 value = new Decimal128();
        private boolean isNullState = true;

        public Decimal64SumOverWholeResultSetFunction(Function arg, int type, int position) {
            super(arg);
            this.type = type;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
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
                if (isNullState) {
                    acc.ofRaw(d);
                    isNullState = false;
                } else {
                    Decimal128.uncheckedAdd(acc, d);
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
                    }
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
        public void preparePass2() {
            if (isNullState) {
                value.ofRawNull();
            } else {
                value.copyFrom(acc);
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            isNullState = true;
            acc.ofRaw(0);
        }
    }

    static class Decimal8SumOverCurrentRowFunction extends BaseWindowFunction {

        private final int type;
        private long value;

        Decimal8SumOverCurrentRowFunction(Function arg, int type) {
            super(arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            value = b == Decimals.DECIMAL8_NULL ? Decimals.DECIMAL64_NULL : b;
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

    static class Decimal8SumOverPartitionFunction extends BasePartitionedWindowFunction {

        private final int type;

        public Decimal8SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                if (mv.isNew()) {
                    mv.putLong(0, b);
                    mv.putBool(1, false);
                } else if (mv.getBool(1)) {
                    mv.putLong(0, b);
                    mv.putBool(1, false);
                } else {
                    mv.putLong(0, mv.getLong(0) + b);
                }
            } else if (mv.isNew()) {
                mv.putLong(0, 0L);
                mv.putBool(1, true);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv != null && !mv.getBool(1)) {
                Unsafe.putLong(addr, mv.getLong(0));
            } else {
                Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
            }
        }
    }

    public static class Decimal8SumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int timestampIndex;
        private final int type;
        private long value;

        public Decimal8SumOverPartitionRangeFrameFunction(
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
            this.frameLoBounded = rangeLo != Long.MIN_VALUE;
            this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            this.minDiff = Math.abs(rangeHi);
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
                        value = acc;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc = 0L;
                        value = Decimals.DECIMAL64_NULL;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc = 0L;
                    value = Decimals.DECIMAL64_NULL;
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

                if (frameSize != 0) {
                    value = acc;
                } else {
                    value = Decimals.DECIMAL64_NULL;
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
        public void reopen() {
            super.reopen();
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
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

    public static class Decimal8SumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int type;
        private long value;

        public Decimal8SumOverPartitionRowsFrameFunction(
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
            frameIncludesCurrentValue = rowsHi == 0;
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
            MapValue mv = key.createValue();

            long count;
            long loIdx;
            long startOffset;
            long acc;
            byte b = arg.getDecimal8(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && b != Decimals.DECIMAL8_NULL) {
                    acc = b;
                    count = 1;
                    value = acc;
                } else {
                    acc = 0L;
                    value = Decimals.DECIMAL64_NULL;
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
                    value = acc;
                } else {
                    value = Decimals.DECIMAL64_NULL;
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

    static class Decimal8SumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        private final int type;
        private long acc;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private long value;

        public Decimal8SumOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int type
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                this.frameLoBounded = rangeLo != Long.MIN_VALUE;
                this.maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
                this.minDiff = Math.abs(rangeHi);
                this.timestampIndex = timestampIdx;
                this.type = type;
                capacity = initialCapacity;
                // memory allocates lazily on reopen(), under the tracker bound by the cursor
                firstIdx = 0;
                frameSize = 0;
                acc = 0L;
                value = Decimals.DECIMAL64_NULL;
            } catch (Throwable th) {
                memory.close();
                throw th;
            }
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
            value = frameSize != 0 ? acc : Decimals.DECIMAL64_NULL;
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
        public void reopen() {
            value = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc = 0L;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
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
            value = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc = 0L;
        }
    }

    static class Decimal8SumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int type;
        private long acc;
        private long count = 0;
        private int loIdx = 0;
        private long value;

        public Decimal8SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int type) {
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
            acc = 0L;
            value = Decimals.DECIMAL64_NULL;
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
            value = count != 0 ? acc : Decimals.DECIMAL64_NULL;

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
        public void reopen() {
            value = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc = 0L;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            value = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc = 0L;
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
            value = Decimals.DECIMAL64_NULL;
            count = 0;
            loIdx = 0;
            acc = 0L;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
        }
    }

    static class Decimal8SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final int type;
        private long value;

        public Decimal8SumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.createValue();

            long acc;
            boolean wasNullState;
            if (mv.isNew()) {
                acc = 0L;
                wasNullState = true;
            } else {
                acc = mv.getLong(0);
                wasNullState = mv.getBool(1);
            }

            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                if (wasNullState) {
                    acc = b;
                    wasNullState = false;
                } else {
                    acc += b;
                }
            }
            mv.putLong(0, acc);
            mv.putBool(1, wasNullState);
            value = wasNullState ? Decimals.DECIMAL64_NULL : acc;
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
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal8SumOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final int type;
        private long acc;
        private boolean isNullState = true;
        private long value;

        public Decimal8SumOverUnboundedRowsFrameFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            acc = 0L;
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                if (isNullState) {
                    acc = b;
                    isNullState = false;
                } else {
                    acc += b;
                }
            }
            value = isNullState ? Decimals.DECIMAL64_NULL : acc;
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
            isNullState = true;
            acc = 0L;
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
            value = Decimals.DECIMAL64_NULL;
            isNullState = true;
            acc = 0L;
        }
    }

    static class Decimal8SumOverWholeResultSetFunction extends BaseWindowFunction {

        private final int type;
        private long acc;
        private boolean isNullState = true;
        private long value;

        public Decimal8SumOverWholeResultSetFunction(Function arg, int type) {
            super(arg);
            this.type = type;
            acc = 0L;
            value = Decimals.DECIMAL64_NULL;
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
                if (isNullState) {
                    acc = b;
                    isNullState = false;
                } else {
                    acc += b;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void preparePass2() {
            value = isNullState ? Decimals.DECIMAL64_NULL : acc;
        }

        @Override
        public void reset() {
            super.reset();
            value = Decimals.DECIMAL64_NULL;
            isNullState = true;
            acc = 0L;
        }

        @Override
        public void toTop() {
            super.toTop();
            value = Decimals.DECIMAL64_NULL;
            isNullState = true;
            acc = 0L;
        }
    }

    static {
        SUM_DECIMAL64_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL64_TYPES.add(ColumnType.DECIMAL128);
        SUM_DECIMAL64_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_TYPES.add(ColumnType.BOOLEAN);

        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL128);
        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL128);
        SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL8_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL8_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_TYPES.add(ColumnType.BOOLEAN);

        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL8_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL16_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL16_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_TYPES.add(ColumnType.BOOLEAN);

        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL16_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL128_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL128_TYPES.add(ColumnType.DECIMAL256);
        SUM_DECIMAL128_TYPES.add(ColumnType.BOOLEAN);

        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL256);
        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL128_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL256);
        SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        SUM_DECIMAL128_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
    }
}
