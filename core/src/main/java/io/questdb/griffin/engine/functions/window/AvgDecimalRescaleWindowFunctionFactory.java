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
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.math.RoundingMode;

public class AvgDecimalRescaleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final ArrayColumnTypes AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES;
    public static final ArrayColumnTypes AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES;
    public static final ArrayColumnTypes AVG_RESCALE_DECIMAL64_TYPES;
    private static final String NAME = "avg";
    private static final String SIGNATURE = NAME + "(Ξi)";

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
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        final int targetScale = args.getQuick(1).getInt(null);
        final int scalePosition = argPositions.getQuick(1);
        if (targetScale < 0) {
            throw SqlException.$(scalePosition, "non-negative scale required: ").put(targetScale);
        }
        if (targetScale > Decimals.MAX_SCALE) {
            throw SqlException.$(scalePosition, "scale exceeds maximum of ").put(Decimals.MAX_SCALE).put(": ").put(targetScale);
        }
        Function arg = args.getQuick(0);
        int argType = arg.getType();
        final int argPrecision = ColumnType.getDecimalPrecision(argType);
        final int argScale = ColumnType.getDecimalScale(argType);
        final int targetPrecision = argPrecision - argScale + targetScale;
        if (targetPrecision > Decimals.MAX_PRECISION) {
            throw SqlException.$(scalePosition, "rescaled decimal has precision that exceeds maximum of ")
                    .put(Decimals.MAX_PRECISION).put(": ").put(targetPrecision);
        }
        final int targetType = ColumnType.getDecimalType(targetPrecision, targetScale);
        int tag = ColumnType.tagOf(argType);
        int argPos = argPositions.getQuick(0);

        if (rowsHi < rowsLo) {
            boolean isRange = framingMode == WindowExpression.FRAMING_RANGE;
            return new Decimal256NullFunction(arg, NAME, rowsLo, rowsHi, isRange, partitionByRecord, targetType);
        }

        if (tag == ColumnType.DECIMAL8) {
            return newInstanceDecimal8(position, args, configuration, sqlExecutionContext, argType, targetType, argPos);
        }
        if (tag == ColumnType.DECIMAL16) {
            return newInstanceDecimal16(position, args, configuration, sqlExecutionContext, argType, targetType, argPos);
        }
        if (tag == ColumnType.DECIMAL32) {
            return newInstanceDecimal32(position, args, configuration, sqlExecutionContext, argType, targetType, argPos);
        }
        if (tag == ColumnType.DECIMAL128) {
            return newInstanceDecimal128(position, args, configuration, sqlExecutionContext, argType, targetType, argPos);
        }
        if (tag == ColumnType.DECIMAL256) {
            return newInstanceDecimal256(position, args, configuration, sqlExecutionContext, argType, targetType, argPos);
        }
        if (tag != ColumnType.DECIMAL64) {
            throw SqlException.$(position, "avg(decimal, scale) is not yet implemented for ").put(ColumnType.nameOf(tag));
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal64Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal64Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64Rescale256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, targetType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal64Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal64Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal64Rescale256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, targetType, argPos);
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
                    return new Decimal64Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64Rescale256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, targetType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal64Rescale256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, targetType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
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
            int targetType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal128Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal128Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128Rescale256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, targetType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal128Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal128Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal128Rescale256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, targetType, argPos);
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
                    return new Decimal128Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128Rescale256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, targetType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal128Rescale256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, targetType, argPos);
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
            int targetType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal16Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal16Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16Rescale256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, targetType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal16Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal16Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal16Rescale256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, targetType, argPos);
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
                    return new Decimal16Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16Rescale256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, targetType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal16Rescale256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, targetType, argPos);
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
            int targetType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal256Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal256Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256Rescale256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, targetType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal256Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal256Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal256Rescale256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, targetType, argPos);
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
                    return new Decimal256Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256Rescale256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, targetType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal256Rescale256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, targetType, argPos);
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
            int targetType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal32Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal32Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32Rescale256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, targetType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal32Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal32Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal32Rescale256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, targetType, argPos);
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
                    return new Decimal32Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32Rescale256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, targetType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal32Rescale256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, targetType, argPos);
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
            int targetType,
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
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal8Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal8Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8Rescale256AvgOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, configuration.getSqlWindowInitialRangeBufferSize(), timestampIndex, argType, targetType, argPos);
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal8Rescale256AvgOverUnboundedPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_TYPES);
                    return new Decimal8Rescale256AvgOverPartitionFunction(map, partitionByRecord, partitionBySink, arg, argType, targetType, argPos);
                } else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES);
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                        return new Decimal8Rescale256AvgOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, arg, mem, argType, targetType, argPos);
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
                    return new Decimal8Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8Rescale256AvgOverRangeFrameFunction(rowsLo, rowsHi, arg, configuration, timestampIndex, argType, targetType, argPos);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8Rescale256AvgOverUnboundedRowsFrameFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8Rescale256AvgOverCurrentRowFunction(arg, argType, targetType, argPos);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8Rescale256AvgOverWholeResultSetFunction(arg, argType, targetType, argPos);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    return new Decimal8Rescale256AvgOverRowsFrameFunction(arg, rowsLo, rowsHi, mem, argType, targetType, argPos);
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static void doDivide(Decimal256 acc, long count, int argScale, int targetScale, int position, Decimal256 outScratch) {
        outScratch.copyRaw(acc);
        outScratch.setScale(argScale);
        try {
            outScratch.divide(0, 0, 0, count, 0, targetScale, RoundingMode.HALF_EVEN);
        } catch (NumericException e) {
            throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
        }
    }

    static void writeSink(WindowSPI spi, long recordOffset, int columnIndex, Decimal256 v, int targetType) {
        final long addr = spi.getAddress(recordOffset, columnIndex);
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
                Unsafe.putByte(addr, v.isNull() ? Decimals.DECIMAL8_NULL : (byte) v.getLl());
                break;
            case ColumnType.DECIMAL16:
                Unsafe.putShort(addr, v.isNull() ? Decimals.DECIMAL16_NULL : (short) v.getLl());
                break;
            case ColumnType.DECIMAL32:
                Unsafe.putInt(addr, v.isNull() ? Decimals.DECIMAL32_NULL : (int) v.getLl());
                break;
            case ColumnType.DECIMAL64:
                Unsafe.putLong(addr, v.isNull() ? Decimals.DECIMAL64_NULL : v.getLl());
                break;
            case ColumnType.DECIMAL128:
                if (v.isNull()) {
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                } else {
                    Unsafe.putLong(addr, v.getLh());
                    Unsafe.putLong(addr + Long.BYTES, v.getLl());
                }
                break;
            default:
                Unsafe.putLong(addr, v.getHh());
                Unsafe.putLong(addr + Long.BYTES, v.getHl());
                Unsafe.putLong(addr + 2 * Long.BYTES, v.getLh());
                Unsafe.putLong(addr + 3 * Long.BYTES, v.getLl());
                break;
        }
    }

    static class Decimal128Rescale256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int argScale;
        private final Decimal256 outScratch = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        Decimal128Rescale256AvgOverCurrentRowFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            if (scratch.isNull()) {
                value.ofRawNull();
            } else {
                Decimal256 acc = outScratch;
                acc.ofRaw(scratch.getHigh(), scratch.getLow());
                doDivide(acc, 1, argScale, targetScale, position, value);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal128Rescale256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;

        public Decimal128Rescale256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (computeAvg(rec)) {
                sink.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return computeAvg(rec) ? (short) divScratch.getLl() : Decimals.DECIMAL16_NULL;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (computeAvg(rec)) {
                sink.copyRaw(divScratch);
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return computeAvg(rec) ? (int) divScratch.getLl() : Decimals.DECIMAL32_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return computeAvg(rec) ? divScratch.getLl() : Decimals.DECIMAL64_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return computeAvg(rec) ? (byte) divScratch.getLl() : Decimals.DECIMAL8_NULL;
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
            return targetType;
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
                acc.ofRaw(scratch.getHigh(), scratch.getLow());
                mv.putDecimal256(0, acc);
                mv.putLong(1, 1);
            } else {
                mv.getDecimal256(0, acc);
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                mv.putDecimal256(0, acc);
                mv.addLong(1, 1);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv == null) {
                writeNull(addr);
                return;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                writeNull(addr);
                return;
            }
            mv.getDecimal256(0, acc);
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            writeSink(spi, recordOffset, columnIndex, divScratch, targetType);
        }

        private boolean computeAvg(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv == null) {
                return false;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                return false;
            }
            mv.getDecimal256(0, acc);
            if (!acc.isNull() && acc.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            return true;
        }

        private void writeNull(long addr) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    Unsafe.putInt(addr, Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                    break;
                default:
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                    break;
            }
        }
    }

    public static class Decimal128Rescale256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
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
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();

        public Decimal128Rescale256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            long inHigh = scratch.getHigh();
            long inLow = scratch.getLow();

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    if (frameIncludesCurrentValue) {
                        acc.ofRaw(inHigh, inLow);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
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
                    scratch.ofRaw(inHigh, inLow);
                    memory.putDecimal128(slotOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                    size++;
                }

                if (frameLoBounded) {
                    for (long j = frameSize; j < size; j++) {
                        long idx = (firstIdx + j) % capacity;
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
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
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);

            if (frameSize != 0) {
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    public static class Decimal128Rescale256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal128Rescale256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                startOffset = memory.appendAddressFor((long) bufferSize * 16L) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !isNull) {
                    acc.ofRaw(inHigh, inLow);
                    count = 1;
                } else {
                    acc.ofRaw(0);
                    count = 0;
                }
                for (int j = 0; j < bufferSize; j++) {
                    memory.putLong(startOffset + (long) j * 16L, Decimals.DECIMAL128_HI_NULL);
                    memory.putLong(startOffset + (long) j * 16L + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
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
                    long hiOff = startOffset + ((loIdx + frameSize - 1) % bufferSize) * 16L;
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
                if (frameLoBounded) {
                    long loH = memory.getLong(startOffset + loIdx * 16L);
                    long loL = memory.getLong(startOffset + loIdx * 16L + Long.BYTES);
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
            memory.putLong(startOffset + loIdx * 16L, inHigh);
            memory.putLong(startOffset + loIdx * 16L + Long.BYTES, inLow);

            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal128Rescale256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + 16;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal128Rescale256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
                for (long j = frameSize, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal128Rescale256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal128Rescale256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int argType, int targetType, int position) {
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                long off = (long) ((loIdx + frameSize - 1) % bufferSize) * 16L;
                hiH = buffer.getLong(off);
                hiL = buffer.getLong(off + Long.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                long off = (long) (loIdx % bufferSize) * 16L;
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                long off = (long) loIdx * 16L;
                long loH = buffer.getLong(off);
                long loL = buffer.getLong(off + Long.BYTES);
                if (!Decimal128.isNull(loH, loL)) {
                    acc.subtract(loH < 0 ? -1L : 0L, loH < 0 ? -1L : 0L, loH, loL, 0);
                    count--;
                }
            }
            long writeOff = (long) loIdx * 16L;
            buffer.putLong(writeOff, inHigh);
            buffer.putLong(writeOff + Long.BYTES, inLow);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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
            super.toTop();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * 16L, Decimals.DECIMAL128_HI_NULL);
                buffer.putLong((long) i * 16L + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    static class Decimal128Rescale256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal128Rescale256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal128Rescale256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal128Rescale256AvgOverUnboundedRowsFrameFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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

    static class Decimal128Rescale256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal128 scratch = new Decimal128();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal128Rescale256AvgOverWholeResultSetFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
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
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
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

    static class Decimal16Rescale256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int argScale;
        private final Decimal256 outScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        Decimal16Rescale256AvgOverCurrentRowFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            if (s == Decimals.DECIMAL16_NULL) {
                value.ofRawNull();
            } else {
                Decimal256 acc = outScratch;
                acc.ofRaw(s);
                doDivide(acc, 1, argScale, targetScale, position, value);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal16Rescale256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;

        public Decimal16Rescale256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (computeAvg(rec)) {
                sink.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return computeAvg(rec) ? (short) divScratch.getLl() : Decimals.DECIMAL16_NULL;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (computeAvg(rec)) {
                sink.copyRaw(divScratch);
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return computeAvg(rec) ? (int) divScratch.getLl() : Decimals.DECIMAL32_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return computeAvg(rec) ? divScratch.getLl() : Decimals.DECIMAL64_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return computeAvg(rec) ? (byte) divScratch.getLl() : Decimals.DECIMAL8_NULL;
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
            return targetType;
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
                acc.ofRaw(s);
                mv.putDecimal256(0, acc);
                mv.putLong(1, 1);
            } else {
                mv.getDecimal256(0, acc);
                try {
                    Decimal256.uncheckedAdd(acc, s);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                mv.putDecimal256(0, acc);
                mv.addLong(1, 1);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv == null) {
                writeNull(addr);
                return;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                writeNull(addr);
                return;
            }
            mv.getDecimal256(0, acc);
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            writeSink(spi, recordOffset, columnIndex, divScratch, targetType);
        }

        private boolean computeAvg(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv == null) {
                return false;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                return false;
            }
            mv.getDecimal256(0, acc);
            if (!acc.isNull() && acc.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            return true;
        }

        private void writeNull(long addr) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    Unsafe.putInt(addr, Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                    break;
                default:
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                    break;
            }
        }
    }

    public static class Decimal16Rescale256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
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
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();

        public Decimal16Rescale256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            short s = arg.getDecimal16(record);
            boolean isNull = s == Decimals.DECIMAL16_NULL;

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putShort(startOffset + Long.BYTES, s);
                    if (frameIncludesCurrentValue) {
                        acc.ofRaw(s);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
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
                                short v = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc.subtract(v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v, 0);
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
                    memory.putShort(slotOffset + Long.BYTES, s);
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            short v = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, v);
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
                            short v = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, v);
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
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);

            if (frameSize != 0) {
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    public static class Decimal16Rescale256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal16Rescale256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            short s = arg.getDecimal16(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && s != Decimals.DECIMAL16_NULL) {
                    acc.ofRaw(s);
                    count = 1;
                } else {
                    acc.ofRaw(0);
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                short hi = frameIncludesCurrentValue ? s : memory.getShort(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Short.BYTES);
                if (hi != Decimals.DECIMAL16_NULL) {
                    count++;
                    try {
                        Decimal256.uncheckedAdd(acc, hi);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                }
                if (frameLoBounded) {
                    short lo = memory.getShort(startOffset + loIdx * Short.BYTES);
                    if (lo != Decimals.DECIMAL16_NULL) {
                        acc.subtract(lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo, 0);
                        count--;
                    }
                }
            }

            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putShort(startOffset + loIdx * Short.BYTES, s);

            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal16Rescale256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal16Rescale256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            short s = arg.getDecimal16(record);
            boolean isNull = s == Decimals.DECIMAL16_NULL;
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            short v = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc.subtract(v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v, 0);
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
                memory.putShort(slotOffset + Long.BYTES, s);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        short v = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, v);
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
                        short v = memory.getShort(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, v);
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
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal16Rescale256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal16Rescale256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int argType, int targetType, int position) {
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            short s = arg.getDecimal16(record);
            short hi;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hi = buffer.getShort((long) ((loIdx + frameSize - 1) % bufferSize) * Short.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hi = buffer.getShort((long) (loIdx % bufferSize) * Short.BYTES);
            } else {
                hi = s;
            }
            if (hi != Decimals.DECIMAL16_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, hi);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                short lo = buffer.getShort((long) loIdx * Short.BYTES);
                if (lo != Decimals.DECIMAL16_NULL) {
                    acc.subtract(lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo, 0);
                    count--;
                }
            }
            buffer.putShort((long) loIdx * Short.BYTES, s);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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
            super.toTop();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
        }
    }

    static class Decimal16Rescale256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal16Rescale256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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

            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, s);
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal16Rescale256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal16Rescale256AvgOverUnboundedRowsFrameFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, s);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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

    static class Decimal16Rescale256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal16Rescale256AvgOverWholeResultSetFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            short s = arg.getDecimal16(record);
            if (s != Decimals.DECIMAL16_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, s);
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
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
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

    static class Decimal256Rescale256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int argScale;
        private final Decimal256 outScratch = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        Decimal256Rescale256AvgOverCurrentRowFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            if (scratch.isNull()) {
                value.ofRawNull();
            } else {
                outScratch.copyRaw(scratch);
                doDivide(outScratch, 1, argScale, targetScale, position, value);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal256Rescale256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;

        public Decimal256Rescale256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (computeAvg(rec)) {
                sink.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return computeAvg(rec) ? (short) divScratch.getLl() : Decimals.DECIMAL16_NULL;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (computeAvg(rec)) {
                sink.copyRaw(divScratch);
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return computeAvg(rec) ? (int) divScratch.getLl() : Decimals.DECIMAL32_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return computeAvg(rec) ? divScratch.getLl() : Decimals.DECIMAL64_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return computeAvg(rec) ? (byte) divScratch.getLl() : Decimals.DECIMAL8_NULL;
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
            return targetType;
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
                acc.copyRaw(scratch);
                mv.putDecimal256(0, acc);
                mv.putLong(1, 1);
            } else {
                mv.getDecimal256(0, acc);
                try {
                    Decimal256.uncheckedAdd(acc, scratch);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                mv.putDecimal256(0, acc);
                mv.addLong(1, 1);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv == null) {
                writeNull(addr);
                return;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                writeNull(addr);
                return;
            }
            mv.getDecimal256(0, acc);
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            writeSink(spi, recordOffset, columnIndex, divScratch, targetType);
        }

        private boolean computeAvg(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv == null) {
                return false;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                return false;
            }
            mv.getDecimal256(0, acc);
            if (!acc.isNull() && acc.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            return true;
        }

        private void writeNull(long addr) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    Unsafe.putInt(addr, Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                    break;
                default:
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                    break;
            }
        }
    }

    public static class Decimal256Rescale256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
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
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();

        public Decimal256Rescale256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
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
                    for (long j = frameSize; j < size; j++) {
                        long idx = (firstIdx + j) % capacity;
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
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
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);

            if (frameSize != 0) {
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    public static class Decimal256Rescale256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal256Rescale256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                startOffset = memory.appendAddressFor((long) bufferSize * 32L) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && !isNull) {
                    acc.copyRaw(scratch);
                    count = 1;
                } else {
                    acc.ofRaw(0);
                    count = 0;
                }
                for (int j = 0; j < bufferSize; j++) {
                    long off = startOffset + (long) j * 32L;
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
                    readD256(memory, startOffset + ((loIdx + frameSize - 1) % bufferSize) * 32L, scratch);
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
                if (frameLoBounded) {
                    readD256(memory, startOffset + loIdx * 32L, scratch);
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
            long writeOff = startOffset + loIdx * 32L;
            memory.putLong(writeOff, inHh);
            memory.putLong(writeOff + Long.BYTES, inHl);
            memory.putLong(writeOff + 2 * Long.BYTES, inLh);
            memory.putLong(writeOff + 3 * Long.BYTES, inLl);

            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal256Rescale256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + 32;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal256Rescale256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
                for (long j = frameSize, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
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
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal256Rescale256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal256Rescale256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int argType, int targetType, int position) {
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                        ? (long) ((loIdx + frameSize - 1) % bufferSize) * 32L
                        : (long) (loIdx % bufferSize) * 32L;
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                readD256(buffer, (long) loIdx * 32L, scratch);
                if (!scratch.isNull()) {
                    acc.subtract(scratch);
                    count--;
                }
            }
            long writeOff = (long) loIdx * 32L;
            buffer.putLong(writeOff, inHh);
            buffer.putLong(writeOff + Long.BYTES, inHl);
            buffer.putLong(writeOff + 2 * Long.BYTES, inLh);
            buffer.putLong(writeOff + 3 * Long.BYTES, inLl);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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
            super.toTop();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                long off = (long) i * 32L;
                buffer.putLong(off, Decimals.DECIMAL256_HH_NULL);
                buffer.putLong(off + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                buffer.putLong(off + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                buffer.putLong(off + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            }
        }
    }

    static class Decimal256Rescale256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal256Rescale256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal256Rescale256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal256Rescale256AvgOverUnboundedRowsFrameFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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

    static class Decimal256Rescale256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final Decimal256 scratch = new Decimal256();
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal256Rescale256AvgOverWholeResultSetFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
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
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
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

    static class Decimal32Rescale256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int argScale;
        private final Decimal256 outScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        Decimal32Rescale256AvgOverCurrentRowFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            if (i == Decimals.DECIMAL32_NULL) {
                value.ofRawNull();
            } else {
                Decimal256 acc = outScratch;
                acc.ofRaw(i);
                doDivide(acc, 1, argScale, targetScale, position, value);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal32Rescale256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;

        public Decimal32Rescale256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (computeAvg(rec)) {
                sink.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return computeAvg(rec) ? (short) divScratch.getLl() : Decimals.DECIMAL16_NULL;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (computeAvg(rec)) {
                sink.copyRaw(divScratch);
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return computeAvg(rec) ? (int) divScratch.getLl() : Decimals.DECIMAL32_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return computeAvg(rec) ? divScratch.getLl() : Decimals.DECIMAL64_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return computeAvg(rec) ? (byte) divScratch.getLl() : Decimals.DECIMAL8_NULL;
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
            return targetType;
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
                acc.ofRaw(i);
                mv.putDecimal256(0, acc);
                mv.putLong(1, 1);
            } else {
                mv.getDecimal256(0, acc);
                try {
                    Decimal256.uncheckedAdd(acc, i);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                mv.putDecimal256(0, acc);
                mv.addLong(1, 1);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv == null) {
                writeNull(addr);
                return;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                writeNull(addr);
                return;
            }
            mv.getDecimal256(0, acc);
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            writeSink(spi, recordOffset, columnIndex, divScratch, targetType);
        }

        private boolean computeAvg(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv == null) {
                return false;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                return false;
            }
            mv.getDecimal256(0, acc);
            if (!acc.isNull() && acc.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            return true;
        }

        private void writeNull(long addr) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    Unsafe.putInt(addr, Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                    break;
                default:
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                    break;
            }
        }
    }

    public static class Decimal32Rescale256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
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
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();

        public Decimal32Rescale256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            int i = arg.getDecimal32(record);
            boolean isNull = i == Decimals.DECIMAL32_NULL;

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putInt(startOffset + Long.BYTES, i);
                    if (frameIncludesCurrentValue) {
                        acc.ofRaw(i);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                int v = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc.subtract(v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v, 0);
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
                    memory.putInt(slotOffset + Long.BYTES, i);
                    size++;
                }

                if (frameLoBounded) {
                    for (long j = frameSize; j < size; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            int v = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, v);
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
                    for (long j = 0, n = size; j < n; j++) {
                        long idx = (firstIdx + j) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            int v = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, v);
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
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);

            if (frameSize != 0) {
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    public static class Decimal32Rescale256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal32Rescale256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            int i = arg.getDecimal32(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && i != Decimals.DECIMAL32_NULL) {
                    acc.ofRaw(i);
                    count = 1;
                } else {
                    acc.ofRaw(0);
                    count = 0;
                }
                for (int j = 0; j < bufferSize; j++) {
                    memory.putInt(startOffset + (long) j * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                int hi = frameIncludesCurrentValue ? i : memory.getInt(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
                if (hi != Decimals.DECIMAL32_NULL) {
                    count++;
                    try {
                        Decimal256.uncheckedAdd(acc, hi);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                }
                if (frameLoBounded) {
                    int lo = memory.getInt(startOffset + loIdx * Integer.BYTES);
                    if (lo != Decimals.DECIMAL32_NULL) {
                        acc.subtract(lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo, 0);
                        count--;
                    }
                }
            }

            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putInt(startOffset + loIdx * Integer.BYTES, i);

            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal32Rescale256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal32Rescale256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            int i = arg.getDecimal32(record);
            boolean isNull = i == Decimals.DECIMAL32_NULL;
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            int v = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc.subtract(v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v, 0);
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
                memory.putInt(slotOffset + Long.BYTES, i);
                size++;
            }

            if (frameLoBounded) {
                for (long j = frameSize, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        int v = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, v);
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
                for (long j = 0, n = size; j < n; j++) {
                    long idx = (firstIdx + j) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        int v = memory.getInt(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, v);
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
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal32Rescale256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal32Rescale256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int argType, int targetType, int position) {
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            int i = arg.getDecimal32(record);
            int hi;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hi = buffer.getInt((long) ((loIdx + frameSize - 1) % bufferSize) * Integer.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hi = buffer.getInt((long) (loIdx % bufferSize) * Integer.BYTES);
            } else {
                hi = i;
            }
            if (hi != Decimals.DECIMAL32_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, hi);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                int lo = buffer.getInt((long) loIdx * Integer.BYTES);
                if (lo != Decimals.DECIMAL32_NULL) {
                    acc.subtract(lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo, 0);
                    count--;
                }
            }
            buffer.putInt((long) loIdx * Integer.BYTES, i);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal32Rescale256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal32Rescale256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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

            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, i);
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal32Rescale256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal32Rescale256AvgOverUnboundedRowsFrameFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, i);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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

    static class Decimal32Rescale256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal32Rescale256AvgOverWholeResultSetFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            int i = arg.getDecimal32(record);
            if (i != Decimals.DECIMAL32_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, i);
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
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
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

    static class Decimal64Rescale256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int argScale;
        private final Decimal256 outScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        Decimal64Rescale256AvgOverCurrentRowFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d == Decimals.DECIMAL64_NULL) {
                value.ofRawNull();
            } else {
                Decimal256 acc = outScratch;
                acc.ofRaw(d);
                doDivide(acc, 1, argScale, targetScale, position, value);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal64Rescale256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;

        public Decimal64Rescale256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (computeAvg(rec)) {
                sink.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return computeAvg(rec) ? (short) divScratch.getLl() : Decimals.DECIMAL16_NULL;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (computeAvg(rec)) {
                sink.copyRaw(divScratch);
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return computeAvg(rec) ? (int) divScratch.getLl() : Decimals.DECIMAL32_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return computeAvg(rec) ? divScratch.getLl() : Decimals.DECIMAL64_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return computeAvg(rec) ? (byte) divScratch.getLl() : Decimals.DECIMAL8_NULL;
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
            return targetType;
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
                acc.ofRaw(d);
                mv.putDecimal256(0, acc);
                mv.putLong(1, 1);
            } else {
                mv.getDecimal256(0, acc);
                try {
                    Decimal256.uncheckedAdd(acc, d);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                mv.putDecimal256(0, acc);
                mv.addLong(1, 1);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv == null) {
                writeNull(addr);
                return;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                writeNull(addr);
                return;
            }
            mv.getDecimal256(0, acc);
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            writeSink(spi, recordOffset, columnIndex, divScratch, targetType);
        }

        private boolean computeAvg(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv == null) {
                return false;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                return false;
            }
            mv.getDecimal256(0, acc);
            if (!acc.isNull() && acc.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            return true;
        }

        private void writeNull(long addr) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    Unsafe.putInt(addr, Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                    break;
                default:
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                    break;
            }
        }
    }

    public static class Decimal64Rescale256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();

        public Decimal64Rescale256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                acc.ofRaw(0, 0, 0, 0);
                if (d != Decimals.DECIMAL64_NULL) {
                    memory.putLong(startOffset, timestamp);
                    memory.putLong(startOffset + Long.BYTES, d);
                    if (frameIncludesCurrentValue) {
                        Decimal256.uncheckedAdd(acc, d);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                        doDivide(acc, 1, argScale, targetScale, position, value);
                    } else {
                        frameSize = 0;
                        size = 1;
                        value.ofRawNull();
                    }
                } else {
                    size = 0;
                    frameSize = 0;
                    value.ofRawNull();
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
                                long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc.subtract(val < 0 ? -1L : 0L, val < 0 ? -1L : 0L, val < 0 ? -1L : 0L, val, 0);
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
                            try {
                                Decimal256.uncheckedAdd(acc, val);
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
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, val);
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
                    doDivide(acc, frameSize, argScale, targetScale, position, value);
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
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(targetScale).val(')');
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
        }
    }

    public static class Decimal64Rescale256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal64Rescale256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            MapValue v = key.createValue();

            long count;
            long loIdx;
            long startOffset;
            long d = arg.getDecimal64(record);

            if (v.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                acc.ofRaw(0, 0, 0, 0);
                if (frameIncludesCurrentValue && d != Decimals.DECIMAL64_NULL) {
                    Decimal256.uncheckedAdd(acc, d);
                    count = 1;
                    doDivide(acc, 1, argScale, targetScale, position, value);
                } else {
                    value.ofRawNull();
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                v.getDecimal256(0, acc);
                count = v.getLong(2);
                loIdx = v.getLong(3);
                startOffset = v.getLong(4);

                long hiValue = frameIncludesCurrentValue ? d : memory.getLong(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Long.BYTES);
                if (hiValue != Decimals.DECIMAL64_NULL) {
                    count++;
                    try {
                        Decimal256.uncheckedAdd(acc, hiValue);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                }
                if (count != 0) {
                    doDivide(acc, count, argScale, targetScale, position, value);
                } else {
                    value.ofRawNull();
                }

                if (frameLoBounded) {
                    long loValue = memory.getLong(startOffset + loIdx * Long.BYTES);
                    if (loValue != Decimals.DECIMAL64_NULL) {
                        acc.subtract(loValue < 0 ? -1L : 0L, loValue < 0 ? -1L : 0L, loValue < 0 ? -1L : 0L, loValue, 0);
                        count--;
                    }
                }
            }

            v.putDecimal256(0, acc);
            v.putLong(2, count);
            v.putLong(3, (loIdx + 1) % bufferSize);
            v.putLong(4, startOffset);
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val('(').val(arg).val(", ").val(targetScale).val(')');
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

    static class Decimal64Rescale256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Long.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal64Rescale256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            acc.ofRaw(0, 0, 0, 0);
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
            long d = arg.getDecimal64(record);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc.subtract(val < 0 ? -1L : 0L, val < 0 ? -1L : 0L, val < 0 ? -1L : 0L, val, 0);
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
                        try {
                            Decimal256.uncheckedAdd(acc, val);
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
                        long val = memory.getLong(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, val);
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
                doDivide(acc, frameSize, argScale, targetScale, position, value);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void reopen() {
            value.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            acc.ofRaw(0, 0, 0, 0);
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(targetScale).val(')');
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
            acc.ofRaw(0, 0, 0, 0);
        }
    }

    static class Decimal64Rescale256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal64Rescale256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int argType, int targetType, int position) {
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0, 0, 0, 0);
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
                try {
                    Decimal256.uncheckedAdd(acc, hiValue);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, value);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                long loValue = buffer.getLong((long) loIdx * Long.BYTES);
                if (loValue != Decimals.DECIMAL64_NULL) {
                    acc.subtract(loValue < 0 ? -1L : 0L, loValue < 0 ? -1L : 0L, loValue < 0 ? -1L : 0L, loValue, 0);
                    count--;
                }
            }
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void reopen() {
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0, 0, 0, 0);
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0, 0, 0, 0);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(targetScale).val(')');
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
            acc.ofRaw(0, 0, 0, 0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
        }
    }

    static class Decimal64Rescale256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal64Rescale256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
                acc.ofRaw(0, 0, 0, 0);
                count = 0;
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
            }

            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, d);
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
                doDivide(acc, count, argScale, targetScale, position, value);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(", ").val(targetScale).val(')');
            sink.val(" over (");
            sink.val("partition by ").val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    static class Decimal64Rescale256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal64Rescale256AvgOverUnboundedRowsFrameFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0, 0, 0, 0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, d);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }

            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, value);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0, 0, 0, 0);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(", ").val(targetScale).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0, 0, 0, 0);
        }
    }

    static class Decimal64Rescale256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count;

        public Decimal64Rescale256AvgOverWholeResultSetFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0, 0, 0, 0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long d = arg.getDecimal64(record);
            if (d != Decimals.DECIMAL64_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, d);
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
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void preparePass2() {
            if (count > 0) {
                doDivide(acc, count, argScale, targetScale, position, value);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void reset() {
            super.reset();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0, 0, 0, 0);
        }

        @Override
        public void toTop() {
            super.toTop();
            value.ofRawNull();
            count = 0;
            acc.ofRaw(0, 0, 0, 0);
        }
    }

    static class Decimal8Rescale256AvgOverCurrentRowFunction extends BaseWindowFunction {

        private final int argScale;
        private final Decimal256 outScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        Decimal8Rescale256AvgOverCurrentRowFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            if (b == Decimals.DECIMAL8_NULL) {
                value.ofRawNull();
            } else {
                Decimal256 acc = outScratch;
                acc.ofRaw(b);
                doDivide(acc, 1, argScale, targetScale, position, value);
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal8Rescale256AvgOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;

        public Decimal8Rescale256AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (computeAvg(rec)) {
                sink.ofRaw(divScratch.getLh(), divScratch.getLl());
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return computeAvg(rec) ? (short) divScratch.getLl() : Decimals.DECIMAL16_NULL;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (computeAvg(rec)) {
                sink.copyRaw(divScratch);
            } else {
                sink.ofRawNull();
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return computeAvg(rec) ? (int) divScratch.getLl() : Decimals.DECIMAL32_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return computeAvg(rec) ? divScratch.getLl() : Decimals.DECIMAL64_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return computeAvg(rec) ? (byte) divScratch.getLl() : Decimals.DECIMAL8_NULL;
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
            return targetType;
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
                acc.ofRaw(b);
                mv.putDecimal256(0, acc);
                mv.putLong(1, 1);
            } else {
                mv.getDecimal256(0, acc);
                try {
                    Decimal256.uncheckedAdd(acc, b);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                mv.putDecimal256(0, acc);
                mv.addLong(1, 1);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            long addr = spi.getAddress(recordOffset, columnIndex);
            if (mv == null) {
                writeNull(addr);
                return;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                writeNull(addr);
                return;
            }
            mv.getDecimal256(0, acc);
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            writeSink(spi, recordOffset, columnIndex, divScratch, targetType);
        }

        private boolean computeAvg(Record rec) {
            partitionByRecord.of(rec);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mv = key.findValue();
            if (mv == null) {
                return false;
            }
            long count = mv.getLong(1);
            if (count == 0) {
                return false;
            }
            mv.getDecimal256(0, acc);
            if (!acc.isNull() && acc.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            doDivide(acc, count, argScale, targetScale, position, divScratch);
            return true;
        }

        private void writeNull(long addr) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    Unsafe.putInt(addr, Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    Unsafe.putLong(addr, Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                    break;
                default:
                    Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                    Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                    Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                    Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                    break;
            }
        }
    }

    public static class Decimal8Rescale256AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction {

        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
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
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();

        public Decimal8Rescale256AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            byte b = arg.getDecimal8(record);
            boolean isNull = b == Decimals.DECIMAL8_NULL;

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                if (!isNull) {
                    memory.putLong(startOffset, timestamp);
                    memory.putByte(startOffset + Long.BYTES, b);
                    if (frameIncludesCurrentValue) {
                        acc.ofRaw(b);
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        acc.ofRaw(0);
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    acc.ofRaw(0);
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
                                byte v = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                acc.subtract(v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v, 0);
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
                    memory.putByte(slotOffset + Long.BYTES, b);
                    size++;
                }

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            byte v = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, v);
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
                            byte v = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            try {
                                Decimal256.uncheckedAdd(acc, v);
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
            }

            mapValue.putDecimal256(0, acc);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);

            if (frameSize != 0) {
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    public static class Decimal8Rescale256AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal8Rescale256AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            byte b = arg.getDecimal8(record);

            if (mv.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && b != Decimals.DECIMAL8_NULL) {
                    acc.ofRaw(b);
                    count = 1;
                } else {
                    acc.ofRaw(0);
                    count = 0;
                }
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
            } else {
                mv.getDecimal256(0, acc);
                count = mv.getLong(1);
                loIdx = mv.getLong(2);
                startOffset = mv.getLong(3);

                byte hi = frameIncludesCurrentValue ? b : memory.getByte(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Byte.BYTES);
                if (hi != Decimals.DECIMAL8_NULL) {
                    count++;
                    try {
                        Decimal256.uncheckedAdd(acc, hi);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    if (acc.hasOverflowed()) {
                        throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                    }
                }
                if (frameLoBounded) {
                    byte lo = memory.getByte(startOffset + loIdx * Byte.BYTES);
                    if (lo != Decimals.DECIMAL8_NULL) {
                        acc.subtract(lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo, 0);
                        count--;
                    }
                }
            }

            mv.putDecimal256(0, acc);
            mv.putLong(1, count);
            mv.putLong(2, (loIdx + 1) % bufferSize);
            mv.putLong(3, startOffset);
            memory.putByte(startOffset + loIdx * Byte.BYTES, b);

            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal8Rescale256AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable {

        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final int timestampIndex;
        private final Decimal256 value = new Decimal256();
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;

        public Decimal8Rescale256AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int argType,
                int targetType,
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            byte b = arg.getDecimal8(record);
            boolean isNull = b == Decimals.DECIMAL8_NULL;
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            byte v = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            acc.subtract(v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v < 0 ? -1L : 0L, v, 0);
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
                memory.putByte(slotOffset + Long.BYTES, b);
                size++;
            }

            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        byte v = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, v);
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
                        byte v = memory.getByte(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        try {
                            Decimal256.uncheckedAdd(acc, v);
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
                doDivide(acc, frameSize, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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

    static class Decimal8Rescale256AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final Decimal256 divScratch = new Decimal256();
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;
        private int loIdx = 0;

        public Decimal8Rescale256AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int argType, int targetType, int position) {
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
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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
            byte b = arg.getDecimal8(record);
            byte hi;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hi = buffer.getByte((long) ((loIdx + frameSize - 1) % bufferSize) * Byte.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hi = buffer.getByte((long) (loIdx % bufferSize) * Byte.BYTES);
            } else {
                hi = b;
            }
            if (hi != Decimals.DECIMAL8_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, hi);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }

            if (frameLoBounded) {
                byte lo = buffer.getByte((long) loIdx * Byte.BYTES);
                if (lo != Decimals.DECIMAL8_NULL) {
                    acc.subtract(lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo < 0 ? -1L : 0L, lo, 0);
                    count--;
                }
            }
            buffer.putByte((long) loIdx * Byte.BYTES, b);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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
            sink.val(getName()).val('(').val(arg).val(',').val(targetScale).val(')');
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
            super.toTop();
            value.ofRawNull();
            count = 0;
            loIdx = 0;
            acc.ofRaw(0);
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
        }
    }

    static class Decimal8Rescale256AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();

        public Decimal8Rescale256AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int argType, int targetType, int position) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
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

            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, b);
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
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }
    }

    static class Decimal8Rescale256AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal8Rescale256AvgOverUnboundedRowsFrameFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, b);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
                }
                if (acc.hasOverflowed()) {
                    throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
                }
                count++;
            }
            if (count != 0) {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
                value.copyRaw(divScratch);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            writeSink(spi, recordOffset, columnIndex, value, targetType);
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

    static class Decimal8Rescale256AvgOverWholeResultSetFunction extends BaseWindowFunction {

        private final Decimal256 acc = new Decimal256();
        private final int argScale;
        private final Decimal256 divScratch = new Decimal256();
        private final int position;
        private final int targetScale;
        private final int targetType;
        private final Decimal256 value = new Decimal256();
        private long count = 0;

        public Decimal8Rescale256AvgOverWholeResultSetFunction(Function arg, int argType, int targetType, int position) {
            super(arg);
            this.argScale = ColumnType.getDecimalScale(argType);
            this.targetScale = ColumnType.getDecimalScale(targetType);
            this.targetType = targetType;
            this.position = position;
            acc.ofRaw(0);
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            sink.ofRaw(value.getLh(), value.getLl());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
            if (!sink.isNull() && sink.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            if (value.hasOverflowed()) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: an overflow occurred");
            }
            return (byte) value.getLl();
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
            return targetType;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            byte b = arg.getDecimal8(record);
            if (b != Decimals.DECIMAL8_NULL) {
                try {
                    Decimal256.uncheckedAdd(acc, b);
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
            writeSink(spi, recordOffset, columnIndex, value, targetType);
        }

        @Override
        public void preparePass2() {
            if (count == 0) {
                value.ofRawNull();
            } else {
                doDivide(acc, count, argScale, targetScale, position, divScratch);
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

    static {
        AVG_RESCALE_DECIMAL64_TYPES = new ArrayColumnTypes();
        AVG_RESCALE_DECIMAL64_TYPES.add(ColumnType.DECIMAL256);
        AVG_RESCALE_DECIMAL64_TYPES.add(ColumnType.LONG);

        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES = new ArrayColumnTypes();
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.DECIMAL256);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_RANGE_TYPES.add(ColumnType.LONG);

        AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES = new ArrayColumnTypes();
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.DECIMAL256);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
        AVG_RESCALE_DECIMAL64_OVER_PARTITION_ROWS_TYPES.add(ColumnType.LONG);
    }
}
