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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

/**
 * Exact aggregate implementations that the fused parallel hash join GROUP BY accepts. Each class
 * merges partial results, keeps its whole state in fixed-size map value columns, keeps no per-group
 * state outside the map (no allocator, sets, sinks or histograms), and returns a result that does not
 * depend on the order of rows, because joined pairs have no order.
 * <p>
 * Lookups match exact classes, so a subclass or an aggregate added later keeps the ordinary plan
 * until it is reviewed and registered here. The registry lives in this package because most of the
 * classes it names are package-private. HashJoinGroupByCandidate.supportsAggregate() checks the
 * parallelism and stability of the aggregate and its arguments before it asks the registry.
 */
public final class HashJoinGroupByAggregates {
    // Every entry reads both arguments through getDouble(), so the argument functions convert
    // other types identically in both plans, and the entries carry no argument type.
    private static final ObjHashSet<Class<?>> BINARY = new ObjHashSet<>();
    private static final ObjList<Class<?>> CLASSES = new ObjList<>();
    private static final int NO_TYPE = -2;
    // Maps a class to the argument type tag it was reviewed and tested for. A class may serve
    // other argument types as well: avg(boolean) returns AvgDoubleGroupByFunction, and the
    // function parser passes a narrower numeric argument through without a cast. Those
    // combinations keep the ordinary plan.
    private static final ObjIntHashMap<Class<?>> UNARY = new ObjIntHashMap<>(256, 0.5, NO_TYPE);

    private HashJoinGroupByAggregates() {
    }

    @TestOnly
    public static ObjList<Class<?>> getSupportedClasses() {
        return new ObjList<>(CLASSES);
    }

    public static boolean isSupportedBinary(Class<?> type) {
        return BINARY.contains(type);
    }

    public static boolean isSupportedNullary(Class<?> type) {
        return type == CountLongConstGroupByFunction.class;
    }

    public static boolean isSupportedUnary(Class<?> type, int argType) {
        final int tag = UNARY.get(type);
        return tag != NO_TYPE && tag == ColumnType.tagOf(argType);
    }

    private static void binary(Class<?> type) {
        BINARY.add(type);
        CLASSES.add(type);
    }

    private static void unary(Class<?> type, int argTag) {
        assert UNARY.get(type) == NO_TYPE;
        UNARY.put(type, argTag);
        CLASSES.add(type);
    }

    static {
        CLASSES.add(CountLongConstGroupByFunction.class);

        unary(AvgDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(AvgIntGroupByFunction.class, ColumnType.INT);
        unary(AvgLongGroupByFunction.class, ColumnType.LONG);
        unary(AvgShortGroupByFunction.class, ColumnType.SHORT);
        unary(AvgDecimal8GroupByFunction.class, ColumnType.DECIMAL8);
        unary(AvgDecimal16GroupByFunction.class, ColumnType.DECIMAL16);
        unary(AvgDecimal32GroupByFunction.class, ColumnType.DECIMAL32);
        unary(AvgDecimal64GroupByFunction.class, ColumnType.DECIMAL64);
        unary(AvgDecimal128GroupByFunction.class, ColumnType.DECIMAL128);
        unary(AvgDecimal256GroupByFunction.class, ColumnType.DECIMAL256);
        unary(AvgDecimal8Rescale256GroupByFunction.class, ColumnType.DECIMAL8);
        unary(AvgDecimal16Rescale256GroupByFunction.class, ColumnType.DECIMAL16);
        unary(AvgDecimal32Rescale256GroupByFunction.class, ColumnType.DECIMAL32);
        unary(AvgDecimal64Rescale256GroupByFunction.class, ColumnType.DECIMAL64);
        unary(AvgDecimal128Rescale256GroupByFunction.class, ColumnType.DECIMAL128);
        unary(AvgDecimal256Rescale256GroupByFunction.class, ColumnType.DECIMAL256);

        unary(SumDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(SumFloatGroupByFunction.class, ColumnType.FLOAT);
        unary(SumIntGroupByFunction.class, ColumnType.INT);
        unary(SumLongGroupByFunction.class, ColumnType.LONG);
        unary(SumShortGroupByFunction.class, ColumnType.SHORT);
        unary(SumLong256GroupByFunction.class, ColumnType.LONG256);
        unary(SumDecimal8GroupByFunction.class, ColumnType.DECIMAL8);
        unary(SumDecimal16GroupByFunction.class, ColumnType.DECIMAL16);
        unary(SumDecimal32GroupByFunction.class, ColumnType.DECIMAL32);
        unary(SumDecimal64GroupByFunction.class, ColumnType.DECIMAL64);
        unary(SumDecimal128GroupByFunction.class, ColumnType.DECIMAL128);
        unary(SumDecimal256GroupByFunction.class, ColumnType.DECIMAL256);
        unary(KSumDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(NSumDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(GeomeanDoubleGroupByFunction.class, ColumnType.DOUBLE);

        // MinShort and MaxShort store and return INT. No min, max, sum or avg class exists for BYTE.
        unary(MinCharGroupByFunction.class, ColumnType.CHAR);
        unary(MinDateGroupByFunction.class, ColumnType.DATE);
        unary(MinDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(MinFloatGroupByFunction.class, ColumnType.FLOAT);
        unary(MinIntGroupByFunction.class, ColumnType.INT);
        unary(MinIPv4GroupByFunction.class, ColumnType.IPv4);
        unary(MinLongGroupByFunction.class, ColumnType.LONG);
        unary(MinShortGroupByFunction.class, ColumnType.SHORT);
        unary(MinTimestampGroupByFunction.class, ColumnType.TIMESTAMP);
        unary(MinDecimalGroupByFunctionFactory.Decimal8Func.class, ColumnType.DECIMAL8);
        unary(MinDecimalGroupByFunctionFactory.Decimal16Func.class, ColumnType.DECIMAL16);
        unary(MinDecimalGroupByFunctionFactory.Decimal32Func.class, ColumnType.DECIMAL32);
        unary(MinDecimalGroupByFunctionFactory.Decimal64Func.class, ColumnType.DECIMAL64);
        unary(MinDecimalGroupByFunctionFactory.Decimal128Func.class, ColumnType.DECIMAL128);
        unary(MinDecimalGroupByFunctionFactory.Decimal256Func.class, ColumnType.DECIMAL256);
        unary(MaxCharGroupByFunction.class, ColumnType.CHAR);
        unary(MaxDateGroupByFunction.class, ColumnType.DATE);
        unary(MaxDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(MaxFloatGroupByFunction.class, ColumnType.FLOAT);
        unary(MaxIntGroupByFunction.class, ColumnType.INT);
        unary(MaxIPv4GroupByFunction.class, ColumnType.IPv4);
        unary(MaxLongGroupByFunction.class, ColumnType.LONG);
        unary(MaxShortGroupByFunction.class, ColumnType.SHORT);
        unary(MaxTimestampGroupByFunction.class, ColumnType.TIMESTAMP);
        unary(MaxDecimalGroupByFunctionFactory.Decimal8Func.class, ColumnType.DECIMAL8);
        unary(MaxDecimalGroupByFunctionFactory.Decimal16Func.class, ColumnType.DECIMAL16);
        unary(MaxDecimalGroupByFunctionFactory.Decimal32Func.class, ColumnType.DECIMAL32);
        unary(MaxDecimalGroupByFunctionFactory.Decimal64Func.class, ColumnType.DECIMAL64);
        unary(MaxDecimalGroupByFunctionFactory.Decimal128Func.class, ColumnType.DECIMAL128);
        unary(MaxDecimalGroupByFunctionFactory.Decimal256Func.class, ColumnType.DECIMAL256);

        unary(BitAndByteGroupByFunction.class, ColumnType.BYTE);
        unary(BitAndShortGroupByFunction.class, ColumnType.SHORT);
        unary(BitAndIntGroupByFunction.class, ColumnType.INT);
        unary(BitAndLongGroupByFunction.class, ColumnType.LONG);
        unary(BitOrByteGroupByFunction.class, ColumnType.BYTE);
        unary(BitOrShortGroupByFunction.class, ColumnType.SHORT);
        unary(BitOrIntGroupByFunction.class, ColumnType.INT);
        unary(BitOrLongGroupByFunction.class, ColumnType.LONG);
        unary(BitXorByteGroupByFunction.class, ColumnType.BYTE);
        unary(BitXorShortGroupByFunction.class, ColumnType.SHORT);
        unary(BitXorIntGroupByFunction.class, ColumnType.INT);
        unary(BitXorLongGroupByFunction.class, ColumnType.LONG);
        unary(BoolAndGroupByFunction.class, ColumnType.BOOLEAN);
        unary(BoolOrGroupByFunction.class, ColumnType.BOOLEAN);

        // The concrete subclasses of AbstractCountGroupByFunction, each with one LONG of state.
        unary(CountDoubleGroupByFunction.class, ColumnType.DOUBLE);
        unary(CountFloatGroupByFunction.class, ColumnType.FLOAT);
        unary(CountIntGroupByFunction.class, ColumnType.INT);
        unary(CountLongGroupByFunction.class, ColumnType.LONG);
        unary(CountIPv4GroupByFunction.class, ColumnType.IPv4);
        unary(CountLong256GroupByFunction.class, ColumnType.LONG256);
        unary(CountUuidGroupByFunction.class, ColumnType.UUID);
        unary(CountStrGroupByFunction.class, ColumnType.STRING);
        unary(CountVarcharGroupByFunction.class, ColumnType.VARCHAR);
        unary(CountSymbolGroupByFunction.class, ColumnType.SYMBOL);
        unary(CountGeoHashGroupByFunctionByte.class, ColumnType.GEOBYTE);
        unary(CountGeoHashGroupByFunctionShort.class, ColumnType.GEOSHORT);
        unary(CountGeoHashGroupByFunctionInt.class, ColumnType.GEOINT);
        unary(CountGeoHashGroupByFunctionLong.class, ColumnType.GEOLONG);
        unary(CountDecimalGroupByFunctionFactory.Decimal8Func.class, ColumnType.DECIMAL8);
        unary(CountDecimalGroupByFunctionFactory.Decimal16Func.class, ColumnType.DECIMAL16);
        unary(CountDecimalGroupByFunctionFactory.Decimal32Func.class, ColumnType.DECIMAL32);
        unary(CountDecimalGroupByFunctionFactory.Decimal64Func.class, ColumnType.DECIMAL64);
        unary(CountDecimalGroupByFunctionFactory.Decimal128Func.class, ColumnType.DECIMAL128);
        unary(CountDecimalGroupByFunctionFactory.Decimal256Func.class, ColumnType.DECIMAL256);

        unary(StdDevSampleGroupByFunctionFactory.StdDevSampleGroupByFunction.class, ColumnType.DOUBLE);
        unary(StdDevPopGroupByFunctionFactory.StdDevPopGroupByFunction.class, ColumnType.DOUBLE);
        unary(VarSampleGroupByFunctionFactory.VarSampleGroupByFunction.class, ColumnType.DOUBLE);
        unary(VarPopGroupByFunctionFactory.VarPopGroupByFunction.class, ColumnType.DOUBLE);
        unary(SkewnessSampleGroupByFunctionFactory.SkewnessSampleGroupByFunction.class, ColumnType.DOUBLE);
        unary(SkewnessPopGroupByFunctionFactory.SkewnessPopGroupByFunction.class, ColumnType.DOUBLE);
        unary(KurtosisSampleGroupByFunctionFactory.KurtosisSampleGroupByFunction.class, ColumnType.DOUBLE);
        unary(KurtosisPopGroupByFunctionFactory.KurtosisPopGroupByFunction.class, ColumnType.DOUBLE);

        binary(CovarSampleGroupByFunctionFactory.CovarSampleGroupByFunction.class);
        binary(CovarPopGroupByFunction.class);
        binary(CorrGroupByFunctionFactory.CorrGroupByFunction.class);
        binary(RegressionSlopeFunctionFactory.RegressionSlopeFunction.class);
        binary(RegressionR2FunctionFactory.RegressionR2Function.class);
        binary(RegressionInterceptFunctionFactory.RegressionInterceptFunction.class);
        binary(WeightedStdDevReliabilityGroupByFunctionFactory.WeightedStdDevReliabilityGroupByFunction.class);
        binary(WeightedStdDevFrequencyGroupByFunctionFactory.WeightedStdDevFrequencyGroupByFunction.class);
        binary(WeightedAvgDoubleGroupByFunction.class);
        binary(VwapDoubleGroupByFunction.class);
    }
}
