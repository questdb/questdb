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

package io.questdb.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.cast.CastByteToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDecimalToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDecimalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleArrayToDoubleArrayFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleArrayToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleArrayToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToDoubleArray;
import io.questdb.griffin.engine.functions.cast.CastDoubleToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntervalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.griffin.engine.union.ExceptAllRecordCursorFactory;
import io.questdb.griffin.engine.union.ExceptRecordCursorFactory;
import io.questdb.griffin.engine.union.IntersectAllRecordCursorFactory;
import io.questdb.griffin.engine.union.IntersectRecordCursorFactory;
import io.questdb.griffin.engine.union.MergeUnionAllRecordCursorFactoryBuilder;
import io.questdb.griffin.engine.union.SetRecordCursorFactoryConstructor;
import io.questdb.griffin.engine.union.UnionAllRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.ColumnType.*;

final class SetOperationFactoryGenerator {
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_ALL_CONSTRUCTOR = ExceptAllRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_CONSTRUCTOR = ExceptRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_ALL_CONSTRUCTOR = IntersectAllRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_CONSTRUCTOR = IntersectRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_UNION_CONSTRUCTOR = UnionRecordCursorFactory::new;
    private final BytecodeAssembler asm;
    private final ListColumnFilter branchSortKeys;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final ArrayColumnTypes keyTypes;
    private final SortFactoryGenerator sortGenerator;
    private final ArrayColumnTypes valueTypes;
    // a bitset of symbol columns serialised as strings by UNION, INTERSECT and EXCEPT record sinks
    private final BitSet writeSymbolAsString;
    private SqlExecutionContext mergeCastContext;
    private final MergeUnionAllRecordCursorFactoryBuilder.CastFunctionFactory mergeCastFactory = this::generateMergeCastFunctions;

    @Nullable SetOperationFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            SortFactoryGenerator sortGenerator,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter branchSortKeys,
            BitSet writeSymbolAsString
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.sortGenerator = sortGenerator;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
        this.keyTypes = keyTypes;
        this.branchSortKeys = branchSortKeys;
        this.writeSymbolAsString = writeSymbolAsString;
        this.valueTypes = valueTypes;
    }

    private static boolean canMergeUnionAll(
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            int timestampIndex,
            int scanDirection
    ) {
        final RecordMetadata metadataA = factoryA.getMetadata();
        final RecordMetadata metadataB = factoryB.getMetadata();
        return timestampIndex >= 0
                && timestampIndex == metadataA.getTimestampIndex()
                && timestampIndex == metadataB.getTimestampIndex()
                && metadataA.getColumnType(timestampIndex) == metadataB.getColumnType(timestampIndex)
                && (scanDirection == RecordCursorFactory.SCAN_DIRECTION_FORWARD
                || scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD)
                && scanDirection == factoryA.getScanDirection()
                && scanDirection == factoryB.getScanDirection();
    }

    private static Function castColumn(
            SqlExecutionContext executionContext,
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toTag,
            int toType,
            int modelPosition
    ) throws SqlException {
        return switch (toTag) {
            case BOOLEAN -> BooleanColumn.newInstance(i);
            case BYTE -> ByteColumn.newInstance(i);
            // BOOLEAN is never cast to CHAR, SHORT, INT or LONG: both sides of such a pair are cast to STRING.
            // Narrower types are cast to wider types, never the other way around.
            case SHORT -> switch (fromTag) {
                case BYTE, CHAR, SHORT -> numericColumn(fromTag, i);
                default -> null;
            };
            case CHAR -> switch (fromTag) {
                case BYTE -> new CastByteToCharFunctionFactory.Func(ByteColumn.newInstance(i));
                case CHAR -> new CharColumn(i);
                default -> null;
            };
            case INT -> switch (fromTag) {
                case BYTE, SHORT, CHAR, INT -> numericColumn(fromTag, i);
                default -> null;
            };
            case LONG -> switch (fromTag) {
                case BYTE, SHORT, CHAR, INT, LONG -> numericColumn(fromTag, i);
                default -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
            };
            case FLOAT -> switch (fromTag) {
                case BYTE, SHORT, INT, LONG, FLOAT -> numericColumn(fromTag, i);
                default -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
            };
            case DOUBLE -> switch (fromTag) {
                case BYTE, SHORT, INT, LONG, FLOAT, DOUBLE -> numericColumn(fromTag, i);
                default -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
            };
            case IPv4 -> {
                if (fromTag != IPv4) {
                    throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
                }
                yield IPv4Column.newInstance(i);
            }
            case DATE -> {
                if (fromTag != DATE) {
                    throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
                }
                yield DateColumn.newInstance(i);
            }
            case UUID -> {
                assert fromTag == UUID;
                yield UuidColumn.newInstance(i);
            }
            case LONG128 -> {
                assert fromTag == LONG128;
                yield Long128Column.newInstance(i);
            }
            case TIMESTAMP -> castToTimestamp(castFromMetadata, i, fromTag, fromType, toType, modelPosition);
            case STRING -> castToString(castFromMetadata, i, fromTag, fromType, toType, modelPosition);
            case SYMBOL ->
                    new CastSymbolToStrFunctionFactory.Func(new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i)));
            case LONG256 -> Long256Column.newInstance(i);
            case GEOBYTE, GEOSHORT, GEOINT, GEOLONG ->
                    castToGeoHash(castFromMetadata, i, fromTag, fromType, toTag, toType, modelPosition);
            case DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 ->
                    castToDecimal(executionContext, castFromMetadata, i, fromTag, fromType, toType, modelPosition);
            case BINARY -> BinColumn.newInstance(i);
            case VARCHAR -> castToVarchar(castFromMetadata, i, fromTag, fromType, toType, modelPosition);
            case INTERVAL -> IntervalColumn.newInstance(i, toType);
            case ARRAY -> castToArray(castFromMetadata, i, fromTag, fromType, toType, modelPosition);
            default -> {
                assert false;
                yield null;
            }
        };
    }

    private static Function castToArray(
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toType,
            int modelPosition
    ) throws SqlException {
        switch (fromTag) {
            case ARRAY: {
                assert decodeArrayElementType(fromType) == DOUBLE;
                assert decodeArrayElementType(toType) == DOUBLE;
                final int fromDims = decodeWeakArrayDimensionality(fromType);
                final int toDims = decodeWeakArrayDimensionality(toType);
                if (toDims == -1) {
                    throw SqlException.$(modelPosition, "cast to array bind variable type is not supported [column=")
                            .put(castFromMetadata.getColumnName(i)).put(']');
                }
                if (fromDims == toDims) {
                    return ArrayColumn.newInstance(i, fromType);
                }
                if (fromDims > toDims) {
                    throw SqlException.$(modelPosition, "array cast to lower dimensionality is not supported [column=")
                            .put(castFromMetadata.getColumnName(i)).put(']');
                }
                if (fromDims == -1) {
                    // must be a bind variable, i.e. weak dimensionality case
                    return new CastDoubleArrayToDoubleArrayFunctionFactory.WeakDimsFunc(ArrayColumn.newInstance(i, fromType), toType, modelPosition);
                }
                return new CastDoubleArrayToDoubleArrayFunctionFactory.Func(ArrayColumn.newInstance(i, fromType), toType, toDims - fromDims);
            }
            case DOUBLE:
                assert decodeArrayElementType(toType) == DOUBLE;
                if (decodeWeakArrayDimensionality(toType) == -1) {
                    throw SqlException
                            .$(modelPosition, "cast to array bind variable type is not supported [column=").put(castFromMetadata.getColumnName(i))
                            .put(']');
                }
                return new CastDoubleToDoubleArray.Func(DoubleColumn.newInstance(i), toType);
            default:
                assert false;
                return null;
        }
    }

    private static Function castToDecimal(
            SqlExecutionContext executionContext,
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toType,
            int modelPosition
    ) throws SqlException {
        if (ColumnType.isDecimalType(fromTag)) {
            if (fromType == toType) {
                return DecimalColumn.newInstance(i, fromType);
            }
            return CastDecimalToDecimalFunctionFactory.newInstance(0, new DecimalColumn(i, fromType), toType, executionContext);
        }
        return switch (fromTag) {
            case INT ->
                    CastIntToDecimalFunctionFactory.newInstance(0, IntColumn.newInstance(i), toType, executionContext);
            case SHORT ->
                    CastShortToDecimalFunctionFactory.newInstance(0, ShortColumn.newInstance(i), toType, executionContext);
            case LONG ->
                    CastLongToDecimalFunctionFactory.newInstance(0, LongColumn.newInstance(i), toType, executionContext.getDecimal256());
            case BYTE ->
                    CastByteToDecimalFunctionFactory.newInstance(0, ByteColumn.newInstance(i), toType, executionContext);
            case STRING ->
                    CastStrToDecimalFunctionFactory.newInstance(executionContext.getDecimal256(), 0, toType, new StrColumn(i));
            case VARCHAR ->
                    CastVarcharToDecimalFunctionFactory.newInstance(executionContext.getDecimal256(), 0, toType, new VarcharColumn(i));
            default -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
        };
    }

    private static Function castToGeoHash(
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toTag,
            int toType,
            int modelPosition
    ) throws SqlException {
        switch (fromTag) {
            case STRING:
                return CastStrToGeoHashFunctionFactory.newInstance(0, toType, new StrColumn(i));
            case VARCHAR:
                return CastVarcharToGeoHashFunctionFactory.newInstance(0, toType, new VarcharColumn(i));
            case GEOBYTE:
            case GEOSHORT:
            case GEOINT:
            case GEOLONG:
                if (fromTag == toTag) {
                    return geoHashColumn(fromTag, i, toTag == GEOSHORT ? toType : fromType);
                }
                if (fromTag > toTag) {
                    return CastGeoHashToGeoHashFunctionFactory.newInstance(0, geoHashColumn(fromTag, i, fromType), toType, fromType);
                }
                // fall through
            default:
                throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
        }
    }

    private static Function castToString(
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toType,
            int modelPosition
    ) throws SqlException {
        return switch (fromTag) {
            case BOOLEAN -> BooleanColumn.newInstance(i);
            case BYTE -> new CastByteToStrFunctionFactory.Func(ByteColumn.newInstance(i));
            case SHORT -> new CastShortToStrFunctionFactory.Func(ShortColumn.newInstance(i));
            // CharFunction has built-in cast to String
            case CHAR -> new CharColumn(i);
            case INT -> new CastIntToStrFunctionFactory.Func(IntColumn.newInstance(i));
            case LONG -> new CastLongToStrFunctionFactory.Func(LongColumn.newInstance(i));
            case DATE -> new CastDateToStrFunctionFactory.Func(DateColumn.newInstance(i));
            case TIMESTAMP -> new CastTimestampToStrFunctionFactory.Func(TimestampColumn.newInstance(i, fromType));
            case FLOAT -> new CastFloatToStrFunctionFactory.Func(FloatColumn.newInstance(i));
            case DOUBLE -> new CastDoubleToStrFunctionFactory.Func(DoubleColumn.newInstance(i));
            case STRING -> new StrColumn(i);
            // VarcharFunction has built-in cast to string
            case VARCHAR -> new VarcharColumn(i);
            case UUID -> new CastUuidToStrFunctionFactory.Func(UuidColumn.newInstance(i));
            case SYMBOL ->
                    new CastSymbolToStrFunctionFactory.Func(new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i)));
            case LONG256 -> new CastLong256ToStrFunctionFactory.Func(Long256Column.newInstance(i));
            case GEOBYTE ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoByteToStrCastFunction(GeoByteColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case GEOSHORT ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoShortToStrCastFunction(GeoShortColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case GEOINT ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoIntToStrCastFunction(GeoIntColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case GEOLONG ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoLongToStrCastFunction(GeoLongColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64 ->
                    new CastDecimalToStrFunctionFactory.Func64(Decimal64LoaderFunctionFactory.getInstance(DecimalColumn.newInstance(i, fromType)));
            case DECIMAL128 -> new CastDecimalToStrFunctionFactory.Func128(DecimalColumn.newInstance(i, fromType));
            case DECIMAL256 -> new CastDecimalToStrFunctionFactory.Func(DecimalColumn.newInstance(i, fromType));
            case INTERVAL -> new CastIntervalToStrFunctionFactory.Func(IntervalColumn.newInstance(i, fromType));
            case BINARY -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
            case ARRAY -> {
                if (decodeArrayElementType(fromType) != DOUBLE) {
                    throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
                }
                yield new CastDoubleArrayToStrFunctionFactory.Func(ArrayColumn.newInstance(i, fromType));
            }
            case IPv4 -> new CastIPv4ToStrFunctionFactory.Func(IPv4Column.newInstance(i));
            default -> null;
        };
    }

    private static Function castToTimestamp(
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toType,
            int modelPosition
    ) throws SqlException {
        return switch (fromTag) {
            case DATE -> new CastDateToTimestampFunctionFactory.Func(DateColumn.newInstance(i), toType);
            case TIMESTAMP -> fromType == toType
                    ? TimestampColumn.newInstance(i, fromType)
                    : new CastTimestampToTimestampFunctionFactory.Func(TimestampColumn.newInstance(i, fromType), fromType, toType);
            default -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
        };
    }

    private static Function castToVarchar(
            RecordMetadata castFromMetadata,
            int i,
            int fromTag,
            int fromType,
            int toType,
            int modelPosition
    ) throws SqlException {
        return switch (fromTag) {
            case BOOLEAN -> BooleanColumn.newInstance(i);
            case BYTE -> new CastByteToVarcharFunctionFactory.Func(ByteColumn.newInstance(i));
            case SHORT -> new CastShortToVarcharFunctionFactory.Func(ShortColumn.newInstance(i));
            // CharFunction has built-in cast to varchar
            case CHAR -> new CharColumn(i);
            case INT -> new CastIntToVarcharFunctionFactory.Func(IntColumn.newInstance(i));
            case LONG -> new CastLongToVarcharFunctionFactory.Func(LongColumn.newInstance(i));
            case DATE -> new CastDateToVarcharFunctionFactory.Func(DateColumn.newInstance(i));
            case TIMESTAMP ->
                    new CastTimestampToVarcharFunctionFactory.Func(TimestampColumn.newInstance(i, fromType), fromType);
            case FLOAT -> new CastFloatToVarcharFunctionFactory.Func(FloatColumn.newInstance(i));
            case DOUBLE -> new CastDoubleToVarcharFunctionFactory.Func(DoubleColumn.newInstance(i));
            // StrFunction has built-in cast to varchar
            case STRING -> new StrColumn(i);
            case VARCHAR -> new VarcharColumn(i);
            case UUID -> new CastUuidToVarcharFunctionFactory.Func(UuidColumn.newInstance(i));
            case IPv4 -> new CastIPv4ToVarcharFunctionFactory.Func(IPv4Column.newInstance(i));
            case SYMBOL ->
                    new CastSymbolToVarcharFunctionFactory.Func(new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i)));
            case LONG256 -> new CastLong256ToVarcharFunctionFactory.Func(Long256Column.newInstance(i));
            case GEOBYTE ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoByteToVarcharCastFunction(GeoByteColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case GEOSHORT ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoShortToVarcharCastFunction(GeoShortColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case GEOINT ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoIntToVarcharCastFunction(GeoIntColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case GEOLONG ->
                    CastGeoHashToGeoHashFunctionFactory.getGeoLongToVarcharCastFunction(GeoLongColumn.newInstance(i, fromType), getGeoHashBits(fromType));
            case BINARY -> throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
            case ARRAY -> {
                if (decodeArrayElementType(fromType) != DOUBLE) {
                    throw unsupportedCast(modelPosition, castFromMetadata, i, fromType, toType);
                }
                yield new CastDoubleArrayToVarcharFunctionFactory.Func(ArrayColumn.newInstance(i, fromType));
            }
            default -> {
                assert false;
                yield null;
            }
        };
    }

    private static ObjList<Function> generateCastFunctions(
            SqlExecutionContext executionContext,
            RecordMetadata castToMetadata,
            RecordMetadata castFromMetadata,
            int modelPosition
    ) throws SqlException {
        final ObjList<Function> castFunctions = new ObjList<>();
        try {
            for (int i = 0, n = castToMetadata.getColumnCount(); i < n; i++) {
                final int toType = castToMetadata.getColumnType(i);
                final int fromType = castFromMetadata.getColumnType(i);
                int fromTag = tagOf(fromType);
                // VARCHAR_SLICE is a transient in-memory type (from read_parquet) accessed
                // through the same getVarcharA() interface as VARCHAR. Normalize it so that
                // the cast switch handles it identically to VARCHAR.
                if (fromTag == VARCHAR_SLICE) {
                    fromTag = VARCHAR;
                }
                if (fromTag == NULL) {
                    castFunctions.add(NullConstant.NULL);
                } else {
                    final Function function = castColumn(executionContext, castFromMetadata, i, fromTag, fromType, tagOf(toType), toType, modelPosition);
                    if (function != null) {
                        castFunctions.add(function);
                    }
                }
            }
            return castFunctions;
        } catch (Throwable th) {
            Misc.freeObjList(castFunctions, th);
            throw th;
        }
    }

    private static Function geoHashColumn(int tag, int i, int type) {
        return switch (tag) {
            case GEOBYTE -> GeoByteColumn.newInstance(i, type);
            case GEOSHORT -> GeoShortColumn.newInstance(i, type);
            case GEOINT -> GeoIntColumn.newInstance(i, type);
            default -> GeoLongColumn.newInstance(i, type);
        };
    }

    /**
     * Every UNION ALL branch has a designated timestamp, and the first one is the requested order column.
     */
    private static boolean isTimestampOrderPushable(SetOperationPlan operation, int orderIndex) {
        LogicalPlan plan = operation;
        while (plan instanceof SetOperationPlan union && union.getOperation() == SetOperationKind.UNION_ALL) {
            if (union.getRight().getOutput().getTimestampIndex() < 0) {
                return false;
            }
            plan = union.getLeft();
        }
        return plan.getOutput().getTimestampIndex() == orderIndex;
    }

    private static Function numericColumn(int tag, int i) {
        return switch (tag) {
            case BYTE -> ByteColumn.newInstance(i);
            case SHORT -> ShortColumn.newInstance(i);
            case CHAR -> new CharColumn(i);
            case INT -> IntColumn.newInstance(i);
            case LONG -> LongColumn.newInstance(i);
            case FLOAT -> FloatColumn.newInstance(i);
            default -> DoubleColumn.newInstance(i);
        };
    }

    private static SqlException unsupportedCast(int position, RecordMetadata castFromMetadata, int index, int fromType, int toType) {
        return SqlException.unsupportedCast(position, castFromMetadata.getColumnName(index), fromType, toType);
    }

    private ObjList<Function> generateMergeCastFunctions(RecordMetadata toMetadata, RecordMetadata fromMetadata, int position) throws SqlException {
        return generateCastFunctions(mergeCastContext, toMetadata, fromMetadata, position);
    }

    /**
     * Consumes both input factories on entry, including every failure path. The requested
     * ordering is advice; actual input metadata and directions must prove it.
     */
    private RecordCursorFactory generateOperation(
            SetOperationPlan plan,
            RecordCursorFactory left,
            RecordCursorFactory right,
            SqlExecutionContext executionContext,
            int orderByIndex,
            int scanDirection
    ) throws SqlException {
        ObjList<Function> leftCasts = null;
        ObjList<Function> rightCasts = null;
        boolean isTransferred = false;
        try {
            final RecordMetadata leftMetadata = left.getMetadata();
            final RecordMetadata rightMetadata = right.getMetadata();
            final boolean isMerge = plan.getOperation() == SetOperationKind.UNION_ALL
                    && canMergeUnionAll(left, right, orderByIndex, scanDirection);
            final boolean castRequired = SetOperationBinder.isCastRequired(plan);
            final GenericRecordMetadata metadata;
            if (castRequired) {
                metadata = new GenericRecordMetadata();
                for (int i = 0, n = leftMetadata.getColumnCount(); i < n; i++) {
                    metadata.add(new TableColumnMetadata(
                            Chars.toString(plan.getOutput().getColumnName(i)),
                            SetOperationBinder.getUnionCastType(leftMetadata.getColumnType(i), rightMetadata.getColumnType(i))
                    ));
                }
                leftCasts = generateCastFunctions(executionContext, metadata, leftMetadata, plan.getPosition());
                rightCasts = generateCastFunctions(executionContext, metadata, rightMetadata, plan.getRightPosition());
            } else {
                metadata = GenericRecordMetadata.copyOfNew(leftMetadata);
                if (plan.getOperation().isUnion()) {
                    metadata.setTimestampIndex(-1);
                }
            }

            final IntList symbolColumns;
            if (isMerge) {
                metadata.setTimestampIndex(orderByIndex);
                // The merge retains this list after the compiler's plan pool is reset.
                symbolColumns = new IntList(plan.getSymbolColumns());
            } else {
                symbolColumns = null;
            }
            isTransferred = true;
            final RecordCursorFactory result = generateSetOperation(
                    plan.getOperation(), metadata, left, right, leftCasts, rightCasts,
                    isMerge, plan.getPosition(), plan.getRightPosition(), symbolColumns, executionContext
            );
            if (plan.getOperation().isUnion() && plan.isSymbolRestorationRequired()) {
                // This helper consumes the completed union on success and on failure.
                return maybeResymboliseUnion(result, plan.getSymbolColumns());
            }
            return result;
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(left, th);
                if (right != left) {
                    Misc.free(right, th);
                }
                Misc.freeObjList(leftCasts, th);
                Misc.freeObjList(rightCasts, th);
            }
            throw th;
        }
    }

    /**
     * Consumes both factories and cast lists on entry. Metadata and symbol columns outlive the compiler.
     */
    private RecordCursorFactory generateSetOperation(
            SetOperationKind operation,
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            boolean isMerge,
            int positionA,
            int positionB,
            @Nullable IntList symbolUnionColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        boolean isTransferred = false;
        try {
            if (operation == SetOperationKind.UNION_ALL) {
                if (isMerge) {
                    final boolean isAscending = factoryA.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD;
                    isTransferred = true;
                    mergeCastContext = executionContext;
                    try {
                        return MergeUnionAllRecordCursorFactoryBuilder.build(
                                metadata, factoryA, positionA, factoryB, positionB,
                                castFunctionsA, castFunctionsB, isAscending, symbolUnionColumns, mergeCastFactory
                        );
                    } finally {
                        mergeCastContext = null;
                    }
                }
                isTransferred = true;
                return new UnionAllRecordCursorFactory(metadata, factoryA, factoryB, castFunctionsA, castFunctionsB);
            }

            final SetRecordCursorFactoryConstructor constructor = switch (operation) {
                case UNION -> SET_UNION_CONSTRUCTOR;
                case EXCEPT -> SET_EXCEPT_CONSTRUCTOR;
                case EXCEPT_ALL -> SET_EXCEPT_ALL_CONSTRUCTOR;
                case INTERSECT -> SET_INTERSECT_CONSTRUCTOR;
                case INTERSECT_ALL -> SET_INTERSECT_ALL_CONSTRUCTOR;
                case UNION_ALL -> throw new IllegalArgumentException("set operation: " + operation);
            };
            keyTypes.clear();
            valueTypes.clear();
            writeSymbolAsString.clear();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final int type = metadata.getColumnType(i);
                if (isSymbol(type)) {
                    keyTypes.add(STRING);
                    writeSymbolAsString.set(i);
                } else {
                    keyTypes.add(type);
                }
            }
            entityColumnFilter.of(metadata.getColumnCount());
            final RecordSink recordSink = RecordSinkFactory.getInstance(
                    configuration, asm, metadata, entityColumnFilter, writeSymbolAsString
            );
            isTransferred = true;
            return constructor.create(
                    configuration, metadata, factoryA, factoryB, castFunctionsA, castFunctionsB,
                    recordSink, keyTypes, valueTypes
            );
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(factoryA, th);
                if (factoryB != factoryA) {
                    Misc.free(factoryB, th);
                }
                Misc.freeObjList(castFunctionsA, th);
                if (castFunctionsB != castFunctionsA) {
                    Misc.freeObjList(castFunctionsB, th);
                }
            }
            throw th;
        }
    }

    /**
     * Sorts a later UNION ALL branch that neither follows the requested timestamp order nor has its own ORDER BY.
     */
    private int sortUnionBranch(GenerationFrame frame, int branchSlot, LogicalPlan branch, int orderIndex, int direction) throws SqlException {
        final RecordCursorFactory base = frame.resources.factory(branchSlot);
        if (LogicalPlans.skipProjects(branch) instanceof SortPlan || base.getMetadata().getTimestampIndex() != orderIndex
                || base.getScanDirection() == direction) {
            return branchSlot;
        }
        branchSortKeys.clear();
        branchSortKeys.add(direction == RecordCursorFactory.SCAN_DIRECTION_BACKWARD ? -orderIndex - 1 : orderIndex + 1);
        final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(base.getMetadata());
        final int slot = frame.resources.reserve();
        frame.resources.detach(branchSlot);
        frame.resources.own(slot, sortGenerator.generateSort(metadata, base, branchSortKeys, null, null, -1));
        return slot;
    }

    // Casts back to SYMBOL every union result column that was SYMBOL on all branches (tracked in
    // symbolUnionColumns) and that the chain downcast to STRING (see getUnionCastType). The cast
    // sits outside the union, so it builds one dictionary over the merged stream instead of trying
    // to reconcile the per-branch dictionaries the wire cannot merge.
    // Columns that are not re-symbolised pass through unchanged; maybeResymboliseUnion returns the
    // union factory as-is when there is nothing to re-symbolise.
    static RecordCursorFactory maybeResymboliseUnion(
            RecordCursorFactory unionFactory,
            @Nullable IntList symbolUnionColumns
    ) {
        if (symbolUnionColumns == null || symbolUnionColumns.size() == 0) {
            return unionFactory;
        }
        final RecordMetadata baseMetadata = unionFactory.getMetadata();
        final int columnCount = baseMetadata.getColumnCount();
        // Own unionFactory from here on: the guard assert and the metadata/list allocations below can
        // all throw (an OutOfMemoryError, say), and for a distinct UNION unionFactory already holds a
        // native OrderedMap, so the catch must free it on every failure path, not just a build-loop throw.
        ObjList<Function> functions = null;
        try {
            // The re-symbolising CastStrToSymbol function builds its dictionary lazily and is not
            // thread-safe (Func.isThreadSafe() == false). That is safe only because a union base is
            // serial: it supports neither page frames nor time frames, so no parallel
            // operator (async filter, parallel GROUP BY) ever clones or snapshots this projection.
            // Enforce the invariant unconditionally rather than with an assert: -ea strips asserts in
            // production, and a future page-frame-capable union must fail loudly here instead of shipping
            // a stale, empty dictionary snapshot to a worker.
            if (unionFactory.supportsPageFrameCursor() || unionFactory.supportsTimeFrameCursor()) {
                throw CairoException.critical(0).put("union symbol projection requires a serial base cursor");
            }
            final GenericRecordMetadata virtualMetadata = new GenericRecordMetadata();
            final IntList columnToFunctionIndex = new IntList(columnCount);
            functions = new ObjList<>();
            int symbolColumnIndex = 0;
            int nextSymbolColumn = symbolUnionColumns.getQuick(0);
            for (int i = 0; i < columnCount; i++) {
                final String columnName = baseMetadata.getColumnName(i);
                final boolean isSymbolCastRequired = i == nextSymbolColumn;
                if (isSymbolCastRequired) {
                    assert tagOf(baseMetadata.getColumnType(i)) == STRING;
                    nextSymbolColumn = ++symbolColumnIndex < symbolUnionColumns.size()
                            ? symbolUnionColumns.getQuick(symbolColumnIndex)
                            : -1;
                    // Register baseColumn before wrapping it: the wrapper construction can throw, and the
                    // catch can only free objects already owned by this list. Once the symbol function is
                    // built it owns baseColumn, so replace the slot to avoid a double close. Only symbol
                    // columns enter this list; all other getters delegate directly to the union record in
                    // UnionSymbolCastRecordCursorFactory.
                    final int functionIndex = functions.size();
                    final Function baseColumn = new StrColumn(i);
                    functions.add(baseColumn);
                    final Function function = new CastStrToSymbolFunctionFactory.Func(baseColumn);
                    functions.setQuick(functionIndex, function);
                    // A cast-to-symbol builds its dictionary lazily, so its symbol table is not static.
                    virtualMetadata.add(new TableColumnMetadata(
                            columnName,
                            SYMBOL,
                            IndexType.NONE,
                            0,
                            false,
                            function.getMetadata()
                    ));
                    columnToFunctionIndex.add(functionIndex);
                } else {
                    virtualMetadata.add(baseMetadata.getColumnMetadata(i));
                    columnToFunctionIndex.add(-1);
                }
            }
            virtualMetadata.setTimestampIndex(baseMetadata.getTimestampIndex());
            return new UnionSymbolCastRecordCursorFactory(
                    virtualMetadata,
                    unionFactory,
                    columnToFunctionIndex,
                    functions
            );
        } catch (Throwable e) {
            Misc.freeObjList(functions);
            Misc.free(unionFactory);
            throw e;
        }
    }

    int generate(
            GenerationFrame frame, SetOperationPlan operation, int requiredOrderColumnId, int requiredScanDirection,
            int orderByMnemonic, SqlExecutionContext executionContext
    ) throws SqlException {
        final int orderIndex = operation.getOperation() == SetOperationKind.UNION_ALL
                ? operation.getOutput().getColumnIndexById(requiredOrderColumnId) : -1;
        final int leftOrderId = orderIndex < 0 ? -1 : operation.getLeft().getOutput().getColumnId(orderIndex);
        final int rightOrderId = orderIndex < 0 ? -1 : operation.getRight().getOutput().getColumnId(orderIndex);
        final int leftSlot = codeGenerator.generate(frame, operation.getLeft(), executionContext, leftOrderId, requiredScanDirection, null, null, orderByMnemonic);
        final LogicalPlan leftBranch = SqlCodeGenerator.unwrapColumnProjections(operation.getLeft());
        final int leftHead = leftBranch instanceof SetOperationPlan ? frame.setOperationPlans.indexOf(leftBranch) : -1;
        frame.setOperationPlans.add(operation);
        frame.setOperationHeads.add(leftHead >= 0 ? frame.setOperationHeads.getQuick(leftHead) : frame.resources.factory(leftSlot));
        int rightSlot = codeGenerator.generate(frame, operation.getRight(), executionContext, rightOrderId, requiredScanDirection, null, null, orderByMnemonic);
        if (orderIndex >= 0 && requiredScanDirection != RecordCursorFactory.SCAN_DIRECTION_OTHER
                && isTimestampOrderPushable(operation, orderIndex)) {
            rightSlot = sortUnionBranch(frame, rightSlot, operation.getRight(), orderIndex, requiredScanDirection);
        }
        final int slot = frame.resources.reserve();
        final RecordCursorFactory left = frame.resources.detachFactory(leftSlot);
        final RecordCursorFactory right = frame.resources.detachFactory(rightSlot);
        frame.resources.own(slot, generateOperation(operation, left, right, executionContext, orderIndex, requiredScanDirection));
        return slot;
    }
}
