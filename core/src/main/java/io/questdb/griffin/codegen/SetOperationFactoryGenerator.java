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

package io.questdb.griffin.codegen;

import io.questdb.ParanoiaState;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
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
import io.questdb.griffin.SetOperationCasts;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
import io.questdb.griffin.plan.logical.SortDirection;
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
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final SortFactoryGenerator sortGenerator;
    private SqlExecutionContext mergeCastContext;
    private final MergeUnionAllRecordCursorFactoryBuilder.CastFunctionFactory mergeCastFactory = this::generateMergeCastFunctions;

    SetOperationFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            SortFactoryGenerator sortGenerator,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.sortGenerator = sortGenerator;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
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
                default -> throw unvalidatedCast();
            };
            case FLOAT -> switch (fromTag) {
                case BYTE, SHORT, INT, LONG, FLOAT -> numericColumn(fromTag, i);
                default -> throw unvalidatedCast();
            };
            case DOUBLE -> switch (fromTag) {
                case BYTE, SHORT, INT, LONG, FLOAT, DOUBLE -> numericColumn(fromTag, i);
                default -> throw unvalidatedCast();
            };
            case IPv4 -> {
                if (fromTag != IPv4) {
                    throw unvalidatedCast();
                }
                yield IPv4Column.newInstance(i);
            }
            case DATE -> {
                if (fromTag != DATE) {
                    throw unvalidatedCast();
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
            case TIMESTAMP -> castToTimestamp(i, fromTag, fromType, toType);
            case STRING -> castToString(castFromMetadata, i, fromTag, fromType);
            case SYMBOL ->
                    new CastSymbolToStrFunctionFactory.Func(new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i)));
            case LONG256 -> Long256Column.newInstance(i);
            case GEOBYTE, GEOSHORT, GEOINT, GEOLONG -> castToGeoHash(i, fromTag, fromType, toTag, toType);
            case DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 ->
                    castToDecimal(executionContext, i, fromTag, fromType, toType);
            case BINARY -> BinColumn.newInstance(i);
            case VARCHAR -> castToVarchar(castFromMetadata, i, fromTag, fromType);
            case INTERVAL -> IntervalColumn.newInstance(i, toType);
            case ARRAY -> castToArray(i, fromTag, fromType, toType, modelPosition);
            default -> {
                assert false;
                yield null;
            }
        };
    }

    private static Function castToArray(int i, int fromTag, int fromType, int toType, int modelPosition) {
        switch (fromTag) {
            case ARRAY: {
                assert decodeArrayElementType(fromType) == DOUBLE;
                assert decodeArrayElementType(toType) == DOUBLE;
                final int fromDims = decodeWeakArrayDimensionality(fromType);
                final int toDims = decodeWeakArrayDimensionality(toType);
                assert toDims != -1 && fromDims <= toDims;
                if (fromDims == toDims) {
                    return ArrayColumn.newInstance(i, fromType);
                }
                if (fromDims == -1) {
                    // must be a bind variable, i.e. weak dimensionality case
                    return new CastDoubleArrayToDoubleArrayFunctionFactory.WeakDimsFunc(ArrayColumn.newInstance(i, fromType), toType, modelPosition);
                }
                return new CastDoubleArrayToDoubleArrayFunctionFactory.Func(ArrayColumn.newInstance(i, fromType), toType, toDims - fromDims);
            }
            case DOUBLE:
                assert decodeArrayElementType(toType) == DOUBLE && decodeWeakArrayDimensionality(toType) != -1;
                return new CastDoubleToDoubleArray.Func(DoubleColumn.newInstance(i), toType);
            default:
                assert false;
                return null;
        }
    }

    private static Function castToDecimal(SqlExecutionContext executionContext, int i, int fromTag, int fromType, int toType) throws SqlException {
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
            default -> throw unvalidatedCast();
        };
    }

    private static Function castToGeoHash(int i, int fromTag, int fromType, int toTag, int toType) throws SqlException {
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
                throw unvalidatedCast();
        }
    }

    private static Function castToString(RecordMetadata castFromMetadata, int i, int fromTag, int fromType) {
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
            case BINARY -> throw unvalidatedCast();
            case ARRAY -> {
                if (decodeArrayElementType(fromType) != DOUBLE) {
                    throw unvalidatedCast();
                }
                yield new CastDoubleArrayToStrFunctionFactory.Func(ArrayColumn.newInstance(i, fromType));
            }
            case IPv4 -> new CastIPv4ToStrFunctionFactory.Func(IPv4Column.newInstance(i));
            default -> null;
        };
    }

    private static Function castToTimestamp(int i, int fromTag, int fromType, int toType) {
        return switch (fromTag) {
            case DATE -> new CastDateToTimestampFunctionFactory.Func(DateColumn.newInstance(i), toType);
            case TIMESTAMP -> fromType == toType
                    ? TimestampColumn.newInstance(i, fromType)
                    : new CastTimestampToTimestampFunctionFactory.Func(TimestampColumn.newInstance(i, fromType), fromType, toType);
            default -> throw unvalidatedCast();
        };
    }

    private static Function castToVarchar(RecordMetadata castFromMetadata, int i, int fromTag, int fromType) {
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
            case BINARY -> throw unvalidatedCast();
            case ARRAY -> {
                if (decodeArrayElementType(fromType) != DOUBLE) {
                    throw unvalidatedCast();
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

    private static IllegalStateException unvalidatedCast() {
        return new IllegalStateException("set operation cast is not validated");
    }

    private ObjList<Function> generateMergeCastFunctions(RecordMetadata toMetadata, RecordMetadata fromMetadata, int position) throws SqlException {
        for (int i = 0, n = toMetadata.getColumnCount(); i < n; i++) {
            SetOperationCasts.validateCast(fromMetadata.getColumnType(i), toMetadata.getColumnType(i), fromMetadata.getColumnName(i), position);
        }
        return generateCastFunctions(mergeCastContext, toMetadata, fromMetadata, position);
    }

    /**
     * Consumes both input factories on entry, including every failure path.
     */
    private RecordCursorFactory generateOperation(
            GenerationFrame frame,
            SetOperationPlan plan,
            RecordCursorFactory left,
            RecordCursorFactory right,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ObjList<Function> leftCasts = null;
        ObjList<Function> rightCasts = null;
        boolean isTransferred = false;
        try {
            final RecordMetadata leftMetadata = left.getMetadata();
            final RecordMetadata rightMetadata = right.getMetadata();
            final boolean castRequired = SetOperationCasts.isCastRequired(plan);
            final GenericRecordMetadata metadata;
            if (castRequired) {
                metadata = new GenericRecordMetadata();
                for (int i = 0, n = leftMetadata.getColumnCount(); i < n; i++) {
                    metadata.add(new TableColumnMetadata(
                            Chars.toString(plan.getOutput().getColumnName(i)),
                            SetOperationCasts.getUnionCastType(leftMetadata.getColumnType(i), rightMetadata.getColumnType(i))
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
            if (plan.isMerged()) {
                metadata.setTimestampIndex(plan.getOutput().getColumnIndexById(plan.getRequestedOrderColumnId()));
                // The merge retains this list after the compiler's plan pool is reset.
                symbolColumns = new IntList(plan.getSymbolColumns());
            } else {
                symbolColumns = null;
            }
            isTransferred = true;
            final RecordCursorFactory result = generateSetOperation(
                    frame, plan.getOperation(), metadata, left, right, leftCasts, rightCasts,
                    plan.isMerged() ? plan.getRequestedOrderDirection() : null, plan.getPosition(), plan.getRightPosition(), symbolColumns, executionContext
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
     * Consumes both factories and cast lists on entry; a UNION ALL merges its branches in {@code mergeDirection} unless
     * it is null. Metadata and symbol columns outlive the compiler.
     */
    private RecordCursorFactory generateSetOperation(
            GenerationFrame frame,
            SetOperationKind operation,
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            @Nullable SortDirection mergeDirection,
            int positionA,
            int positionB,
            @Nullable IntList symbolUnionColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        boolean isTransferred = false;
        try {
            if (operation == SetOperationKind.UNION_ALL) {
                if (mergeDirection != null) {
                    isTransferred = true;
                    mergeCastContext = executionContext;
                    try {
                        return MergeUnionAllRecordCursorFactoryBuilder.build(
                                metadata, factoryA, positionA, factoryB, positionB,
                                castFunctionsA, castFunctionsB, mergeDirection == SortDirection.ASCENDING, symbolUnionColumns, mergeCastFactory
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
            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ArrayColumnTypes valueTypes = frame.valueTypes;
            final BitSet writeSymbolAsString = frame.writeSymbolAsString;
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
     * Sorts a later UNION ALL branch into the requested timestamp order of the merge.
     */
    private RecordCursorFactory sortUnionBranch(GenerationFrame frame, RecordCursorFactory branch, SortPlan.Algorithm algorithm, int orderIndex,
                                                SortDirection direction) throws SqlException {
        final ListColumnFilter branchSortKeys = frame.listColumnFilterB;
        final GenericRecordMetadata metadata;
        try {
            branchSortKeys.clear();
            branchSortKeys.add(direction == SortDirection.DESCENDING ? -orderIndex - 1 : orderIndex + 1);
            metadata = GenericRecordMetadata.copyOfNew(branch.getMetadata());
        } catch (Throwable th) {
            Misc.free(branch, th);
            throw th;
        }
        return sortGenerator.generateSort(metadata, branch, branchSortKeys, null, null, -1, algorithm == SortPlan.Algorithm.MATERIALIZED);
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
        final GenericRecordMetadata virtualMetadata;
        final IntList columnToFunctionIndex;
        try {
            // The re-symbolising CastStrToSymbol function builds its dictionary lazily and is not
            // thread-safe (Func.isThreadSafe() == false). That is safe only because a union base is
            // serial: it supports neither page frames nor time frames.
            if (ParanoiaState.PLAN_PARANOIA_MODE && (unionFactory.supportsPageFrameCursor() || unionFactory.supportsTimeFrameCursor())) {
                throw new AssertionError("union symbol projection requires a serial base cursor");
            }
            virtualMetadata = new GenericRecordMetadata();
            columnToFunctionIndex = new IntList(columnCount);
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
        } catch (Throwable e) {
            Misc.freeObjList(functions, e);
            Misc.free(unionFactory, e);
            throw e;
        }
        return new UnionSymbolCastRecordCursorFactory(
                virtualMetadata,
                unionFactory,
                columnToFunctionIndex,
                functions
        );
    }

    RecordCursorFactory generate(GenerationFrame frame, SetOperationPlan operation, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory left = codeGenerator.generate(frame, operation.getLeft(), executionContext);
        RecordCursorFactory right;
        try {
            final LogicalPlan leftBranch = SqlCodeGenerator.unwrapColumnProjections(operation.getLeft());
            final int leftHead = leftBranch instanceof SetOperationPlan ? frame.setOperationPlans.indexOf(leftBranch) : -1;
            frame.setOperationPlans.add(operation);
            frame.setOperationHeads.add(leftHead >= 0 ? frame.setOperationHeads.getQuick(leftHead) : left);
            right = codeGenerator.generate(frame, operation.getRight(), executionContext);
            if (operation.getRightBranchSort() != null) {
                right = sortUnionBranch(frame, right, operation.getRightBranchSort(),
                        operation.getOutput().getColumnIndexById(operation.getRequestedOrderColumnId()), operation.getRequestedOrderDirection());
            }
        } catch (Throwable th) {
            Misc.free(left, th);
            throw th;
        }
        return generateOperation(frame, operation, left, right, executionContext);
    }
}
