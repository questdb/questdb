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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.RelationRules;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.Decimals;
import io.questdb.std.FiberLocal;
import io.questdb.std.IntList;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.ColumnType.*;

public class CaseCommon {
    private static final Cast[] NO_CASTS = {};
    private static final LongObjHashMap<FunctionFactory> castFactories = new LongObjHashMap<>();
    private static final ObjList<CaseFunctionConstructor> constructors = new ObjList<>(MAX_TAG + 1);
    private static final FiberLocal<IntList> tlArgPositions = new FiberLocal<>(IntList::new);
    private static final FiberLocal<ObjList<Function>> tlArgs = new FiberLocal<>(ObjList::new);
    private static final LongIntHashMap typeEscalationMap = new LongIntHashMap();

    /**
     * The cast factory {@link #getCastFunction} wraps an argument of {@code fromType} with to
     * read it as {@code toType}; null when the argument is handed back as is. Keyed by encoded
     * type: a TIMESTAMP_NANO argument has no entries. {@code TypeRelationGoldenTest} pins the table.
     */
    @TestOnly
    public static FunctionFactory getCastFactory(int fromType, int toType) {
        return castFactories.get(Numbers.encodeLowHighInts(fromType, toType));
    }

    // public for testing
    @TestOnly
    public static Function getCastFunction(
            Function arg,
            int argPosition,
            int toType,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int argType = arg.getType();
        if (isNull(argType)) {
            return Constants.getNullConstant(toType);
        }
        if (ColumnType.isArray(argType)) {
            assert argType == toType; // no type escalation for arrays
            return arg;
        }
        if (ColumnType.isDecimal(toType)) {
            return DecimalUtil.getImplicitCastFunction(arg, argPosition, toType, sqlExecutionContext);
        }
        final int keyIndex = castFactories.keyIndex(Numbers.encodeLowHighInts(argType, toType));
        if (keyIndex < 0) {
            FunctionFactory factory = castFactories.valueAt(keyIndex);
            ObjList<Function> args = tlArgs.get();
            args.clear();
            args.add(arg);

            IntList argPositions = tlArgPositions.get();
            argPositions.clear();
            argPositions.add(argPosition);
            return factory.newInstance(0, args, argPositions, configuration, sqlExecutionContext);
        }

        return arg;
    }

    // public for testing
    @TestOnly
    public static int getCommonType(int commonType, int valueType, int valuePos, String undefinedErrorMsg) throws SqlException {
        if (isUndefined(valueType)) {
            throw SqlException.$(valuePos, undefinedErrorMsg);
        }

        if (commonType == -1 || isNull(commonType) || commonType == 0) {
            return valueType;
        }
        if (isNull(valueType)) {
            return commonType;
        }

        boolean arrayCommonType = ColumnType.isArray(commonType);
        boolean arrayValueType = ColumnType.isArray(valueType);
        if (arrayCommonType && arrayValueType) {
            if (commonType == valueType) {
                return commonType;
            }
            throw SqlException.inconvertibleTypes(valuePos, valueType, ColumnType.nameOf(valueType), commonType, ColumnType.nameOf(commonType));
        }

        if (ColumnType.isDecimal(commonType) || ColumnType.isDecimal(valueType)) {
            return getDecimalCommonType(commonType, valueType, valuePos);
        }

        final int type = typeEscalationMap.get(Numbers.encodeLowHighInts(commonType, valueType));
        if (type == LongIntHashMap.NO_ENTRY_VALUE) {
            throw SqlException.inconvertibleTypes(valuePos, valueType, ColumnType.nameOf(valueType), commonType, ColumnType.nameOf(commonType));
        }
        return type;
    }

    private static void addRows(int fromType) {
        // pairs of (next branch type, common type) from RelationRules.caseEscalation()
        final int[] escalation = RelationRules.caseEscalation(fromType);
        for (int i = 0, n = escalation.length; i < n; i += 2) {
            typeEscalationMap.put(Numbers.encodeLowHighInts(fromType, escalation[i]), escalation[i + 1]);
        }
        for (Cast cast : castRow(fromType)) {
            castFactories.put(Numbers.encodeLowHighInts(fromType, cast.toType), cast.factory);
        }
    }

    private static Cast cast(int toType, FunctionFactory factory) {
        return new Cast(toType, factory);
    }

    /**
     * The cast factories that make a branch of {@code fromType} read as the common type. A target
     * missing from the row needs no cast: the branch's own getter converts, or the pair never
     * escalates (see {@link RelationRules#caseEscalation}). Rows are keyed by encoded type: of the
     * two TIMESTAMP encodings only TIMESTAMP_MICRO has casts, and TIMESTAMP_NANO is neither a
     * source nor a target. Decimal and array types bypass the table.
     * {@code TypeRelationGoldenTest.testCaseCastFactory} pins the rows.
     */
    private static Cast[] castRow(int fromType) {
        return switch (ColumnTypeTag.of(fromType)) {
            case BOOLEAN -> casts(cast(LONG256, new CastBooleanToLong256FunctionFactory()));
            case BYTE -> casts(
                    cast(LONG256, new CastByteToLong256FunctionFactory()),
                    cast(STRING, new CastByteToStrFunctionFactory()),
                    cast(VARCHAR, new CastByteToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastByteToSymbolFunctionFactory()),
                    cast(CHAR, new CastByteToCharFunctionFactory()),
                    cast(DATE, new CastByteToDateFunctionFactory()),
                    cast(TIMESTAMP_MICRO, new CastByteToTimestampFunctionFactory())
            );
            case SHORT -> casts(
                    cast(LONG256, new CastShortToLong256FunctionFactory()),
                    cast(STRING, new CastShortToStrFunctionFactory()),
                    cast(VARCHAR, new CastShortToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastShortToSymbolFunctionFactory()),
                    cast(DATE, new CastShortToDateFunctionFactory()),
                    cast(TIMESTAMP_MICRO, new CastShortToTimestampFunctionFactory())
            );
            case CHAR -> casts(
                    cast(LONG256, new CastCharToLong256FunctionFactory()),
                    cast(STRING, new CastCharToStrFunctionFactory()),
                    cast(VARCHAR, new CastCharToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastCharToSymbolFunctionFactory()),
                    cast(DATE, new CastCharToDateFunctionFactory()),
                    cast(TIMESTAMP_MICRO, new CastCharToTimestampFunctionFactory())
            );
            case INT -> casts(
                    cast(LONG256, new CastIntToLong256FunctionFactory()),
                    cast(STRING, new CastIntToStrFunctionFactory()),
                    cast(VARCHAR, new CastIntToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastIntToSymbolFunctionFactory()),
                    cast(IPv4, new CastIntToIPv4FunctionFactory()),
                    cast(SHORT, new CastIntToShortFunctionFactory()),
                    cast(BYTE, new CastIntToByteFunctionFactory())
            );
            case LONG -> casts(
                    cast(LONG256, new CastLongToLong256FunctionFactory()),
                    cast(STRING, new CastLongToStrFunctionFactory()),
                    cast(VARCHAR, new CastLongToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastLongToSymbolFunctionFactory()),
                    cast(INT, new CastLongToIntFunctionFactory()),
                    cast(SHORT, new CastLongToShortFunctionFactory()),
                    cast(BYTE, new CastLongToByteFunctionFactory())
            );
            case DATE -> casts(
                    cast(LONG256, new CastDateToLong256FunctionFactory()),
                    cast(STRING, new CastDateToStrFunctionFactory()),
                    cast(VARCHAR, new CastDateToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastDateToSymbolFunctionFactory())
            );
            case TIMESTAMP -> fromType == TIMESTAMP_MICRO
                    ? casts(
                    cast(LONG256, new CastTimestampToLong256FunctionFactory()),
                    cast(STRING, new CastTimestampToStrFunctionFactory()),
                    cast(VARCHAR, new CastTimestampToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastTimestampToSymbolFunctionFactory())
            )
                    : NO_CASTS;
            case FLOAT -> casts(
                    cast(LONG256, new CastFloatToLong256FunctionFactory()),
                    cast(STRING, new CastFloatToStrFunctionFactory()),
                    cast(VARCHAR, new CastFloatToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastFloatToSymbolFunctionFactory()),
                    cast(DATE, new CastFloatToDateFunctionFactory())
            );
            case DOUBLE -> casts(
                    cast(LONG256, new CastDoubleToLong256FunctionFactory()),
                    cast(STRING, new CastDoubleToStrFunctionFactory()),
                    cast(VARCHAR, new CastDoubleToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastDoubleToSymbolFunctionFactory())
            );
            case STRING -> casts(
                    cast(IPv4, new CastStrToIPv4FunctionFactory()),
                    cast(UUID, new CastStrToUuidFunctionFactory())
            );
            case VARCHAR -> casts(
                    cast(IPv4, new CastVarcharToIPv4FunctionFactory()),
                    cast(UUID, new CastVarcharToUuidFunctionFactory())
            );
            case LONG256 -> casts(
                    cast(STRING, new CastLong256ToStrFunctionFactory()),
                    cast(VARCHAR, new CastLong256ToVarcharFunctionFactory()),
                    cast(SYMBOL, new CastLong256ToSymbolFunctionFactory())
            );
            case UUID -> casts(
                    cast(STRING, new CastUuidToStrFunctionFactory()),
                    cast(VARCHAR, new CastUuidToVarcharFunctionFactory())
            );
            case IPv4 -> casts(
                    cast(STRING, new CastIPv4ToStrFunctionFactory()),
                    cast(VARCHAR, new CastIPv4ToVarcharFunctionFactory()),
                    cast(INT, new CastIPv4ToIntFunctionFactory())
            );
            case UNDEFINED, SYMBOL, GEOBYTE, GEOSHORT, GEOINT, GEOLONG, BINARY, CURSOR, VAR_ARG, RECORD, GEOHASH,
                 LONG128, ARRAY, DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL, REGCLASS,
                 REGPROCEDURE, ARRAY_STRING, PARAMETER, INTERVAL, VARCHAR_SLICE, NULL, UNKNOWN -> NO_CASTS;
        };
    }

    private static Cast[] casts(Cast... casts) {
        return casts;
    }

    @NotNull
    private static CaseFunctionConstructor getCaseFunctionConstructor(int position, int returnType) throws SqlException {
        final CaseFunctionConstructor constructor = constructors.getQuick(tagOf(returnType));
        if (constructor == null) {
            throw SqlException.$(position, "unsupported CASE value type '").put(nameOf(returnType)).put('\'');
        }
        return constructor;
    }

    private static int getDecimalCommonType(int commonType, int valueType, int valuePos) throws SqlException {
        if (commonType == valueType) {
            return commonType;
        }

        commonType = DecimalUtil.getImplicitCastType(commonType);
        valueType = DecimalUtil.getImplicitCastType(valueType);
        if (commonType == 0 || valueType == 0) {
            throw SqlException.inconvertibleTypes(valuePos, valueType, ColumnType.nameOf(valueType), commonType, ColumnType.nameOf(commonType));
        }

        final int commonPrecision = ColumnType.getDecimalPrecision(commonType);
        final int commonScale = ColumnType.getDecimalScale(commonType);
        final int valuePrecision = ColumnType.getDecimalPrecision(valueType);
        final int valueScale = ColumnType.getDecimalScale(valueType);

        final int targetScale = Math.max(commonScale, valueScale);
        final int targetPrecision = Math.min(
                Math.max(commonPrecision - commonScale, valuePrecision - valueScale) + targetScale,
                Decimals.MAX_PRECISION
        );

        return ColumnType.getDecimalType(targetPrecision, targetScale);
    }

    static Function getCaseFunction(int position, int returnType, CaseFunctionPicker picker, ObjList<Function> args) throws SqlException {
        if (isGeoHash(returnType)) {
            return switch (tagOf(returnType)) {
                case GEOBYTE -> new GeoByteCaseFunction(returnType, picker, args);
                case GEOSHORT -> new GeoShortCaseFunction(returnType, picker, args);
                case GEOINT -> new GeoIntCaseFunction(returnType, picker, args);
                default -> new GeoLongCaseFunction(returnType, picker, args);
            };
        }
        if (ColumnType.isArray(returnType)) {
            return new ArrayCaseFunction(returnType, picker, args);
        }

        return getCaseFunctionConstructor(position, returnType).getInstance(position, picker, args, returnType);
    }

    static {
        // both tables are keyed by encoded type: every tag is a row, plus TIMESTAMP_NANO, the
        // one tag with a second encoding that the rows tell apart
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            if (tag.code() >= 0) {
                addRows(tag.code());
            }
        }
        addRows(TIMESTAMP_NANO);
    }

    private record Cast(int toType, FunctionFactory factory) {
    }

    static {
        constructors.set(UNDEFINED, MAX_TAG + 1, null);
        constructors.extendAndSet(STRING, (position, picker, args, returnType) -> new StrCaseFunction(picker, args));
        constructors.extendAndSet(INT, (position, picker, args, returnType) -> new IntCaseFunction(picker, args));
        constructors.extendAndSet(LONG, (position, picker, args, returnType) -> new LongCaseFunction(picker, args));
        constructors.extendAndSet(BYTE, (position, picker, args, returnType) -> new ByteCaseFunction(picker, args));
        constructors.extendAndSet(BOOLEAN, (position, picker, args, returnType) -> new BooleanCaseFunction(picker, args));
        constructors.extendAndSet(SHORT, (position, picker, args, returnType) -> new ShortCaseFunction(picker, args));
        constructors.extendAndSet(CHAR, (position, picker, args, returnType) -> new CharCaseFunction(picker, args));
        constructors.extendAndSet(FLOAT, (position, picker, args, returnType) -> new FloatCaseFunction(picker, args));
        constructors.extendAndSet(DOUBLE, (position, picker, args, returnType) -> new DoubleCaseFunction(picker, args));
        constructors.extendAndSet(LONG256, (position, picker, args, returnType) -> new Long256CaseFunction(picker, args));
        constructors.extendAndSet(SYMBOL, (position, picker, args, returnType) -> new StrCaseFunction(picker, args));
        constructors.extendAndSet(DATE, (position, picker, args, returnType) -> new DateCaseFunction(picker, args));
        constructors.extendAndSet(TIMESTAMP, (position, picker, args, returnType) -> new TimestampCaseFunction(picker, args, returnType));
        constructors.extendAndSet(BINARY, (position, picker, args, returnType) -> new BinCaseFunction(picker, args));
        constructors.extendAndSet(LONG128, (position, picker, args, returnType) -> new Long128CaseFunction(picker, args));
        constructors.extendAndSet(UUID, (position, picker, args, returnType) -> new UuidCaseFunction(picker, args));
        constructors.extendAndSet(IPv4, (position, picker, args, returnType) -> new IPv4CaseFunction(picker, args));
        constructors.extendAndSet(DECIMAL8, (position, picker, args, returnType) -> new DecimalCaseFunction(returnType, picker, args));
        constructors.extendAndSet(DECIMAL16, (position, picker, args, returnType) -> new DecimalCaseFunction(returnType, picker, args));
        constructors.extendAndSet(DECIMAL32, (position, picker, args, returnType) -> new DecimalCaseFunction(returnType, picker, args));
        constructors.extendAndSet(DECIMAL64, (position, picker, args, returnType) -> new DecimalCaseFunction(returnType, picker, args));
        constructors.extendAndSet(DECIMAL128, (position, picker, args, returnType) -> new DecimalCaseFunction(returnType, picker, args));
        constructors.extendAndSet(DECIMAL256, (position, picker, args, returnType) -> new DecimalCaseFunction(returnType, picker, args));
        constructors.extendAndSet(VARCHAR, (position, picker, args, returnType) -> new VarcharCaseFunction(picker, args));
        constructors.extendAndSet(NULL, (position, picker, args, returnType) -> new NullCaseFunction(args));
        constructors.setPos(MAX_TAG + 1);
    }
}
