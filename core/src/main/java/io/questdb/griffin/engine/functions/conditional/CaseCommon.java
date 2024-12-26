/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import org.jetbrains.annotations.NotNull;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;

import static io.questdb.cairo.ColumnType.BINARY;
import static io.questdb.cairo.ColumnType.BOOLEAN;
import static io.questdb.cairo.ColumnType.BYTE;
import static io.questdb.cairo.ColumnType.CHAR;
import static io.questdb.cairo.ColumnType.DATE;
import static io.questdb.cairo.ColumnType.DOUBLE;
import static io.questdb.cairo.ColumnType.FLOAT;
import static io.questdb.cairo.ColumnType.GEOBYTE;
import static io.questdb.cairo.ColumnType.GEOINT;
import static io.questdb.cairo.ColumnType.GEOSHORT;
import static io.questdb.cairo.ColumnType.INT;
import static io.questdb.cairo.ColumnType.IPv4;
import static io.questdb.cairo.ColumnType.LONG;
import static io.questdb.cairo.ColumnType.LONG128;
import static io.questdb.cairo.ColumnType.LONG256;
import static io.questdb.cairo.ColumnType.NULL;
import static io.questdb.cairo.ColumnType.SHORT;
import static io.questdb.cairo.ColumnType.STRING;
import static io.questdb.cairo.ColumnType.SYMBOL;
import static io.questdb.cairo.ColumnType.TIMESTAMP;
import static io.questdb.cairo.ColumnType.UNDEFINED;
import static io.questdb.cairo.ColumnType.UUID;
import static io.questdb.cairo.ColumnType.VARCHAR;
import static io.questdb.cairo.ColumnType.isGeoHash;
import static io.questdb.cairo.ColumnType.isNull;
import static io.questdb.cairo.ColumnType.isUndefined;
import static io.questdb.cairo.ColumnType.nameOf;
import static io.questdb.cairo.ColumnType.tagOf;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.cast.CastBooleanToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToUuidFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToUuidFunctionFactory;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;

public class CaseCommon {
    private static final LongObjHashMap<FunctionFactory> castFactories = new LongObjHashMap<>();
    private static final ObjList<CaseFunctionConstructor> constructors = new ObjList<>(NULL + 1);
    private static final ThreadLocal<IntList> tlArgPositions = new ThreadLocal<>(IntList::new);
    private static final ThreadLocal<ObjList<Function>> tlArgs = new ThreadLocal<>(ObjList::new);
    private static final LongIntHashMap typeEscalationMap = new LongIntHashMap();

    // public for testing
    public static Function getCastFunction(
            Function arg,
            int argPosition,
            int toType,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (isNull(arg.getType())) {
            return Constants.getNullConstant(toType);
        }
        final int keyIndex = castFactories.keyIndex(Numbers.encodeLowHighInts(arg.getType(), toType));
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

        final int type = typeEscalationMap.get(Numbers.encodeLowHighInts(commonType, valueType));
        if (type == LongIntHashMap.NO_ENTRY_VALUE) {
            throw SqlException.inconvertibleTypes(valuePos, valueType, ColumnType.nameOf(valueType), commonType, ColumnType.nameOf(commonType));
        }
        return type;
    }

    @NotNull
    private static CaseFunctionConstructor getCaseFunctionConstructor(int position, int returnType) throws SqlException {
        final CaseFunctionConstructor constructor = constructors.getQuick(returnType);
        if (constructor == null) {
            throw SqlException.$(position, "unsupported CASE value type '").put(nameOf(returnType)).put('\'');
        }
        return constructor;
    }

    static Function getCaseFunction(int position, int returnType, CaseFunctionPicker picker, ObjList<Function> args) throws SqlException {
        if (isGeoHash(returnType)) {
            switch (tagOf(returnType)) {
                case GEOBYTE:
                    return new GeoByteCaseFunction(returnType, picker, args);
                case GEOSHORT:
                    return new GeoShortCaseFunction(returnType, picker, args);
                case GEOINT:
                    return new GeoIntCaseFunction(returnType, picker, args);
                default:
                    return new GeoLongCaseFunction(returnType, picker, args);
            }
        }

        return getCaseFunctionConstructor(position, returnType).getInstance(position, picker, args);
    }

    static {
        // self for all
        castFactories.put(Numbers.encodeLowHighInts(BYTE, LONG256), new CastByteToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BYTE, STRING), new CastByteToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BYTE, VARCHAR), new CastByteToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BYTE, SYMBOL), new CastByteToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BYTE, CHAR), new CastByteToCharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BYTE, DATE), new CastByteToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BYTE, TIMESTAMP), new CastByteToTimestampFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(CHAR, LONG256), new CastCharToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(CHAR, STRING), new CastCharToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(CHAR, VARCHAR), new CastCharToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(CHAR, SYMBOL), new CastCharToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(CHAR, DATE), new CastCharToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(CHAR, TIMESTAMP), new CastCharToTimestampFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(SHORT, LONG256), new CastShortToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(SHORT, STRING), new CastShortToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(SHORT, VARCHAR), new CastShortToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(SHORT, SYMBOL), new CastShortToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(SHORT, DATE), new CastShortToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(SHORT, TIMESTAMP), new CastShortToTimestampFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(INT, LONG256), new CastIntToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(INT, STRING), new CastIntToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(INT, VARCHAR), new CastIntToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(IPv4, STRING), new CastIPv4ToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(IPv4, VARCHAR), new CastIPv4ToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(STRING, IPv4), new CastStrToIPv4FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(VARCHAR, IPv4), new CastVarcharToIPv4FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(INT, IPv4), new CastIntToIPv4FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(IPv4, INT), new CastIPv4ToIntFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(INT, SYMBOL), new CastIntToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG, LONG256), new CastLongToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG, STRING), new CastLongToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG, VARCHAR), new CastLongToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG, SYMBOL), new CastLongToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(FLOAT, LONG256), new CastFloatToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(FLOAT, STRING), new CastFloatToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(FLOAT, VARCHAR), new CastFloatToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(FLOAT, SYMBOL), new CastFloatToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(FLOAT, DATE), new CastFloatToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DOUBLE, LONG256), new CastDoubleToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DOUBLE, STRING), new CastDoubleToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DOUBLE, VARCHAR), new CastDoubleToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DOUBLE, SYMBOL), new CastDoubleToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DATE, LONG256), new CastDateToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DATE, STRING), new CastDateToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DATE, VARCHAR), new CastDateToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(DATE, SYMBOL), new CastDateToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(TIMESTAMP, LONG256), new CastTimestampToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(TIMESTAMP, STRING), new CastTimestampToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(TIMESTAMP, VARCHAR), new CastTimestampToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(TIMESTAMP, SYMBOL), new CastTimestampToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(BOOLEAN, LONG256), new CastBooleanToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG256, STRING), new CastLong256ToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG256, VARCHAR), new CastLong256ToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(LONG256, SYMBOL), new CastLong256ToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(UUID, STRING), new CastUuidToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(UUID, VARCHAR), new CastUuidToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(STRING, UUID), new CastStrToUuidFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(VARCHAR, UUID), new CastVarcharToUuidFunctionFactory());
    }

    static {
        typeEscalationMap.put(Numbers.encodeLowHighInts(BYTE, BYTE), BYTE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(BYTE, SHORT), SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(BYTE, INT), INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(BYTE, LONG), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(BYTE, FLOAT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(BYTE, DOUBLE), DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(CHAR, CHAR), CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(CHAR, STRING), STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(CHAR, VARCHAR), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(CHAR, SYMBOL), SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(SHORT, BYTE), SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SHORT, SHORT), SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SHORT, INT), INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SHORT, LONG), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SHORT, FLOAT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SHORT, DOUBLE), DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(INT, BYTE), INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(INT, SHORT), INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(INT, INT), INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(INT, LONG), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(INT, FLOAT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(INT, DOUBLE), DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(IPv4, IPv4), IPv4);
        typeEscalationMap.put(Numbers.encodeLowHighInts(IPv4, STRING), IPv4);
        typeEscalationMap.put(Numbers.encodeLowHighInts(IPv4, VARCHAR), IPv4);

        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG, BYTE), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG, SHORT), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG, INT), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG, LONG), LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG, FLOAT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG, DOUBLE), DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(FLOAT, BYTE), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(FLOAT, SHORT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(FLOAT, INT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(FLOAT, LONG), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(FLOAT, FLOAT), FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(FLOAT, DOUBLE), DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(DOUBLE, BYTE), DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(DOUBLE, SHORT), DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(DOUBLE, INT), DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(DOUBLE, LONG), DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(DOUBLE, FLOAT), DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(DOUBLE, DOUBLE), DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(DATE, DATE), DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(TIMESTAMP, TIMESTAMP), TIMESTAMP);

        typeEscalationMap.put(Numbers.encodeLowHighInts(STRING, STRING), STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(STRING, SYMBOL), STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(STRING, VARCHAR), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(STRING, CHAR), STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(STRING, UUID), UUID);
        typeEscalationMap.put(Numbers.encodeLowHighInts(STRING, IPv4), IPv4);

        typeEscalationMap.put(Numbers.encodeLowHighInts(VARCHAR, STRING), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(VARCHAR, VARCHAR), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(VARCHAR, SYMBOL), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(VARCHAR, CHAR), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(VARCHAR, UUID), UUID);
        typeEscalationMap.put(Numbers.encodeLowHighInts(VARCHAR, IPv4), IPv4);

        typeEscalationMap.put(Numbers.encodeLowHighInts(SYMBOL, STRING), STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SYMBOL, VARCHAR), VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SYMBOL, SYMBOL), SYMBOL);
        typeEscalationMap.put(Numbers.encodeLowHighInts(SYMBOL, CHAR), STRING);

        typeEscalationMap.put(Numbers.encodeLowHighInts(BOOLEAN, BOOLEAN), BOOLEAN);

        typeEscalationMap.put(Numbers.encodeLowHighInts(UUID, UUID), UUID);
        typeEscalationMap.put(Numbers.encodeLowHighInts(UUID, STRING), UUID);
        typeEscalationMap.put(Numbers.encodeLowHighInts(UUID, VARCHAR), UUID);

        typeEscalationMap.put(Numbers.encodeLowHighInts(LONG256, LONG256), LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(BINARY, BINARY), BINARY);
    }

    static {
        constructors.set(UNDEFINED, NULL + 1, null);
        constructors.extendAndSet(STRING, (position, picker, args) -> new StrCaseFunction(picker, args));
        constructors.extendAndSet(INT, (position, picker, args) -> new IntCaseFunction(picker, args));
        constructors.extendAndSet(LONG, (position, picker, args) -> new LongCaseFunction(picker, args));
        constructors.extendAndSet(BYTE, (position, picker, args) -> new ByteCaseFunction(picker, args));
        constructors.extendAndSet(BOOLEAN, (position, picker, args) -> new BooleanCaseFunction(picker, args));
        constructors.extendAndSet(SHORT, (position, picker, args) -> new ShortCaseFunction(picker, args));
        constructors.extendAndSet(CHAR, (position, picker, args) -> new CharCaseFunction(picker, args));
        constructors.extendAndSet(FLOAT, (position, picker, args) -> new FloatCaseFunction(picker, args));
        constructors.extendAndSet(DOUBLE, (position, picker, args) -> new DoubleCaseFunction(picker, args));
        constructors.extendAndSet(LONG256, (position, picker, args) -> new Long256CaseFunction(picker, args));
        constructors.extendAndSet(SYMBOL, (position, picker, args) -> new StrCaseFunction(picker, args));
        constructors.extendAndSet(DATE, (position, picker, args) -> new DateCaseFunction(picker, args));
        constructors.extendAndSet(TIMESTAMP, (position, picker, args) -> new TimestampCaseFunction(picker, args));
        constructors.extendAndSet(BINARY, (position, picker, args) -> new BinCaseFunction(picker, args));
        constructors.extendAndSet(LONG128, (position, picker, args) -> new Long128CaseFunction(picker, args));
        constructors.extendAndSet(UUID, (position, picker, args) -> new UuidCaseFunction(picker, args));
        constructors.extendAndSet(IPv4, (position, picker, args) -> new IPv4CaseFunction(picker, args));
        constructors.extendAndSet(VARCHAR, (position, picker, args) -> new VarcharCaseFunction(picker, args));
        constructors.extendAndSet(NULL, (position, picker, args) -> new NullCaseFunction(args));
        constructors.setPos(NULL + 1);
    }
}
