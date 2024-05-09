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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class CaseCommon {
    private static final LongObjHashMap<FunctionFactory> castFactories = new LongObjHashMap<>();
    private static final ObjList<CaseFunctionConstructor> constructors = new ObjList<>();
    private static final ThreadLocal<IntList> tlArgPositions = new ThreadLocal<>(IntList::new);
    private static final ThreadLocal<ObjList<Function>> tlArgs = new ThreadLocal<>(ObjList::new);
    private static final LongIntHashMap typeEscalationMap = new LongIntHashMap();

    @NotNull
    private static CaseFunctionConstructor getCaseFunctionConstructor(int position, int returnType) throws SqlException {
        final CaseFunctionConstructor constructor = constructors.getQuick(returnType);
        if (constructor == null) {
            throw SqlException.$(position, "not implemented for type '").put(ColumnType.nameOf(returnType)).put('\'');
        }
        return constructor;
    }

    static Function getCaseFunction(int position, int returnType, CaseFunctionPicker picker, ObjList<Function> args) throws SqlException {
        if (ColumnType.isGeoHash(returnType)) {
            switch (ColumnType.tagOf(returnType)) {
                case ColumnType.GEOBYTE:
                    return new GeoByteCaseFunction(returnType, picker, args);
                case ColumnType.GEOSHORT:
                    return new GeoShortCaseFunction(returnType, picker, args);
                case ColumnType.GEOINT:
                    return new GeoIntCaseFunction(returnType, picker, args);
                default:
                    return new GeoLongCaseFunction(returnType, picker, args);
            }
        }

        return getCaseFunctionConstructor(position, returnType).getInstance(position, picker, args);
    }

    // public for testing
    public static Function getCastFunction(
            Function arg,
            int argPosition,
            int toType,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (ColumnType.isNull(arg.getType())) {
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
    public static int getCommonType(int commonType, int valueType, int valuePos) throws SqlException {
        if (commonType == -1 || ColumnType.isNull(commonType) || commonType == 0) {
            return valueType;
        }
        if (ColumnType.isNull(valueType)) {
            return commonType;
        }
        final int type = typeEscalationMap.get(Numbers.encodeLowHighInts(commonType, valueType));

        if (type == LongIntHashMap.NO_ENTRY_VALUE) {
            throw SqlException.$(valuePos, "inconvertible types ").put(ColumnType.nameOf(valueType)).put(" to ").put(ColumnType.nameOf(commonType));
        }
        return type;
    }

    static {
        // self for all
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.LONG256), new CastByteToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.STRING), new CastByteToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.VARCHAR), new CastByteToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.SYMBOL), new CastByteToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.CHAR), new CastByteToCharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.DATE), new CastByteToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.TIMESTAMP), new CastByteToTimestampFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.LONG256), new CastCharToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.STRING), new CastCharToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.VARCHAR), new CastCharToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.SYMBOL), new CastCharToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.DATE), new CastCharToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.TIMESTAMP), new CastCharToTimestampFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.LONG256), new CastShortToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.STRING), new CastShortToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.VARCHAR), new CastShortToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.SYMBOL), new CastShortToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.DATE), new CastShortToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.TIMESTAMP), new CastShortToTimestampFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.LONG256), new CastIntToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.STRING), new CastIntToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.VARCHAR), new CastIntToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.IPv4, ColumnType.STRING), new CastIPv4ToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.IPv4, ColumnType.VARCHAR), new CastIPv4ToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.IPv4), new CastStrToIPv4FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.VARCHAR, ColumnType.IPv4), new CastVarcharToIPv4FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.IPv4), new CastIntToIPv4FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.IPv4, ColumnType.INT), new CastIPv4ToIntFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.SYMBOL), new CastIntToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.LONG256), new CastLongToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.STRING), new CastLongToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.VARCHAR), new CastLongToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.SYMBOL), new CastLongToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.LONG256), new CastFloatToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.STRING), new CastFloatToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.VARCHAR), new CastFloatToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.SYMBOL), new CastFloatToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.DATE), new CastFloatToDateFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.LONG256), new CastDoubleToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.STRING), new CastDoubleToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.VARCHAR), new CastDoubleToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.SYMBOL), new CastDoubleToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.LONG256), new CastDateToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.STRING), new CastDateToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.VARCHAR), new CastDateToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.SYMBOL), new CastDateToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.LONG256), new CastTimestampToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.STRING), new CastTimestampToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.VARCHAR), new CastTimestampToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.SYMBOL), new CastTimestampToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.LONG256), new CastBooleanToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.STRING), new CastLong256ToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.VARCHAR), new CastLong256ToVarcharFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.SYMBOL), new CastLong256ToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.UUID, ColumnType.STRING), new CastUuidToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.UUID, ColumnType.VARCHAR), new CastUuidToVarcharFunctionFactory());
    }

    static {
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.BYTE), ColumnType.BYTE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.SHORT), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.DOUBLE), ColumnType.DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.CHAR), ColumnType.CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.VARCHAR), ColumnType.VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.BYTE), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.SHORT), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.DOUBLE), ColumnType.DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.BYTE), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.SHORT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.DOUBLE), ColumnType.DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.IPv4, ColumnType.IPv4), ColumnType.IPv4);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.BYTE), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.SHORT), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.INT), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.DOUBLE), ColumnType.DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.BYTE), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.SHORT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.INT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.LONG), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.DOUBLE), ColumnType.DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.BYTE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.SHORT), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.INT), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.LONG), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.FLOAT), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.DOUBLE), ColumnType.DOUBLE);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.SYMBOL), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.VARCHAR), ColumnType.VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.CHAR), ColumnType.STRING);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.VARCHAR, ColumnType.STRING), ColumnType.VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.VARCHAR, ColumnType.VARCHAR), ColumnType.VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.VARCHAR, ColumnType.SYMBOL), ColumnType.VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.VARCHAR, ColumnType.CHAR), ColumnType.VARCHAR);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.VARCHAR), ColumnType.VARCHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.SYMBOL), ColumnType.SYMBOL);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.CHAR), ColumnType.STRING);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.BOOLEAN), ColumnType.BOOLEAN);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.UUID, ColumnType.UUID), ColumnType.UUID);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BINARY, ColumnType.BINARY), ColumnType.BINARY);
    }

    static {
        constructors.set(0, ColumnType.NULL, null);
        constructors.extendAndSet(ColumnType.STRING, (position1, picker1, args1) -> new StrCaseFunction(picker1, args1));
        constructors.extendAndSet(ColumnType.INT, IntCaseFunction::new);
        constructors.extendAndSet(ColumnType.LONG, (position2, picker2, args2) -> new LongCaseFunction(picker2, args2));
        constructors.extendAndSet(ColumnType.BYTE, (position2, picker2, args2) -> new ByteCaseFunction(picker2, args2));
        constructors.extendAndSet(ColumnType.BOOLEAN, (position1, picker1, args1) -> new BooleanCaseFunction(picker1, args1));
        constructors.extendAndSet(ColumnType.SHORT, (position3, picker3, args3) -> new ShortCaseFunction(picker3, args3));
        constructors.extendAndSet(ColumnType.CHAR, (position2, picker2, args2) -> new CharCaseFunction(picker2, args2));
        constructors.extendAndSet(ColumnType.FLOAT, (position1, picker1, args1) -> new FloatCaseFunction(picker1, args1));
        constructors.extendAndSet(ColumnType.DOUBLE, (position1, picker1, args1) -> new DoubleCaseFunction(picker1, args1));
        constructors.extendAndSet(ColumnType.LONG256, (position1, picker1, args1) -> new Long256CaseFunction(picker1, args1));
        constructors.extendAndSet(ColumnType.SYMBOL, (position, picker, args) -> new StrCaseFunction(picker, args));
        constructors.extendAndSet(ColumnType.DATE, (position, picker, args) -> new DateCaseFunction(picker, args));
        constructors.extendAndSet(ColumnType.TIMESTAMP, (position, picker, args) -> new TimestampCaseFunction(picker, args));
        constructors.extendAndSet(ColumnType.BINARY, (position, picker, args) -> new BinCaseFunction(picker, args));
        constructors.extendAndSet(ColumnType.LONG128, (position, picker, args) -> new Long128CaseFunction(picker, args));
        constructors.extendAndSet(ColumnType.UUID, (position, picker, args) -> new UuidCaseFunction(picker, args));
        constructors.extendAndSet(ColumnType.IPv4, IPv4CaseFunction::new);
        constructors.extendAndSet(ColumnType.VARCHAR, (position, picker, args) -> new VarcharCaseFunction(picker, args));
    }
}
