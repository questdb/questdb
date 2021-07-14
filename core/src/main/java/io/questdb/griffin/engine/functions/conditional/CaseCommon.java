/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
    private static final ObjList<CaseFunctionConstructor> constructors = new ObjList<>();
    private static final LongIntHashMap typeEscalationMap = new LongIntHashMap();
    private static final LongObjHashMap<FunctionFactory> castFactories = new LongObjHashMap<>();
    private static final ThreadLocal<ObjList<Function>> tlArgs = new ThreadLocal<>(ObjList::new);
    private static final ThreadLocal<IntList> tlArgPositions = new ThreadLocal<>(IntList::new);

    static {
        // self for all
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.LONG256), new CastByteToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.STRING), new CastByteToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.SYMBOL), new CastByteToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.LONG256), new CastCharToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.STRING), new CastCharToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.SYMBOL), new CastCharToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.LONG256), new CastShortToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.STRING), new CastShortToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.SYMBOL), new CastShortToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.LONG256), new CastIntToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.STRING), new CastIntToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.SYMBOL), new CastIntToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.LONG256), new CastLongToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.STRING), new CastLongToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.SYMBOL), new CastLongToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.LONG256), new CastFloatToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.STRING), new CastFloatToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.SYMBOL), new CastFloatToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.LONG256), new CastDoubleToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.STRING), new CastDoubleToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.SYMBOL), new CastDoubleToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.LONG256), new CastDateToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.STRING), new CastDateToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.SYMBOL), new CastDateToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.LONG256), new CastTimestampToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.STRING), new CastTimestampToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.SYMBOL), new CastTimestampToSymbolFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.LONG256), new CastBooleanToLong256FunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.STRING), new CastLong256ToStrFunctionFactory());
        castFactories.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.SYMBOL), new CastLong256ToSymbolFunctionFactory());
    }

    static {
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.BYTE), ColumnType.BYTE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.BOOLEAN), ColumnType.BYTE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.CHAR), ColumnType.CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.SHORT), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BYTE, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.BYTE), ColumnType.CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.BOOLEAN), ColumnType.CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.CHAR), ColumnType.CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.SHORT), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.CHAR, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.BYTE), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.BOOLEAN), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.CHAR), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.SHORT), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SHORT, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.BYTE), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.BOOLEAN), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.CHAR), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.SHORT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.INT, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.BYTE), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.BOOLEAN), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.CHAR), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.SHORT), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.INT), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.BYTE), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.BOOLEAN), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.CHAR), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.SHORT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.INT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.LONG), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.TIMESTAMP), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.FLOAT, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.BYTE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.BOOLEAN), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.CHAR), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.SHORT), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.INT), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.LONG), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.FLOAT), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.DATE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.TIMESTAMP), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DOUBLE, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.BYTE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.BOOLEAN), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.CHAR), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.SHORT), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.INT), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.LONG), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.DATE, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.BYTE), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.BOOLEAN), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.CHAR), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.SHORT), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.INT), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.LONG), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.DATE), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.TIMESTAMP, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.BYTE), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.BOOLEAN), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.CHAR), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.SHORT), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.INT), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.LONG), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.LONG256), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.FLOAT), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.DOUBLE), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.DATE), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.TIMESTAMP), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.STRING, ColumnType.SYMBOL), ColumnType.STRING);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.BYTE), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.BOOLEAN), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.CHAR), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.SHORT), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.INT), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.LONG), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.LONG256), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.FLOAT), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.DOUBLE), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.DATE), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.TIMESTAMP), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.SYMBOL, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.BYTE), ColumnType.BYTE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.BOOLEAN), ColumnType.BOOLEAN);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.CHAR), ColumnType.CHAR);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.SHORT), ColumnType.SHORT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.INT), ColumnType.INT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.LONG), ColumnType.LONG);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.FLOAT), ColumnType.FLOAT);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.DOUBLE), ColumnType.DOUBLE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.DATE), ColumnType.DATE);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.TIMESTAMP), ColumnType.TIMESTAMP);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BOOLEAN, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.BYTE), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.BOOLEAN), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.CHAR), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.SHORT), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.INT), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.LONG), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.LONG256), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.FLOAT), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.DOUBLE), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.DATE), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.TIMESTAMP), ColumnType.LONG256);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.STRING), ColumnType.STRING);
        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.LONG256, ColumnType.SYMBOL), ColumnType.SYMBOL);

        typeEscalationMap.put(Numbers.encodeLowHighInts(ColumnType.BINARY, ColumnType.BINARY), ColumnType.BINARY);
    }

    static {
        constructors.set(0, ColumnType.MAX, null);
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
    }

    static int getCommonType(int commonType, int valueType, int valuePos) throws SqlException {
        if (commonType == -1 || commonType == ColumnType.NULL) {
            return valueType;
        }
        if (valueType == ColumnType.NULL) {
            return commonType;
        }
        final int type = typeEscalationMap.get(Numbers.encodeLowHighInts(commonType, valueType));

        if (type == LongIntHashMap.NO_ENTRY_VALUE) {
            throw SqlException.$(valuePos, "inconvertible types ").put(ColumnType.nameOf(valueType)).put(" to ").put(ColumnType.nameOf(commonType));
        }
        return type;
    }

    static Function getCastFunction(
            Function arg,
            int argPosition,
            int toType,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (arg.getType() == ColumnType.NULL) {
            return Constants.getNullConstant(toType);
        }
        final int keyIndex = castFactories.keyIndex(Numbers.encodeLowHighInts(arg.getType(), toType));
        if (keyIndex < 0) {
            FunctionFactory fact = castFactories.valueAt(keyIndex);
            ObjList<Function> args = tlArgs.get();
            args.clear();
            args.add(arg);

            IntList argPositions = tlArgPositions.get();
            argPositions.clear();
            argPositions.add(argPosition);
            return fact.newInstance(0, args, argPositions, configuration, sqlExecutionContext);
        }

        return arg;
    }

    @NotNull
    private static CaseFunctionConstructor getCaseFunctionConstructor(int position, int returnType) throws SqlException {
        final CaseFunctionConstructor constructor = constructors.getQuick(returnType);
        if (constructor == null) {
            throw SqlException.$(position, "not implemented for type '").put(ColumnType.nameOf(returnType)).put('\'');
        }
        return constructor;
    }

    static Function getCaseFunction(int position, int returnType, CaseFunctionPicker picker, ObjList<Function> args) throws SqlException {
        return getCaseFunctionConstructor(position, returnType).getInstance(position, picker, args);
    }
}
