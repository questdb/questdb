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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class FunctionParserCastFunctionsNullTest extends BaseFunctionFactoryTest {

    private static final FunctionFactory[] CAST_FUNCS = {
            new CastBooleanToBooleanFunctionFactory(),
            new CastByteToByteFunctionFactory(),
            new CastShortToShortFunctionFactory(),
            new CastIntToIntFunctionFactory(),
            new CastLongToLongFunctionFactory(),
            new CastDateToDateFunctionFactory(),
            new CastTimestampToTimestampFunctionFactory(),
            new CastFloatToFloatFunctionFactory(),
            new CastDoubleToDoubleFunctionFactory(),
            new CastStrToStrFunctionFactory(),
            new CastSymbolToSymbolFunctionFactory(),
            new CastLong256ToLong256FunctionFactory(),
            new CastNullFunctionFactory()
    };

    private static final CharSequenceIntHashMap typeNameToId = new CharSequenceIntHashMap();

    static {
        typeNameToId.put("boolean", ColumnType.BOOLEAN);
        typeNameToId.put("byte", ColumnType.BYTE);
        typeNameToId.put("short", ColumnType.SHORT);
        typeNameToId.put("char", ColumnType.CHAR);
        typeNameToId.put("int", ColumnType.INT);
        typeNameToId.put("long", ColumnType.LONG);
        typeNameToId.put("date", ColumnType.DATE);
        typeNameToId.put("timestamp", ColumnType.TIMESTAMP);
        typeNameToId.put("float", ColumnType.FLOAT);
        typeNameToId.put("double", ColumnType.DOUBLE);
        typeNameToId.put("string", ColumnType.STRING);
        typeNameToId.put("symbol", ColumnType.SYMBOL);
        typeNameToId.put("long256", ColumnType.LONG256);
        typeNameToId.put("binary", ColumnType.BINARY);
    }

    private static final ObjList<CharSequence> typeNames = typeNameToId.keys();

    @Test
    public void testCastNull() throws SqlException {
        Arrays.stream(CAST_FUNCS).forEach(functions::add);
        FunctionParser functionParser = createFunctionParser();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < typeNames.size(); i++) {
            Collections.shuffle(functions);
            CharSequence type = typeNames.getQuick(i);
            Function function = parseFunction(String.format("cast(null as %s)", type), metadata, functionParser);
            Assert.assertEquals(typeNameToId.get(type), function.getType());
            Assert.assertEquals(true, function.isConstant());
        }
    }
}