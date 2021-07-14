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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.engine.functions.eq.*;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Collections;

public class FunctionParserEqFunctionsNullTest extends BaseFunctionFactoryTest {

    private static final FunctionFactory[] EQ_FUNCS = {
            new EqBinaryFunctionFactory(),
            new EqBooleanFunctionFactory(),
            new EqByteFunctionFactory(),
            new EqCharCharFunctionFactory(),
            new EqDoubleFunctionFactory(),
            new EqIntFunctionFactory(),
            new EqIntStrCFunctionFactory(),
            new EqLong256FunctionFactory(),
            new EqLong256StrFunctionFactory(),
            new EqLongFunctionFactory(),
            new EqShortFunctionFactory(),
            new EqStrCharFunctionFactory(),
            new EqStrFunctionFactory(),
            new EqSymCharFunctionFactory(),
            new EqSymStrFunctionFactory(),
            new EqTimestampFunctionFactory()
    };

    // SqlCompiler.isAssignableFrom
    private static final int[] EQUIVALENT_NULL_TYPES = {
            ColumnType.NULL,
            ColumnType.UNDEFINED,
            ColumnType.BOOLEAN,
            ColumnType.BYTE,
            ColumnType.SHORT,
            ColumnType.CHAR,
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.DATE,
            ColumnType.TIMESTAMP,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.STRING,
            ColumnType.LONG256,
            ColumnType.BINARY
    };

    private static final Record ILLEGAL_ACCESS_RECORD = (Record) Proxy.newProxyInstance(
            Record.class.getClassLoader(),
            new Class[]{Record.class},
            (proxy, method, args) -> {
                throw new IllegalAccessException();
            }
    );

    private static final Class<?>[] RECORD_SIG = {Record.class};
    private static final Object[] NULL_RECORD_ARG = {null};
    private static final Record NULL_RECORD = (Record) Proxy.newProxyInstance(
            Record.class.getClassLoader(),
            new Class[]{Record.class},
            (proxy, method, args) -> {
                try {
                    Method constMethod = NullConstant.class.getMethod(method.getName(), RECORD_SIG);
                    return constMethod.invoke(NullConstant.NULL, NULL_RECORD_ARG);
                } catch (UndeclaredThrowableException undeclared) {
                    return method.invoke(proxy, args);
                }
            }
    );

    @Test
    public void testEqFunctionResolutionNullArgsForNumericTypes() throws SqlException {
        Arrays.stream(EQ_FUNCS).forEach(functions::add);
        FunctionParser functionParser = createFunctionParser();
        for (int col0Type : EQUIVALENT_NULL_TYPES) {
            for (int col1Type : EQUIVALENT_NULL_TYPES) {

                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("col0", col0Type, null));
                metadata.add(new TableColumnMetadata("col1", col1Type, null));

                Collections.shuffle(functions);

                Function function = parseFunction("null = null", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertTrue(function.getBool(ILLEGAL_ACCESS_RECORD));

                function = parseFunction("null != null", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertFalse(function.getBool(ILLEGAL_ACCESS_RECORD));

                function = parseFunction("col0 = null", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertTrue(function.getBool(NULL_RECORD));

                function = parseFunction("col1 = null", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertTrue(function.getBool(NULL_RECORD));

                function = parseFunction("col0 = col0", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertTrue(function.getBool(NULL_RECORD));

                function = parseFunction("null = col0", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertTrue(function.getBool(NULL_RECORD));

                function = parseFunction("null = col1", metadata, functionParser);
                Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
                Assert.assertTrue(function.getBool(NULL_RECORD));
            }
        }
    }
}