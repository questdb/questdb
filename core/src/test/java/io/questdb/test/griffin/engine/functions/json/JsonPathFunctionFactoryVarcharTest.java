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

package io.questdb.test.griffin.engine.functions.json;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.json.JsonPathFunctionFactory;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.griffin.engine.functions.json.JsonPathFunc.DEFAULT_VALUE_ON_ERROR;
import static io.questdb.griffin.engine.functions.json.JsonPathFunc.FAIL_ON_ERROR;

public class JsonPathFunctionFactoryVarcharTest extends AbstractFunctionFactoryTest {

    @Override
    public Invocation call(Object... args) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void test10000() throws SqlException {
        callFn(utf8("{\"path\": 10000.5}"), utf8(".path")).andAssert("10000.5");
        callFn(utf8("{\"path\": 10000.5}"), dirUtf8(".path")).andAssert("10000.5");
        callFn(dirUtf8("{\"path\": 10000.5}"), utf8(".path")).andAssert("10000.5");
        callFn(dirUtf8("{\"path\": 10000.5}"), dirUtf8(".path")).andAssert("10000.5");
    }

    @Test
    public void testBigNumber() throws SqlException {
        callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path")).andAssert("100000000000000000000000000");
    }

    @Test
    public void testDblPrecision() throws SqlException {
        callFn(utf8("{\"path\": 0.0000000000000000000000000001}"), utf8(".path")).andAssert("0.0000000000000000000000000001");
    }

    @Test
    public void testDefaultDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, utf8("def")).andAssert("abc");
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, dirUtf8("def")).andAssert("abc");
    }

    @Test
    public void testDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, utf8("def_val")).andAssert("def_val");
        callFn(utf8("{\"path\": null}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, dirUtf8("def_val")).andAssert("def_val");
    }

    @Test
    public void testDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssert("abc");
        callFn(dirUtf8("{\"path\": \"abc\"}"), dirUtf8(".path")).andAssert("abc");
    }

    @Test
    public void testEmptyJson() throws SqlException {
        callFn(utf8("{}"), utf8(".path")).andAssert((String) null);
    }

    @Test
    public void testInString() throws SqlException {
        callFn(utf8("{\"path\": \"123.5\"}"), utf8(".path")).andAssert("123.5");
    }

    @Test
    public void testInt() throws SqlException {
        callFn(dirUtf8("{\"path\": 123}"), dirUtf8(".path")).andAssert("123");
    }

    @Test
    public void testNegative() throws SqlException {
        callFn(utf8("{\"path\": -123.5}"), utf8(".path")).andAssert("-123.5");
    }

    @Test
    public void testNullJson() throws SqlException {
        callFn(
                utf8(null),
                utf8(".path")
        ).andAssert((String) null);
        callFn(
                utf8(null),
                dirUtf8(".path")
        ).andAssert((String) null);
    }

    @Test
    public void testNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path")).andAssert((String) null);
        callFn(utf8("{\"path\": null}"), dirUtf8(".path")).andAssert((String) null);
        callFn(dirUtf8("{\"path\": null}"), utf8(".path")).andAssert((String) null);
        callFn(dirUtf8("{\"path\": null}"), dirUtf8(".path")).andAssert((String) null);
    }

    @Test
    public void testOne() throws SqlException {
        callFn(utf8("{\"path\": 1}"), utf8(".path")).andAssert("1");
    }

    @Test
    public void testStrict10000() throws SqlException {
        callFn(utf8("{\"path\": 10000.0}"), utf8(".path"), FAIL_ON_ERROR).andAssert("10000.0");
    }

    @Test
    public void testStrictBigNumber() throws SqlException {
        callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path"), FAIL_ON_ERROR).andAssert("100000000000000000000000000");
    }

    @Test
    public void testStrictDefaultString() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), FAIL_ON_ERROR, "42.0").andAssert("abc");
    }

    @Test
    public void testStrictDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), FAIL_ON_ERROR, "42.0").andAssert((String) null);
    }

    @Test
    public void testStrictString() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), FAIL_ON_ERROR).andAssert("abc");
    }

    @Test
    public void testStrictEmptyJson() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{}"), utf8(".path"), FAIL_ON_ERROR));
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): NO_SUCH_FIELD:");
    }

    @Test
    public void testStrictNullJson() throws SqlException {
        callFn(utf8(null), utf8(".path"), FAIL_ON_ERROR).andAssertDoubleNan();
    }

    @Test
    public void testStrictNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), FAIL_ON_ERROR).andAssert((String) null);
    }

    @Test
    public void testStrictOne() throws SqlException {
        callFn(utf8("{\"path\": 1}"), utf8(".path"), FAIL_ON_ERROR).andAssert("1");
    }

    @Test
    public void testStrictZero() throws SqlException {
        callFn(utf8("{\"path\": 0}"), utf8(".path"), FAIL_ON_ERROR).andAssert("0");
    }

    @Test
    public void testUnsigned64Bit() throws SqlException {
        callFn(utf8("{\"path\": 9999999999999999999}"), utf8(".path")).andAssert("9999999999999999999");
    }

    @Test
    public void testArray() throws SqlException {
        callFn(utf8("{\"path\": [1, 2, 3]}"), utf8(".path")).andAssert("[1, 2, 3]");
    }

    @Test
    public void testArratStrict() throws SqlException {
        callFn(utf8("{\"path\": [1, 2, 3]}"), utf8(".path"), FAIL_ON_ERROR).andAssert("[1, 2, 3]");
    }

    @Test
    public void testDict() throws SqlException {
        callFn(utf8("{\"path\": {\"a\": 1, \"b\": 2}}"), utf8(".path")).andAssert("{\"a\": 1, \"b\": 2}");
    }

    @Test
    public void testDictStrict() throws SqlException {
        callFn(utf8("{\"path\": {\"a\": 1, \"b\": 2}}"), utf8(".path"), FAIL_ON_ERROR).andAssert("{\"a\": 1, \"b\": 2}");
    }

    @Test
    public void testViaSql() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select '{\"path\": \"  x   z\"}'::varchar as path from long_sequence(10));");
        });

        assertMemoryLeak(() -> {
            assertSql(
                    "json_path\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n" +
                            "  x   z\n",
                    "select json_path(path, '.path', " + ColumnType.VARCHAR + ") from x"
            );
        });
    }

    private Invocation callFn(Utf8Sequence json, Utf8Sequence path, Object... args) throws SqlException {
        final boolean[] forceConstants = new boolean[args.length + 3];
        Arrays.fill(forceConstants, true);
        forceConstants[0] = false;
        final Object[] newArgs = new Object[args.length + 3];
        newArgs[0] = json;
        newArgs[1] = path;
        newArgs[2] = (int) ColumnType.VARCHAR;
        System.arraycopy(args, 0, newArgs, 3, args.length);
        return callCustomised(
                null,
                forceConstants,
                true,
                newArgs);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathFunctionFactory();
    }
}
