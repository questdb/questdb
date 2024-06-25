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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.griffin.engine.functions.json.JsonPathFunc.DEFAULT_VALUE_ON_ERROR;
import static io.questdb.griffin.engine.functions.json.JsonPathFunc.FAIL_ON_ERROR;

public class JsonPathFunctionFactoryFloatTest extends AbstractFunctionFactoryTest {

    @Override
    public Invocation call(Object... args) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void test10000() throws SqlException {
        callFn(utf8("{\"path\": 10000.5}"), utf8(".path")).andAssert(10000.5f, DELTA_F);
        callFn(utf8("{\"path\": 10000.5}"), dirUtf8(".path")).andAssert(10000.5f, DELTA_F);
        callFn(dirUtf8("{\"path\": 10000.5}"), utf8(".path")).andAssert(10000.5f, DELTA_F);
        callFn(dirUtf8("{\"path\": 10000.5}"), dirUtf8(".path")).andAssert(10000.5f, DELTA_F);
    }

    @Test
    public void testBigNumber() throws SqlException {
        callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path")).andAssert(1.0E26f, DELTA_F);
    }

    @Test
    public void testDblPrecision() throws SqlException {
        callFn(utf8("{\"path\": 0.0000000000000000000000000001}"), utf8(".path")).andAssert(1.0E-28f, DELTA_F);
    }

    @Test
    public void testDefaultDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, 42.5f).andAssert(42.5f, DELTA_F);
    }

    @Test
    public void testDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, 42.5f).andAssertFloatNan();
    }

    @Test
    public void testDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssertFloatNan();
        callFn(dirUtf8("{\"path\": \"abc\"}"), dirUtf8(".path")).andAssertFloatNan();
    }

    @Test
    public void testEmptyJson() throws SqlException {
        callFn(utf8("{}"), utf8(".path")).andAssertFloatNan();
    }

    @Test
    public void testInString() throws SqlException {
        callFn(utf8("{\"path\": \"123.5\"}"), utf8(".path")).andAssert(123.5f, DELTA_F);
    }

    @Test
    public void testInt() throws SqlException {
        callFn(dirUtf8("{\"path\": 123}"), dirUtf8(".path")).andAssert(123.0f, DELTA_F);
    }

    @Test
    public void testNegative() throws SqlException {
        callFn(utf8("{\"path\": -123.5}"), utf8(".path")).andAssert(-123.5f, DELTA_F);
    }

    @Test
    public void testNullJson() throws SqlException {
        callFn(
                utf8(null),
                utf8(".path")
        ).andAssertFloatNan();
        callFn(
                utf8(null),
                dirUtf8(".path")
        ).andAssertFloatNan();
    }

    @Test
    public void testNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path")).andAssertFloatNan();
        callFn(utf8("{\"path\": null}"), dirUtf8(".path")).andAssertFloatNan();
        callFn(dirUtf8("{\"path\": null}"), utf8(".path")).andAssertFloatNan();
        callFn(dirUtf8("{\"path\": null}"), dirUtf8(".path")).andAssertFloatNan();
    }

    @Test
    public void testOne() throws SqlException {
        callFn(utf8("{\"path\": 1.0}"), utf8(".path")).andAssert(1.0f, DELTA_F);
    }

    @Test
    public void testStrict10000() throws SqlException {
        callFn(utf8("{\"path\": 10000.0}"), utf8(".path"), FAIL_ON_ERROR).andAssert(10000.0f, DELTA_F);
    }

    @Test
    public void testStrictBigNumber() throws SqlException {
        callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path"), FAIL_ON_ERROR).andAssert(1.0E26f, DELTA_F);
    }

    @Test
    public void testStrictDefaultDifferentType() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), FAIL_ON_ERROR, 42.0f));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testStrictDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), FAIL_ON_ERROR, 42.0f).andAssertFloatNan();
    }

    @Test
    public void testStrictDifferentType() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), FAIL_ON_ERROR));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testStrictEmptyJson() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{}"), utf8(".path"), FAIL_ON_ERROR));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): NO_SUCH_FIELD:"));
    }

    @Test
    public void testStrictFloat() throws SqlException {
        callFn(utf8("{\"path\": 123.45}"), utf8(".path"), FAIL_ON_ERROR).andAssert(123.45f, DELTA_F);
    }

    @Test
    public void testStrictNegative() throws SqlException {
        callFn(utf8("{\"path\": -123\n}"), utf8(".path"), FAIL_ON_ERROR).andAssert(-123.0f, DELTA_F);
        callFn(utf8("{\"path\": -123.5\n}"), utf8(".path"), FAIL_ON_ERROR).andAssert(-123.5f, DELTA_F);
    }

    @Test
    public void testStrictNullJson() throws SqlException {
        callFn(utf8(null), utf8(".path"), FAIL_ON_ERROR).andAssertFloatNan();
    }

    @Test
    public void testStrictNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), FAIL_ON_ERROR).andAssertFloatNan();
    }

    @Test
    public void testStrictOne() throws SqlException {
        callFn(utf8("{\"path\": 1}"), utf8(".path"), FAIL_ON_ERROR).andAssert(1.0f, DELTA_F);
    }

    @Test
    public void testStrictOobNeg() throws SqlException {
        // -(2^31) -1: The first neg number that requires a 64-bit range.
        callFn(utf8("{\"path\": -2147483649\n}"), utf8(".path"), FAIL_ON_ERROR).andAssert(-2.147483649E9f, DELTA_F);
    }

    @Test
    public void testStrictOobPos() throws SqlException {
        // 2^31: The first pos number that requires a 64-bit range.
        // Number rounded during parsing as float.
        callFn(utf8("{\"path\": 2147483648\n}"), utf8(".path"), FAIL_ON_ERROR).andAssert(2.147483648E9f, DELTA_F);
    }

    @Test
    public void testStrictUnsigned64Bit() throws SqlException {
        // Number gets rounded up during parsing.
        callFn(utf8("{\"path\": 9999999999999999999}"), utf8(".path"), FAIL_ON_ERROR).andAssert(1.0E19f, DELTA_F);
    }

    @Test
    public void testStrictZero() throws SqlException {
        callFn(utf8("{\"path\": 0}"), utf8(".path"), FAIL_ON_ERROR).andAssert(0.0f, DELTA_F);
    }

    @Test
    public void testUnsigned64Bit() throws SqlException {
        callFn(utf8("{\"path\": 9999999999999999999}"), utf8(".path")).andAssert(1.0E19f, DELTA_F);
    }

    @Test
    public void testViaSql() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select '{\"path\": 2.5}'::varchar as path from long_sequence(10));");
        });

        assertMemoryLeak(() -> {
            assertSql(
                    "json_path\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n" +
                            "2.5000\n",
                    "select json_path(path, '.path', " + ColumnType.FLOAT + ") from x"
            );
        });
    }

    @Test
    public void testViaSqlDefault() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select '{\"path\": 2.5}'::varchar as path from long_sequence(3));");
        });

        assertMemoryLeak(() -> {
            assertSql(
                    "json_path\n" +
                            "10.2500\n" +
                            "10.2500\n" +
                            "10.2500\n",
                    "select json_path(path, '.bad[0].path', " + ColumnType.FLOAT + ", " + DEFAULT_VALUE_ON_ERROR + ", cast(10.25 as float)) from x"
            );
        });
    }

    @Test
    public void testZero() throws SqlException {
        callFn(utf8("{\"path\": 0}"), utf8(".path")).andAssert(0.0f, DELTA_F);
    }

    private Invocation callFn(Utf8Sequence json, Utf8Sequence path, Object... args) throws SqlException {
        final boolean[] forceConstants = new boolean[args.length + 3];
        Arrays.fill(forceConstants, true);
        forceConstants[0] = false;
        final Object[] newArgs = new Object[args.length + 3];
        newArgs[0] = json;
        newArgs[1] = path;
        newArgs[2] = (int) ColumnType.FLOAT;
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
