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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.json.JsonPathFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.griffin.engine.functions.json.JsonPathFunc.DEFAULT_VALUE_ON_ERROR;
import static io.questdb.griffin.engine.functions.json.JsonPathFunc.FAIL_ON_ERROR;

public class JsonPathIntFunctionFactoryTest extends AbstractFunctionFactoryTest {
    private static final int INT = io.questdb.cairo.ColumnType.INT;

    @Override
    public Invocation call(Object... args) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testInt10000() throws SqlException {
        callFn(utf8("{\"path\": 10000}"), utf8(".path"), INT).andAssert(10000);
        callFn(utf8("{\"path\": 10000}"), dirUtf8(".path"), INT).andAssert(10000);
        callFn(dirUtf8("{\"path\": 10000}"), utf8(".path"), INT).andAssert(10000);
        callFn(dirUtf8("{\"path\": 10000}"), dirUtf8(".path"), INT).andAssert(10000);
    }

    @Test
    public void testIntBigNumber() throws SqlException {
        callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path"), INT).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntDefaultDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), INT, DEFAULT_VALUE_ON_ERROR, 42).andAssert(42);
    }

    @Test
    public void testIntDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), INT, DEFAULT_VALUE_ON_ERROR, 42).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), INT).andAssert(Integer.MIN_VALUE);
        callFn(dirUtf8("{\"path\": \"abc\"}"), dirUtf8(".path"), INT).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntEmptyJson() throws SqlException {
        callFn(utf8("{}"), utf8(".path"), INT).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntFloat() throws SqlException {
        callFn(dirUtf8("{\"path\": 123.45}"), dirUtf8(".path"), INT).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntInString() throws SqlException {
        callFn(utf8("{\"path\": \"123\"}"), utf8(".path"), INT).andAssert(123);
    }

    @Test
    public void testIntNegative() throws SqlException {
        callFn(utf8("{\"path\": -123}"), utf8(".path"), INT).andAssert(-123);
    }

    @Test
    public void testIntNullJson() throws SqlException {
        callFn(
                utf8(null),
                utf8(".path"),
                INT
        ).andAssert(Integer.MIN_VALUE);
        callFn(
                utf8(null),
                dirUtf8(".path"),
                INT
        ).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), INT).andAssert(Integer.MIN_VALUE);
        callFn(utf8("{\"path\": null}"), dirUtf8(".path"), INT).andAssert(Integer.MIN_VALUE);
        callFn(dirUtf8("{\"path\": null}"), utf8(".path"), INT).andAssert(Integer.MIN_VALUE);
        callFn(dirUtf8("{\"path\": null}"), dirUtf8(".path"), INT).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntOne() throws SqlException {
        callFn(utf8("{\"path\": 1}"), utf8(".path"), INT).andAssert(1);
    }

    @Test
    public void testIntStrict10000() throws SqlException {
        callFn(utf8("{\"path\": 10000}"), utf8(".path"), INT, FAIL_ON_ERROR).andAssert(10000);
    }

    @Test
    public void testIntStrictBigNumber() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path"), INT, FAIL_ON_ERROR)
        );
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testIntStrictDefaultDifferentType() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), INT, FAIL_ON_ERROR, 42));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testIntStrictDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), INT, FAIL_ON_ERROR, 42).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntStrictDifferentType() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), INT, FAIL_ON_ERROR));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testIntStrictEmptyJson() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{}"), utf8(".path"), INT, FAIL_ON_ERROR));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): NO_SUCH_FIELD:"));
    }

    @Test
    public void testIntStrictFloat() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 123.45}"), utf8(".path"), INT, FAIL_ON_ERROR));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testIntStrictNegative() throws SqlException {
        callFn(utf8("{\"path\": -123\n}"), utf8(".path"), INT, FAIL_ON_ERROR).andAssert(-123);
    }

    @Test
    public void testIntStrictNullJson() throws SqlException {
        callFn(utf8(null), utf8(".path"), INT, FAIL_ON_ERROR).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntStrictNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), INT, FAIL_ON_ERROR).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntStrictOne() throws SqlException {
        callFn(utf8("{\"path\": 1}"), utf8(".path"), INT, FAIL_ON_ERROR).andAssert(1);
    }

    @Test
    public void testIntStrictOobNeg() throws SqlException {
        // -(2^31) -1: The first neg number that requires a 64-bit range.
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": -2147483649\n}"), utf8(".path"), INT, FAIL_ON_ERROR)
        );
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): NUMBER_OUT_OF_RANGE"));
    }

    @Test
    public void testIntStrictOobPos() throws SqlException {
        // 2^31: The first pos number that requires a 64-bit range.
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 2147483648\n}"), utf8(".path"), INT, FAIL_ON_ERROR)
        );
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): NUMBER_OUT_OF_RANGE"));
    }

    @Test
    public void testIntStrictUnsigned64Bit() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 9999999999999999999}"), utf8(".path"), INT, FAIL_ON_ERROR)
        );
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): INCORRECT_TYPE:"));
    }

    @Test
    public void testIntStrictZero() throws SqlException {
        callFn(utf8("{\"path\": 0}"), utf8(".path"), INT, FAIL_ON_ERROR).andAssert(0);
    }

    @Test
    public void testIntUnsigned64Bit() throws SqlException {
        callFn(utf8("{\"path\": 9999999999999999999}"), utf8(".path"), INT).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testIntViaSql() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select '{\"path\": 2}'::varchar as path from long_sequence(10));");
        });

        assertMemoryLeak(() -> {
            assertSql(
                    "json_path\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n" +
                            "2\n",
                    "select json_path(path, '.path', " + INT + ") from x"
            );
        });
    }

    @Test
    public void testIntZero() throws SqlException {
        callFn(utf8("{\"path\": 0}"), utf8(".path"), INT).andAssert(0);
    }

    private Invocation callFn(Object... args) throws SqlException {
        final boolean[] forceConstants = new boolean[args.length];
        Arrays.fill(forceConstants, true);
        forceConstants[0] = false;
        return callCustomised(
                null,
                forceConstants,
                true,
                args);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathFunctionFactory();
    }
}
