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

public class JsonPathFunctionFactoryBooleanTest extends AbstractFunctionFactoryTest {

    @Override
    public Invocation call(Object... args) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void test10000() throws SqlException {
        callFn(utf8("{\"path\": 10000}"), utf8(".path")).andAssert(false);
        callFn(utf8("{\"path\": 10000}"), dirUtf8(".path")).andAssert(false);
        callFn(dirUtf8("{\"path\": 10000}"), utf8(".path")).andAssert(false);
        callFn(dirUtf8("{\"path\": 10000}"), dirUtf8(".path")).andAssert(false);
    }

    @Test
    public void testBigNumber() throws SqlException {
        callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testDefaultDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, false).andAssert(false);
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, true).andAssert(true);
    }

    @Test
    public void testDefaultNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, true).andAssert(true);
        callFn(utf8("{\"path\": null}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, false).andAssert(false);
    }

    @Test
    public void testDifferentType() throws SqlException {
        callFn(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssert(false);
        callFn(dirUtf8("{\"path\": \"abc\"}"), dirUtf8(".path")).andAssert(false);
    }

    @Test
    public void testEmptyJson() throws SqlException {
        callFn(utf8("{}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testFalse() throws SqlException {
        callFn(utf8("{\"path\": false}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testFalseDefault() throws SqlException {
        callFn(utf8("{\"path\": false}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, true).andAssert(false);
    }

    @Test
    public void testFalseDefaultStrict() throws SqlException {
        callFn(utf8("{\"path\": false}"), utf8(".path"), FAIL_ON_ERROR, true).andAssert(false);
    }

    @Test
    public void testFalseStrict() throws SqlException {
        callFn(utf8("{\"path\": false}"), utf8(".path"), FAIL_ON_ERROR).andAssert(false);
    }

    @Test
    public void testFloat() throws SqlException {
        callFn(dirUtf8("{\"path\": 123.45}"), dirUtf8(".path")).andAssert(false);
    }

    @Test
    public void testInString() throws SqlException {
        callFn(utf8("{\"path\": \"123\"}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testNegative() throws SqlException {
        callFn(utf8("{\"path\": -123}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testNullJson() throws SqlException {
        callFn(
                utf8(null),
                utf8(".path")
        ).andAssert(false);
        callFn(
                utf8(null),
                utf8(".path"),
                DEFAULT_VALUE_ON_ERROR,
                true
        ).andAssert(true);
        callFn(
                utf8(null),
                dirUtf8(".path")
        ).andAssert(false);
        callFn(
                utf8(null),
                dirUtf8(".path"),
                DEFAULT_VALUE_ON_ERROR,
                true
        ).andAssert(true);
    }

    @Test
    public void testNullJsonValue() throws SqlException {
        callFn(utf8("{\"path\": null}"), utf8(".path")).andAssert(false);
        callFn(utf8("{\"path\": null}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, true).andAssert(true);
    }

    @Test
    public void testOne() throws SqlException {
        callFn(utf8("{\"path\": 1}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testStrictBigNumber() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path"), FAIL_ON_ERROR)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictDefaultDifferentType() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), FAIL_ON_ERROR, 42));
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictDefaultNullJsonValue() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": null}"), utf8(".path"), FAIL_ON_ERROR, 42)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictDifferentType() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": \"abc\"}"), utf8(".path"), FAIL_ON_ERROR));
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictEmptyJson() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{}"), utf8(".path"), FAIL_ON_ERROR));
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): NO_SUCH_FIELD:");
    }

    @Test
    public void testStrictFloat() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 123.45}"), utf8(".path"), FAIL_ON_ERROR));
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictNullJson() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8(null), utf8(".path"), FAIL_ON_ERROR)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): NO_SUCH_FIELD:");
    }

    @Test
    public void testStrictNullJsonValue() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": null}"), utf8(".path"), FAIL_ON_ERROR)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictOne() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 1}"), utf8(".path"), FAIL_ON_ERROR)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testStrictZero() throws SqlException {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> callFn(utf8("{\"path\": 0}"), utf8(".path"), FAIL_ON_ERROR)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path(.., '.path'): INCORRECT_TYPE:");
    }

    @Test
    public void testTrue() throws SqlException {
        callFn(utf8("{\"path\": true}"), utf8(".path")).andAssert(true);
    }

    @Test
    public void testTrueDefault() throws SqlException {
        callFn(utf8("{\"path\": true}"), utf8(".path"), DEFAULT_VALUE_ON_ERROR, false).andAssert(true);
    }

    @Test
    public void testTrueDefaultStrict() throws SqlException {
        callFn(utf8("{\"path\": true}"), utf8(".path"), FAIL_ON_ERROR, false).andAssert(true);
    }

    @Test
    public void testTrueStrict() throws SqlException {
        callFn(utf8("{\"path\": true}"), utf8(".path"), FAIL_ON_ERROR).andAssert(true);
    }

    @Test
    public void testUnsigned64Bit() throws SqlException {
        callFn(utf8("{\"path\": 9999999999999999999}"), utf8(".path")).andAssert(false);
    }

    @Test
    public void testViaSql() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select '{\"path\": true}'::varchar as path from long_sequence(10));");
        });

        assertMemoryLeak(() -> {
            assertSql(
                    "json_path\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n",
                    "select json_path(path, '.path', " + ColumnType.BOOLEAN + ") from x"
            );
        });
    }

    @Test
    public void testZero() throws SqlException {
        callFn(utf8("{\"path\": 0}"), utf8(".path")).andAssert(0);
    }

    private Invocation callFn(Utf8Sequence json, Utf8Sequence path, Object... args) throws SqlException {
        final boolean[] forceConstants = new boolean[args.length + 3];
        Arrays.fill(forceConstants, true);
        forceConstants[0] = false;
        final Object[] newArgs = new Object[args.length + 3];
        newArgs[0] = json;
        newArgs[1] = path;
        newArgs[2] = (int) ColumnType.BOOLEAN;
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
