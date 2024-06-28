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
import io.questdb.griffin.engine.functions.json.JsonPathStrictVarcharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JsonPathStrictVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void test10000() throws SqlException {
        call(utf8("{\"path\": 10000.5}"), utf8(".path")).andAssert("10000.5");
        call(utf8("{\"path\": 10000.5}"), dirUtf8(".path")).andAssert("10000.5");
        call(dirUtf8("{\"path\": 10000.5}"), utf8(".path")).andAssert("10000.5");
        call(dirUtf8("{\"path\": 10000.5}"), dirUtf8(".path")).andAssert("10000.5");
    }

    @Test
    public void testBigNumber() throws SqlException {
        call(
                utf8("{\"path\": 100000000000000000000000000}"),
                utf8(".path")
        ).andAssert("100000000000000000000000000");
    }

    @Test
    public void testDblPrecision() throws SqlException {
        call(
                utf8("{\"path\": 0.0000000000000000000000000001}"),
                utf8(".path")
        ).andAssert("0.0000000000000000000000000001");
    }

    @Test
    public void testString() throws SqlException {
        call(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssert("abc");
        call(dirUtf8("{\"path\": \"abc\"}"), dirUtf8(".path")).andAssert("abc");
    }

    @Test
    public void testEmptyJson() {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> call(utf8("{}"), utf8(".path")).andAssert((String) null)
        );
        TestUtils.assertContains(exc.getMessage(), "json_path_s(.., '.path'): NO_SUCH_FIELD");
    }

    @Test
    public void testInt() throws SqlException {
        call(dirUtf8("{\"path\": 123}"), dirUtf8(".path")).andAssert("123");
    }

    @Test
    public void testNegative() throws SqlException {
        call(utf8("{\"path\": -123.5}"), utf8(".path")).andAssert("-123.5");
    }

    @Test
    public void testNullJson() throws SqlException {
        call(
                utf8(null),
                utf8(".path")
        ).andAssert((String) null);
        call(
                utf8(null),
                dirUtf8(".path")
        ).andAssert((String) null);
    }

    @Test
    public void testNullJsonValue() throws SqlException {
        call(utf8("{\"path\": null}"), utf8(".path")).andAssert((String) null);
        call(utf8("{\"path\": null}"), dirUtf8(".path")).andAssert((String) null);
        call(dirUtf8("{\"path\": null}"), utf8(".path")).andAssert((String) null);
        call(dirUtf8("{\"path\": null}"), dirUtf8(".path")).andAssert((String) null);
    }

    @Test
    public void testOne() throws SqlException {
        call(utf8("{\"path\": 1}"), utf8(".path")).andAssert("1");
    }

    @Test
    public void testUnsigned64Bit() throws SqlException {
        call(
                utf8("{\"path\": 9999999999999999999}"),
                utf8(".path")
        ).andAssert("9999999999999999999");
    }

    @Test
    public void testArray() throws SqlException {
        call(utf8("{\"path\": [1, 2, 3]}"), utf8(".path")).andAssert("[1, 2, 3]");
    }

    @Test
    public void testDict() throws SqlException {
        call(
                utf8("{\"path\": {\"a\": 1, \"b\": 2}}"),
                utf8(".path")
        ).andAssert("{\"a\": 1, \"b\": 2}");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathStrictVarcharFunctionFactory();
    }
}
