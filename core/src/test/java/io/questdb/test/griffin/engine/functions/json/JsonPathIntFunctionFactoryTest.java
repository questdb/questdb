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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.json.JsonPathIntFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class JsonPathIntFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNullJson() throws SqlException {
        call(utf8(null), utf8(".path")).andAssert(Integer.MIN_VALUE);
        call(utf8(null), dirUtf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testDifferentType() throws SqlException {
        call(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
        call(dirUtf8("{\"path\": \"abc\"}"), dirUtf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testEmptyJson() throws SqlException {
        call(utf8("{}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testNullJsonValue() throws SqlException {
        call(utf8("{\"path\": null}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
        call(utf8("{\"path\": null}"), dirUtf8(".path")).andAssert(Integer.MIN_VALUE);
        call(dirUtf8("{\"path\": null}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
        call(dirUtf8("{\"path\": null}"), dirUtf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testZero() throws SqlException {
        call(utf8("{\"path\": 0}"), utf8(".path")).andAssert(0);
    }

    @Test
    public void testOne() throws SqlException {
        call(utf8("{\"path\": 1}"), utf8(".path")).andAssert(1);
    }

    @Test
    public void test10000() throws SqlException {
        call(utf8("{\"path\": 10000}"), utf8(".path")).andAssert(10000);
        call(utf8("{\"path\": 10000}"), dirUtf8(".path")).andAssert(10000);
        call(dirUtf8("{\"path\": 10000}"), utf8(".path")).andAssert(10000);
        call(dirUtf8("{\"path\": 10000}"), dirUtf8(".path")).andAssert(10000);
    }

    @Test
    public void testNegative() throws SqlException {
        call(utf8("{\"path\": -123}"), utf8(".path")).andAssert(-123);
    }

    @Test
    public void testUnsigned64Bit() throws SqlException {
        call(utf8("{\"path\": 9999999999999999999}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testBigNumber() throws SqlException {
        call(utf8("{\"path\": 100000000000000000000000000}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testFloat() throws SqlException {
        call(dirUtf8("{\"path\": 123.45}"), dirUtf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Test
    public void testStringContainingNumber() throws SqlException {
        call(utf8("{\"path\": \"123\"}"), utf8(".path")).andAssert(Integer.MIN_VALUE);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathIntFunctionFactory();
    }
}
