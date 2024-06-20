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
import io.questdb.griffin.engine.functions.json.JsonPathStrictFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.std.json.JsonParserTest;
import org.junit.Assert;
import org.junit.Test;

public class JsonPathStrictFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNullJson() throws SqlException {
        call(utf8(null), utf8(".path")).andAssertUtf8(null);
    }

    @Test
    public void testNullPath() {
        Assert.assertThrows(SqlException.class, () -> call(utf8("{}"), null));
    }

    @Test
    public void testEmptyJson() {
        final CairoException exc = Assert.assertThrows(
                CairoException.class,
                () -> call(utf8("{}"), utf8(".path")));
        Assert.assertTrue(exc.getMessage().contains("json_path(.., '.path'): NO_SUCH_FIELD:"));
    }

    @Test
    public void testEmptyPath() throws SqlException {
        call(utf8("{\"path\": 1}"), utf8("")).andAssertUtf8("{\"path\": 1}");
    }

    @Test
    public void testSimplePath() throws SqlException {
        call(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssertUtf8("abc");
    }

    @Test
    public void testLargeJson() throws SqlException {
        call(utf8(JsonParserTest.jsonStr), utf8(".name")).andAssertUtf8("John");
    }

    @Test
    public void testRawExtraction() throws SqlException {
        call(utf8("{\"path\": 123}"), utf8(".path")).andAssertUtf8("123");
        call(utf8("{\"path\": [1, 2,  3, {\"x\": true}, 4.5, null]}"), utf8(".path")).andAssertUtf8("[1, 2,  3, {\"x\": true}, 4.5, null]");
        call(utf8("{\"path\": {\"x\": true}}"), utf8(".path")).andAssertUtf8("{\"x\": true}");
        call(utf8("{\"path\": {\"x\": true}}"), utf8(".path.x")).andAssertUtf8("true");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathStrictFunctionFactory();
    }
}
