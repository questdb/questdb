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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.JsonPathVarcharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.std.json.JsonTest;
import org.junit.Test;

public class JsonPathVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNullJson() throws SqlException {
        call(utf8(null), utf8(".path")).andAssertUtf8(null);
    }

    @Test
    public void testNullPath() throws SqlException {
        call(utf8("{}"), utf8(null)).andAssertUtf8(null);
    }

    @Test
    public void testEmptyJson() throws SqlException {
        call(utf8("{}"), utf8(".path")).andAssertUtf8(null);
    }

    @Test
    public void testEmptyPath() throws SqlException {
        call(utf8("{\"path\": 1}"), utf8("")).andAssertUtf8(null);
    }

    @Test
    public void testSimplePath() throws SqlException {
        call(utf8("{\"path\": \"abc\"}"), utf8(".path")).andAssertUtf8("abc");
    }

    @Test
    public void testLargeJson() throws SqlException {
        call(utf8(JsonTest.jsonStr), utf8(".name")).andAssertUtf8("John");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathVarcharFunctionFactory();
    }
}
