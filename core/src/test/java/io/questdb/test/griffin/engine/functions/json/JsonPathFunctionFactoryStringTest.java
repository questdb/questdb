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

public class JsonPathFunctionFactoryStringTest extends AbstractFunctionFactoryTest {

    @Override
    public Invocation call(Object... args) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void test10000() {
        final SqlException exc = Assert.assertThrows(
                SqlException.class,
                () -> callFn(utf8("{\"path\": 10000}"), utf8(".path"), FAIL_ON_ERROR)
        );
        Assert.assertTrue(exc.getMessage().contains("unsupported target type"));
    }

    @Test
    public void testString() {
        final SqlException exc = Assert.assertThrows(
                SqlException.class,
                () -> callFn(dirUtf8("{\"path\": \"X\"}"), utf8(".path"), FAIL_ON_ERROR)
        );
        Assert.assertTrue(exc.getMessage().contains("unsupported target type"));
    }

    @Test
    public void testNullJson() throws SqlException {
        {
            final SqlException exc = Assert.assertThrows(
                    SqlException.class,
                    () -> callFn(utf8(null), utf8(".path"), FAIL_ON_ERROR)
            );
            Assert.assertTrue(exc.getMessage().contains("unsupported target type"));
        }

        {
            final SqlException exc = Assert.assertThrows(
                    SqlException.class,
                    () -> callFn(utf8(null), dirUtf8(".path"))
            );
            Assert.assertTrue(exc.getMessage().contains("unsupported target type"));
        }
    }

    private Invocation callFn(Utf8Sequence json, Utf8Sequence path, Object... args) throws SqlException {
        final boolean[] forceConstants = new boolean[args.length + 3];
        Arrays.fill(forceConstants, true);
        forceConstants[0] = false;
        final Object[] newArgs = new Object[args.length + 3];
        newArgs[0] = json;
        newArgs[1] = path;
        newArgs[2] = (int) ColumnType.STRING;
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
