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
import io.questdb.griffin.engine.functions.json.JsonPathDefaultTypedExplicitDefaultFunctionFactory;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class JsonPathDefaultTypedExplicitDefaultFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Override
    public Invocation call(Object... args) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testNullDefaultBoolean() {
        final SqlException exc = Assert.assertThrows(
                SqlException.class,
                () -> callFn(
                        utf8("{\"path\": true}"),
                        utf8(".path"),
                        ColumnType.BOOLEAN,
                        (Object) null
                ));
        TestUtils.assertContains(exc.getMessage(), "json_path's the default value cannot be NULL");
    }

    @Test
    public void testMatchingTypeBoolean() throws SqlException {
        callFn(
                utf8("{\"path\": false}"),
                utf8(".path"),
                ColumnType.BOOLEAN,
                true
        ).andAssert(false);
    }

    @Test
    public void testMatchingTypeBoolean2() throws SqlException {
        callFn(
                utf8("{\"path\": true}"),
                utf8(".path"),
                ColumnType.BOOLEAN,
                false
        ).andAssert(true);
    }

    @Test
    public void testMatchingTypeDouble() throws SqlException {
        callFn(
                utf8("{\"path\": 0.5}"),
                utf8(".path"),
                ColumnType.DOUBLE,
                1.5
        ).andAssert(.5, DELTA);
    }

    @Test
    public void testMatchingTypeFloat() throws SqlException {
        callFn(
                utf8("{\"path\": 0.5}"),
                utf8(".path"),
                ColumnType.FLOAT,
                1.5f
        ).andAssert(.5f, DELTA_F);
    }

    @Test
    public void testMatchingTypeInt() throws SqlException {
        callFn(
                utf8("{\"path\": 42}"),
                utf8(".path"),
                ColumnType.INT,
                43
        ).andAssert(42);
        callFn(
                utf8("{\"path\": 42}"),
                dirUtf8(".path"),
                ColumnType.INT,
                43
        ).andAssert(42);
    }

    @Test
    public void testMatchingTypeLong() throws SqlException {
        callFn(
                dirUtf8("{\"path\": 42}"),
                dirUtf8(".path"),
                ColumnType.LONG,
                43L
        ).andAssert(42L);
    }

    @Test
    public void testMatchingTypeLong2() throws SqlException {
        callFn(
                dirUtf8("{\"path\": 42}"),
                dirUtf8(".path"),
                ColumnType.LONG,
                43
        ).andAssert(42L);
    }

    @Test
    public void testMatchingTypeLong3() throws SqlException {
        callFn(
                dirUtf8("{\"path\": 42}"),
                dirUtf8(".path"),
                ColumnType.LONG,
                (short) 43
        ).andAssert(42L);
    }

    @Test
    public void testMatchingTypeShort() throws SqlException {
        callFn(
                dirUtf8("{\"path\": 42}"),
                dirUtf8(".path"),
                ColumnType.SHORT,
                (short)43
        ).andAssert((short)42);
    }

    @Test
    public void testMatchingTypeVarchar() throws SqlException {
        callFn(
                utf8("{\"path\": \"abc\"}"),
                utf8(".path"),
                ColumnType.VARCHAR,
                utf8("def")
        ).andAssert("abc");
        callFn(
                utf8("{\"path\": \"abc\"}"),
                utf8(".path"),
                ColumnType.VARCHAR,
                dirUtf8("def")
        ).andAssert("abc");
    }

    private Invocation callFn(Utf8Sequence json, Utf8Sequence path, int targetType, Object... args) throws SqlException {
        final boolean[] forceConstants = new boolean[args.length + 3];
        Arrays.fill(forceConstants, true);
        forceConstants[0] = false;
        final Object[] newArgs = new Object[args.length + 3];
        newArgs[0] = json;
        newArgs[1] = path;
        newArgs[2] = targetType;
        System.arraycopy(args, 0, newArgs, 3, args.length);
        return callCustomised(
                null,
                forceConstants,
                true,
                newArgs);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new JsonPathDefaultTypedExplicitDefaultFunctionFactory();
    }
}
