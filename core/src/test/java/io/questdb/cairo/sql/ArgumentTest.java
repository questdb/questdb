/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ArgumentTest {
    @Test
    public void fromDynamicFunctionWithConstantFunctionTest() {
        int value = new Random().nextInt();
        Function constantFunc = new MockConstantFunction(value);
        Argument<Integer> argument = Argument.fromDynamicFunction(constantFunc, "arg", 0, func -> func::getInt);

        Assert.assertTrue(argument instanceof Argument.ConstantArgument);
        Assert.assertEquals(((Argument.ConstantArgument<Integer>) argument).getValue().intValue(), value);
    }

    @Test
    public void fromDynamicFunctionWithRuntimeConstantFunctionTest() {
        int value = new Random().nextInt();
        Function runtimeConstantFunc = new MockRuntimeConstantFunction(value);
        Argument<Integer> argument = Argument.fromDynamicFunction(runtimeConstantFunc, "arg", 0, func -> func::getInt);

        Assert.assertTrue(argument instanceof Argument.RuntimeConstantArgument);
        Assert.assertEquals(((Argument.RuntimeConstantArgument<Integer>) argument).getValue().intValue(), value);
    }

    @Test
    public void fromDynamicFunctionWithDynamicFunctionTest() {
        int value = new Random().nextInt();
        Function dynamicFunc = new MockDynamicFunction(value);
        Argument<Integer> argument = Argument.fromDynamicFunction(dynamicFunc, "arg", 0, func -> func::getInt);

        Assert.assertTrue(argument instanceof Argument.DynamicArgument);
        Assert.assertEquals(argument.getValue(null).intValue(), value);
    }

    @Test
    public void fromRuntimeConstantFunctionWithConstantFunctionTest() throws SqlException {
        int value = new Random().nextInt();
        Function constantFunc = new MockConstantFunction(value);
        Argument<Integer> argument = Argument.fromRuntimeConstantFunction(constantFunc, "arg", 0, func -> func::getInt);

        Assert.assertTrue(argument instanceof Argument.ConstantArgument);
        Assert.assertEquals(((Argument.ConstantArgument<Integer>) argument).getValue().intValue(), value);
    }

    @Test
    public void fromRuntimeConstantFunctionWithRuntimeConstantFunctionTest() throws SqlException {
        int value = new Random().nextInt();
        Function runtimeConstantFunc = new MockRuntimeConstantFunction(value);
        Argument<Integer> argument = Argument.fromRuntimeConstantFunction(runtimeConstantFunc, "arg", 0, func -> func::getInt);

        Assert.assertTrue(argument instanceof Argument.RuntimeConstantArgument);
        Assert.assertEquals(((Argument.RuntimeConstantArgument<Integer>) argument).getValue().intValue(), value);
    }

    @Test
    public void fromRuntimeConstantFunctionWithDynamicFunctionTest() {
        int value = new Random().nextInt();
        Function dynamicFunc = new MockDynamicFunction(value);

        try {
            Argument.fromRuntimeConstantFunction(dynamicFunc, "arg", 0, func -> func::getInt);
            Assert.fail("Should fail for dynamic argument function");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "arg must be a constant or runtime-constant");
        }
    }

    @Test
    public void fromConstantFunctionWithConstantFunctionTest() throws SqlException {
        int value = new Random().nextInt();
        Function constantFunc = new MockConstantFunction(value);
        Argument<Integer> argument = Argument.fromConstantFunction(constantFunc, "arg", 0, func -> func::getInt);

        Assert.assertTrue(argument instanceof Argument.ConstantArgument);
        Assert.assertEquals(((Argument.ConstantArgument<Integer>) argument).getValue().intValue(), value);
    }

    @Test
    public void fromConstantFunctionWithRuntimeConstantFunctionTest() {
        int value = new Random().nextInt();
        Function runtimeConstantFunc = new MockRuntimeConstantFunction(value);

        try {
            Argument.fromConstantFunction(runtimeConstantFunc, "arg", 0, func -> func::getInt);
            Assert.fail("Should fail for runtime constant argument function");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "arg must be a constant");
        }
    }

    @Test
    public void fromConstantFunctionWithDynamicFunctionTest() {
        int value = new Random().nextInt();
        Function dynamicFunc = new MockDynamicFunction(value);

        try {
            Argument.fromConstantFunction(dynamicFunc, "arg", 0, func -> func::getInt);
            Assert.fail("Should fail for dynamic argument function");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "arg must be a constant");
        }
    }

    @Test
    public void validateDynamicArgumentTest() throws SqlException {
        Function dynamicFunc = new MockDynamicFunction();
        Argument<Integer> dynamicArg = Argument.fromDynamicFunction(dynamicFunc, "arg", 0, func -> func::getInt);
        Function fallback = dynamicArg.validateConstant((value, argName, argPosition) -> {
            throw SqlException.$(argPosition, "constant validation failed");
        });
        dynamicArg.registerRuntimeConstantValidator((value, argName, argPosition) -> {
            throw SqlException.$(argPosition, "runtime constant validation failed");
        });

        // validate() should skip constant validations for dynamic args
        Assert.assertNull(fallback);

        // init() should skip runtime constant validations for dynamic args
        dynamicArg.init(null, null);
    }

    @Test
    public void validateRuntimeConstantArgumentTest() throws SqlException {
        Function runtimeConstantFunc = new MockRuntimeConstantFunction();
        Argument<Integer> runtimeConstantArg = Argument.fromRuntimeConstantFunction(runtimeConstantFunc, "arg", 0, func -> func::getInt);
        Function fallback = runtimeConstantArg.validateConstant((value, argName, argPosition) -> {
            throw SqlException.$(argPosition, "constant validation failed");
        });
        runtimeConstantArg.registerRuntimeConstantValidator((value, argName, argPosition) -> {
            throw SqlException.$(argPosition, "runtime constant validation failed");
        });

        // validate() should skip constant validations for runtime constant args
        Assert.assertNull(fallback);

        try {
            runtimeConstantArg.init(null, null);
            Assert.fail("Should fail due to runtime constant validation");
        } catch (SqlException e) {
            TestUtils.assertEquals(e.getFlyweightMessage(), "runtime constant validation failed");
        }
    }

    @Test
    public void validateConstantArgumentWithoutFallbackTest() throws SqlException {
        Function constantFunc = new MockConstantFunction();
        Argument<Integer> constantArg = Argument.fromConstantFunction(constantFunc, "arg", 0, func -> func::getInt);

        try {
            constantArg.validateConstant((value, argName, argPosition) -> {
                throw SqlException.$(argPosition, "constant validation failed");
            });
            Assert.fail("Should fail due to constant validation");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "constant validation failed");
        }

        constantArg.registerRuntimeConstantValidator((value, argName, argPosition) -> {
            throw SqlException.$(argPosition, "runtime constant validation failed");
        });

        // init() should skip runtime constant validations for constant args
        constantArg.init(null, null);
    }

    @Test
    public void validateConstantArgumentWithFallbackTest() throws SqlException {
        Function constantFunc = new MockConstantFunction();
        Argument<Integer> constantArg = Argument.fromConstantFunction(constantFunc, "arg", 0, func -> func::getInt);
        Function fallback = constantArg.validateConstant((value, argName, argPosition) -> StrConstant.EMPTY);
        constantArg.registerRuntimeConstantValidator((value, argName, argPosition) -> {
            throw SqlException.$(argPosition, "runtime constant validation failed");
        });

        Assert.assertEquals(fallback, StrConstant.EMPTY);

        // init() should skip runtime constant validations for constant args
        constantArg.init(null, null);
    }

    @Test
    public void initTest() throws SqlException {
        MockConstantFunction constantFunc = new MockConstantFunction();
        MockRuntimeConstantFunction runtimeConstantFunc = new MockRuntimeConstantFunction();
        MockDynamicFunction dynamicFunc = new MockDynamicFunction();
        Argument<Integer> constantArgument = Argument.fromConstantFunction(constantFunc, "constant", 0, func -> func::getInt);
        Argument<Integer> runtimeConstantArgument = Argument.fromRuntimeConstantFunction(runtimeConstantFunc, "runtimeConstant", 1, func -> func::getInt);
        Argument<Integer> dynamicArgument = Argument.fromDynamicFunction(dynamicFunc, "dynamic", 2, func -> func::getInt);
        constantArgument.init(null, null);
        runtimeConstantArgument.init(null, null);
        dynamicArgument.init(null, null);

        Assert.assertFalse(constantFunc.init);
        Assert.assertTrue(runtimeConstantFunc.init);
        Assert.assertTrue(dynamicFunc.init);
    }

    private static class MockConstantFunction extends IntConstant {
        private boolean init;

        public MockConstantFunction() {
            this(new Random().nextInt());
        }

        public MockConstantFunction(int value) {
            super(value);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            init = true;
        }
    }

    ;

    private static class MockRuntimeConstantFunction extends IntConstant {
        private boolean init;

        public MockRuntimeConstantFunction() {
            this(new Random().nextInt());
        }

        public MockRuntimeConstantFunction(int value) {
            super(value);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            init = true;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }

    private static class MockDynamicFunction extends IntFunction {
        private final int value;
        private boolean init;

        public MockDynamicFunction() {
            this(new Random().nextInt());
        }

        public MockDynamicFunction(int value) {
            this.value = value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            init = true;
        }

        @Override
        public int getInt(Record rec) {
            return value;
        }
    }
}
