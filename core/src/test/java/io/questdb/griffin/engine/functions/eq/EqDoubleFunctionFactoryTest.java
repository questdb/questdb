/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class EqDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException {
        call(150.123, 150.123).andAssert(true);
    }

    @Test
    public void testEqualsNegative() throws SqlException {
        call(-150.123, -150.123).andAssert(true);
    }

    @Test
    public void testNotEquals() throws SqlException {
        call(10.123, 20.134).andAssert(false);
    }

    @Test
    public void testLeftNaN() throws SqlException {
        call(Double.NaN, 77.9).andAssert(false);
    }

    @Test
    public void testLeftNaNInt() throws SqlException {
        call(Double.NaN, 98).andAssert(false);
    }

    @Test
    public void testLeftNaNLong() throws SqlException {
        call(Double.NaN, 99099112312313100L).andAssert(false);
    }

    @Test
    public void testLeftNaNFloat() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new FloatConstant(1, 3.4f));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(null));
        Assert.assertTrue(function.isConstant());
    }

    @Test
    public void testRightNaNFloat() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DoubleConstant(2, Double.NaN));
        args.add(new FloatConstant(1, 5.1f) {
            @Override
            public boolean isConstant() {
                return false;
            }
        });
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(null));
        Assert.assertFalse(function.isConstant());
    }

    @Test
    public void testLeftNaNLongNaN() throws SqlException {
        // for constant expression this would generate
        // NaN = NaN the outcome will be false
        // however for col = NaN, where col is long this must be true
        callCustomised(false, false, Double.NaN, Numbers.LONG_NaN).andAssertOnlyColumnValues(true);
    }

    @Test
    public void testLeftNaNFloatNaN() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new FloatConstant(1, Float.NaN));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertTrue(function.getBool(null));
        Assert.assertTrue(function.isConstant());
    }

    @Test
    public void testLeftNaNIntNaN() throws SqlException {
        // for constant expression this would generate
        // NaN = NaN the outcome will be false
        // however for col = NaN, where col is long this must be true
        callCustomised(false, false, Double.NaN, Numbers.INT_NaN).andAssertOnlyColumnValues(true);
    }

    @Test
    public void testRightNaN() throws SqlException {
        call(77.1, Double.NaN).andAssert(false);
    }

    @Test
    public void testRightNaNInt() throws SqlException {
        call(123, Double.NaN).andAssert(false);
    }

    @Test
    public void testRightNaNLong() throws SqlException {
        call(9992290902224442L, Double.NaN).andAssert(false);
    }

    @Test
    public void testNullEqualsNull() throws SqlException {
        call(Double.NaN, Double.NaN).andAssert(true);
    }

    @Test
    public void testRightNaNTimestamp() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new TimestampConstant(1, 20000L));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testRightNaNTimestampNaN() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new TimestampConstant(1, Numbers.LONG_NaN) {
            @Override
            public boolean isConstant() {
                return false;
            }
        });
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertTrue(function.getBool(null));
        Assert.assertFalse(function.isConstant());
    }

    @Test
    public void testLeftNaNTimestamp() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DoubleConstant(2, Double.NaN));
        args.add(new TimestampConstant(1, 20000L));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(null));
        Assert.assertTrue(function.isConstant());
    }

    @Test
    public void testLeftNaNDate() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DoubleConstant(2, Double.NaN));
        args.add(new DateConstant(1, 10000L));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testRightNaNDate() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DateConstant(1, 10000L));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testRightNaNDateNaN() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DateConstant(1, Numbers.LONG_NaN));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration, sqlExecutionContext);
        Assert.assertTrue(function.getBool(null));
        Assert.assertTrue(function.isConstant());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqDoubleFunctionFactory();
    }
}