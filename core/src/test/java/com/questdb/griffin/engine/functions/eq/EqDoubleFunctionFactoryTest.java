/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.eq;

import com.questdb.cairo.sql.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.constants.DateConstant;
import com.questdb.griffin.engine.functions.constants.DoubleConstant;
import com.questdb.griffin.engine.functions.constants.FloatConstant;
import com.questdb.griffin.engine.functions.constants.TimestampConstant;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;
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
        Function function = factory.newInstance(args, 4, configuration);
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
        Function function = factory.newInstance(args, 4, configuration);
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
        Function function = factory.newInstance(args, 4, configuration);
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
        Function function = factory.newInstance(args, 4, configuration);
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
        Function function = factory.newInstance(args, 4, configuration);
        Assert.assertTrue(function.getBool(null));
        Assert.assertFalse(function.isConstant());
    }

    @Test
    public void testLeftNaNTimestamp() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DoubleConstant(2, Double.NaN));
        args.add(new TimestampConstant(1, 20000L));
        Function function = factory.newInstance(args, 4, configuration);
        Assert.assertFalse(function.getBool(null));
        Assert.assertTrue(function.isConstant());
    }

    @Test
    public void testLeftNaNDate() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DoubleConstant(2, Double.NaN));
        args.add(new DateConstant(1, 10000L));
        Function function = factory.newInstance(args, 4, configuration);
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testRightNaNDate() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DateConstant(1, 10000L));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration);
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testRightNaNDateNaN() throws SqlException {
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new DateConstant(1, Numbers.LONG_NaN));
        args.add(new DoubleConstant(2, Double.NaN));
        Function function = factory.newInstance(args, 4, configuration);
        Assert.assertTrue(function.getBool(null));
        Assert.assertTrue(function.isConstant());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqDoubleFunctionFactory();
    }
}