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

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.griffin.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import com.questdb.std.ObjHashSet;
import org.junit.Assert;
import org.junit.Test;

public class RndSymbolFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testFixedLength() throws SqlException {
        int symbolCount = 10;
        int symbolLen = 4;
        int nullRate = 2;
        Invocation invocation = call(symbolCount, symbolLen, symbolLen, nullRate);
        assertFixLen(symbolCount, symbolLen, nullRate, invocation.getFunction1());
        assertFixLen(symbolCount, symbolLen, nullRate, invocation.getFunction2());
    }

    @Test
    public void testFixedLengthNoNull() throws SqlException {
        int symbolCount = 10;
        int symbolLen = 4;
        int nullRate = 0;
        Invocation invocation = call(symbolCount, symbolLen, symbolLen, nullRate);
        assertFixLen(symbolCount, symbolLen, nullRate, invocation.getFunction1());
        assertFixLen(symbolCount, symbolLen, nullRate, invocation.getFunction2());
    }

    @Test
    public void testInvalidNullRate() {
        assertFailure(18, "rate must be positive", 10, 1, 2, -5);
    }

    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 10, 8, 6, 2);
    }

    @Test
    public void testNegativeLen() {
        assertFailure(0, "invalid range", 10, -8, -6, 2);
    }

    @Test
    public void testPositiveCount() {
        assertFailure(11, "invalid symbol count", 0, 4, 6, 2);
    }

    @Test
    public void testVarLength() throws SqlException {
        int symbolCount = 10;
        int symbolMin = 4;
        int symbolMax = 6;
        int nullRate = 2;
        Invocation invocation = call(symbolCount, symbolMin, symbolMax, nullRate);
        assertVarLen(symbolCount, symbolMin, symbolMax, nullRate, invocation.getFunction1());
        assertVarLen(symbolCount, symbolMin, symbolMax, nullRate, invocation.getFunction2());
    }

    @Test
    public void testVarLengthNoNull() throws SqlException {
        int symbolCount = 10;
        int symbolMin = 4;
        int symbolMax = 6;
        int nullRate = 0;
        Invocation invocation = call(symbolCount, symbolMin, symbolMax, nullRate);
        assertVarLen(symbolCount, symbolMin, symbolMax, nullRate, invocation.getFunction1());
        assertVarLen(symbolCount, symbolMin, symbolMax, nullRate, invocation.getFunction2());
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndSymbolFunctionFactory();
    }

    private void assertFixLen(int symbolCount, int symbolLen, int nullRate, Function function) {
        ObjHashSet<CharSequence> set = new ObjHashSet<>();
        int nullCount = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < 100; i++) {
            CharSequence value = function.getSymbol(null);
            if (value == null) {
                nullCount++;
            } else {
                if (value.length() < min) {
                    min = value.length();
                }

                if (value.length() > max) {
                    max = value.length();
                }
                set.add(value);
            }
        }

        if (nullRate > 0) {
            Assert.assertTrue(nullCount > 0);
        } else {
            Assert.assertEquals(0, nullCount);
        }
        Assert.assertTrue(set.size() <= symbolCount);
        Assert.assertEquals(symbolLen, max);
        Assert.assertEquals(symbolLen, min);
    }

    private void assertVarLen(int symbolCount, int symbolMin, int symbolMax, int nullRate, Function function) {
        ObjHashSet<CharSequence> set = new ObjHashSet<>();
        int nullCount = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < 100; i++) {
            CharSequence value = function.getSymbol(null);
            if (value == null) {
                nullCount++;
            } else {
                if (value.length() < min) {
                    min = value.length();
                }

                if (value.length() > max) {
                    max = value.length();
                }
                set.add(value);
            }
        }

        if (nullRate > 0) {
            Assert.assertTrue(nullCount > 0);
        } else {
            Assert.assertEquals(0, nullCount);
        }
        Assert.assertTrue(set.size() <= symbolCount);
        Assert.assertTrue(symbolMin <= min);
        Assert.assertTrue(symbolMax >= max);
        Assert.assertNotEquals(min, max);
    }
}