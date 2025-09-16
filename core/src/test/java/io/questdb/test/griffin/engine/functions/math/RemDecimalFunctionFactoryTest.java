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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.RemDecimalFunctionFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class RemDecimalFunctionFactoryTest extends AbstractTest {
    private static final RemDecimalFunctionFactory factory = new RemDecimalFunctionFactory();

    @Test
    public void testRemDecimal128DivideByZero() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)));
        args.add(new Decimal128Constant(0, 0, ColumnType.getDecimalType(19, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(19, 2));
    }

    @Test
    public void testRemDecimal128Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 500, ColumnType.getDecimalType(20, 2))); // 5.00
        args.add(new Decimal128Constant(0, 300, ColumnType.getDecimalType(20, 2))); // 3.00
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(20, 2)); // 2.00
    }

    @Test
    public void testRemDecimal128WithCarry() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 1000, ColumnType.getDecimalType(20, 2))); // 10.00
        args.add(new Decimal128Constant(0, 300, ColumnType.getDecimalType(20, 2))); // 3.00
        createFunctionAndAssert(args, 0, 0, 0, 100, ColumnType.getDecimalType(20, 2)); // 1.00
    }

    @Test
    public void testRemDecimal128WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 500, ColumnType.getDecimalType(19, 2))); // 5.00
        args.add(new Decimal128Constant(0, 15, ColumnType.getDecimalType(19, 1))); // 1.5
        createFunctionAndAssert(args, 0, 0, 0, 50, ColumnType.getDecimalType(19, 2)); // 0.50
    }

    @Test
    public void testRemDecimal128WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)));
        args.add(new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(20, 2));
    }

    @Test
    public void testRemDecimal16DivideByZero() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)));
        args.add(new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(4, 2));
    }

    @Test
    public void testRemDecimal16Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 500, ColumnType.getDecimalType(4, 2))); // 5.00
        args.add(new Decimal16Constant((short) 300, ColumnType.getDecimalType(4, 2))); // 3.00
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(4, 2)); // 2.00
    }

    @Test
    public void testRemDecimal16WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 500, ColumnType.getDecimalType(4, 2))); // 5.00
        args.add(new Decimal16Constant((short) 15, ColumnType.getDecimalType(4, 1))); // 1.5
        createFunctionAndAssert(args, 0, 0, 0, 50, ColumnType.getDecimalType(4, 2)); // 0.50
    }

    @Test
    public void testRemDecimal16WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)));
        args.add(new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(4, 2));
    }

    @Test
    public void testRemDecimal256DivideByZero() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(40, 2));
    }

    @Test
    public void testRemDecimal256MaxPrecision() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 500, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)));
        args.add(new Decimal256Constant(0, 0, 0, 300, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)));
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE));
    }

    @Test
    public void testRemDecimal256Negative() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, -10, ColumnType.getDecimalType(40, 0))); // -10
        args.add(new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(40, 0))); // 1
        createFunctionAndAssert(args, 0, 0, 0, 0, ColumnType.getDecimalType(40, 0)); // 0
    }

    @Test
    public void testRemDecimal256Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 500, ColumnType.getDecimalType(40, 2))); // 5.00
        args.add(new Decimal256Constant(0, 0, 0, 300, ColumnType.getDecimalType(40, 2))); // 3.00
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(40, 2)); // 2.00
    }

    @Test
    public void testRemDecimal256WithCarry() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(39, 0))); // large number
        args.add(new Decimal256Constant(0, 0, 0, 3, ColumnType.getDecimalType(39, 0))); // 3
        createFunctionAndAssert(args, 0, 0, 0, 1, ColumnType.getDecimalType(39, 0)); // 1
    }

    @Test
    public void testRemDecimal256WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                ColumnType.getDecimalType(40, 0))
        );
        args.add(new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(40, 2));
    }

    @Test
    public void testRemDecimal32DivideByZero() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)));
        args.add(new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(8, 2));
    }

    @Test
    public void testRemDecimal32Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(500, ColumnType.getDecimalType(8, 2))); // 5.00
        args.add(new Decimal32Constant(300, ColumnType.getDecimalType(8, 2))); // 3.00
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(8, 2)); // 2.00
    }

    @Test
    public void testRemDecimal32WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(500, ColumnType.getDecimalType(9, 2))); // 5.00
        args.add(new Decimal32Constant(15, ColumnType.getDecimalType(9, 1))); // 1.5
        createFunctionAndAssert(args, 0, 0, 0, 50, ColumnType.getDecimalType(9, 2)); // 0.50
    }

    @Test
    public void testRemDecimal32WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(100, ColumnType.getDecimalType(9, 2)));
        args.add(new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(9, 2));
    }

    @Test
    public void testRemDecimal64DivideByZero() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)));
        args.add(new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(10, 2));
    }

    @Test
    public void testRemDecimal64Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(500, ColumnType.getDecimalType(10, 2))); // 5.00
        args.add(new Decimal64Constant(300, ColumnType.getDecimalType(10, 2))); // 3.00
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(10, 2)); // 2.00
    }

    @Test
    public void testRemDecimal64WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(500, ColumnType.getDecimalType(14, 2))); // 5.00
        args.add(new Decimal64Constant(15, ColumnType.getDecimalType(14, 1))); // 1.5
        createFunctionAndAssert(args, 0, 0, 0, 50, ColumnType.getDecimalType(14, 2)); // 0.50
    }

    @Test
    public void testRemDecimal64WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(100, ColumnType.getDecimalType(15, 2)));
        args.add(new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(15, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(15, 2));
    }

    @Test
    public void testRemDecimal8DivideByZero() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)));
        args.add(new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(2, 1));
    }

    @Test
    public void testRemDecimal8Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 1))); // 5.0
        args.add(new Decimal8Constant((byte) 30, ColumnType.getDecimalType(2, 1))); // 3.0
        createFunctionAndAssert(args, 0, 0, 0, 20, ColumnType.getDecimalType(2, 1)); // 2.0
    }

    @Test
    public void testRemDecimal8WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 2))); // 0.50
        args.add(new Decimal8Constant((byte) 3, ColumnType.getDecimalType(2, 1))); // 0.3
        createFunctionAndAssert(args, 0, 0, 0, 20, ColumnType.getDecimalType(2, 2)); // 0.20
    }

    @Test
    public void testRemDecimal8WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)));
        args.add(new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(2, 1));
    }

    @Test
    public void testRemMixedDecimalTypes() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(1, 1, 1, 5, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal128Constant(0, 1, ColumnType.getDecimalType(20, 2)));
        createFunctionAndAssert(args, 0, 0, 0, 0, ColumnType.getDecimalType(40, 2));

        args.clear();
        args.add(new Decimal256Constant(1, 1, 1, 11, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal64Constant(2, ColumnType.getDecimalType(10, 2)));
        createFunctionAndAssert(args, 0, 0, 0, 1, ColumnType.getDecimalType(40, 2));
    }

    @Test
    public void testRemRandomValues() {
        for (int i = 2; i < 50; i++) {
            long dividend = i * 13;
            long divisor = i % 7 + 1; // ensure divisor is not zero
            long remainder = dividend % divisor;
            int scale = i % 3;

            ObjList<Function> args = new ObjList<>();
            args.add(new Decimal128Constant(0, dividend, ColumnType.getDecimalType(21, scale)));
            args.add(new Decimal128Constant(0, divisor, ColumnType.getDecimalType(21, scale)));
            createFunctionAndAssert(args, 0, 0, 0, remainder, ColumnType.getDecimalType(21, scale));
        }
    }

    @Test
    public void testRemWithPrecisionScaling() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 1234, ColumnType.getDecimalType(22, 2))); // 12.34
        args.add(new Decimal128Constant(0, 567, ColumnType.getDecimalType(22, 1))); // 56.7
        // 12.34 % 56.7 = 12.34
        createFunctionAndAssert(args, 0, 0, 0, 1234, ColumnType.getDecimalType(22, 2));
    }

    private void createFunctionAndAssert(ObjList<Function> args, long hh, long hl, long lh, long ll, int expectedType) {
        try (Function func = factory.newInstance(-1, args, null, null, null)) {
            Decimal256 value = new Decimal256();
            DecimalUtil.load(value, func, null);
            Assert.assertEquals(hh, value.getHh());
            Assert.assertEquals(hl, value.getHl());
            Assert.assertEquals(lh, value.getLh());
            Assert.assertEquals(ll, value.getLl());
            Assert.assertEquals(expectedType, func.getType());
        }
    }

    private void createFunctionAndAssertNull(ObjList<Function> args, int expectedType) {
        createFunctionAndAssert(
                args,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }
}