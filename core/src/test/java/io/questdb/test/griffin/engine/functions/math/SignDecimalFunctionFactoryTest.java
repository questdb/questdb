/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.SignDecimalFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SignDecimalFunctionFactoryTest extends AbstractCairoTest {
    private final ObjList<Function> args = new ObjList<>();
    private final SignDecimalFunctionFactory factory = new SignDecimalFunctionFactory();

    @Test
    public void testSignDecimal128LargeNegative() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, 0, ColumnType.getDecimalType(25, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128LargePositive() {
        createFunctionAndAssert(
                new Decimal128Constant(1, 0, ColumnType.getDecimalType(25, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128MaxValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Decimal128.MAX_VALUE.getHigh(), Decimal128.MAX_VALUE.getLow(), ColumnType.getDecimalType(37, 0)),
                0, 0, 0, 1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128MinValue() {
        // Test close to MIN_VALUE
        createFunctionAndAssert(
                new Decimal128Constant(Decimal128.MIN_VALUE.getHigh(), Decimal128.MIN_VALUE.getLow() + 1, ColumnType.getDecimalType(37, 0)),
                -1, -1, -1, -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128Negative() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128Positive() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128WithNull() {
        createFunctionAndAssertNull(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal128Zero() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(20, 2)),
                0,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal16MaxValue() {
        createFunctionAndAssert(
                new Decimal16Constant(Short.MAX_VALUE, ColumnType.getDecimalType(4, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal16MinValue() {
        createFunctionAndAssert(
                new Decimal16Constant((short) (Short.MIN_VALUE + 1), ColumnType.getDecimalType(4, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal16Negative() {
        createFunctionAndAssert(
                new Decimal16Constant((short) -100, ColumnType.getDecimalType(4, 2)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal16Positive() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal16WithNull() {
        createFunctionAndAssertNull(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal16Zero() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                0,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256LargeNegative() {
        // Test large negative value
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -2, 0, ColumnType.getDecimalType(76, 0)),
                -1, -1, -1, -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256LargePositive() {
        // Test large positive value
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(76, 0)),
                0, 0, 0, 1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256Negative() {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                -1, -1, -1, -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256Positive() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                0, 0, 0, 1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256VeryLargeNegative() {
        // Test very large negative value
        createFunctionAndAssert(
                new Decimal256Constant(-2, 0, 0, 0, ColumnType.getDecimalType(76, 0)),
                -1, -1, -1, -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256VeryLargePositive() {
        // Test very large positive value
        createFunctionAndAssert(
                new Decimal256Constant(1, 0, 0, 0, ColumnType.getDecimalType(76, 0)),
                0, 0, 0, 1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256WithNull() {
        createFunctionAndAssertNull(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal256Zero() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                0, 0, 0, 0,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal32MaxValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MAX_VALUE, ColumnType.getDecimalType(9, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal32MinValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MIN_VALUE + 1, ColumnType.getDecimalType(9, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal32Negative() {
        createFunctionAndAssert(
                new Decimal32Constant(-100, ColumnType.getDecimalType(8, 2)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal32Positive() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal32WithNull() {
        createFunctionAndAssertNull(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal32Zero() {
        createFunctionAndAssert(
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                0,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64LargeValues() {
        createFunctionAndAssert(
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal64Constant(-1000000, ColumnType.getDecimalType(15, 2)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64MaxValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MAX_VALUE, ColumnType.getDecimalType(18, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64MinValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MIN_VALUE + 1, ColumnType.getDecimalType(18, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64Negative() {
        createFunctionAndAssert(
                new Decimal64Constant(-100, ColumnType.getDecimalType(10, 2)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64Positive() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64WithNull() {
        createFunctionAndAssertNull(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal64Zero() {
        createFunctionAndAssert(
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                0,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal8MaxValue() {
        createFunctionAndAssert(
                new Decimal8Constant(Byte.MAX_VALUE, ColumnType.getDecimalType(2, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal8MinValue() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) (Byte.MIN_VALUE + 1), ColumnType.getDecimalType(2, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal8Negative() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) -10, ColumnType.getDecimalType(2, 1)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal8Positive() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal8WithNull() {
        createFunctionAndAssertNull(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testSignDecimal8Zero() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                0,
                ColumnType.getDecimalType(1, 0)
        );
    }

    // Edge case: One values
    @Test
    public void testSignOneValues() {
        // Test that 1 returns 1, -1 returns -1 for all types
        createFunctionAndAssert(
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(1, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) -1, ColumnType.getDecimalType(1, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );

        createFunctionAndAssert(
                new Decimal16Constant((short) 1, ColumnType.getDecimalType(3, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) -1, ColumnType.getDecimalType(3, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );

        createFunctionAndAssert(
                new Decimal32Constant(1, ColumnType.getDecimalType(5, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal32Constant(-1, ColumnType.getDecimalType(5, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );

        createFunctionAndAssert(
                new Decimal64Constant(1, ColumnType.getDecimalType(10, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal64Constant(-1, ColumnType.getDecimalType(10, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    // Precision boundary tests
    @Test
    public void testSignPrecisionBoundaries() {
        // Test values at different precision boundaries

        // 2 digits (max for DECIMAL8)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 99, ColumnType.getDecimalType(2, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) -99, ColumnType.getDecimalType(2, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );

        // 4 digits (max for DECIMAL16)
        createFunctionAndAssert(
                new Decimal16Constant((short) 9999, ColumnType.getDecimalType(4, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) -9999, ColumnType.getDecimalType(4, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );

        // 9 digits (max for DECIMAL32)
        createFunctionAndAssert(
                new Decimal32Constant(999999999, ColumnType.getDecimalType(9, 0)),
                1,
                ColumnType.getDecimalType(1, 0)
        );
        createFunctionAndAssert(
                new Decimal32Constant(-999999999, ColumnType.getDecimalType(9, 0)),
                -1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    // Random values test across different scales
    @Test
    public void testSignRandomValues() {
        for (int i = 1; i < 20; i++) {
            long val = i * 17;
            int scale = i % 4;

            // Test positive values -> expect 1
            createFunctionAndAssert(
                    new Decimal64Constant(val, ColumnType.getDecimalType(15, scale)),
                    1,
                    ColumnType.getDecimalType(1, 0)
            );

            // Test negative values -> expect -1
            createFunctionAndAssert(
                    new Decimal64Constant(-val, ColumnType.getDecimalType(15, scale)),
                    -1,
                    ColumnType.getDecimalType(1, 0)
            );

            // Test with Decimal128 positive -> expect 1
            createFunctionAndAssert(
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(25, scale)),
                    1,
                    ColumnType.getDecimalType(1, 0)
            );

            // Test with Decimal128 negative -> expect -1
            createFunctionAndAssert(
                    new Decimal128Constant(-1, -val, ColumnType.getDecimalType(25, scale)),
                    -1,
                    ColumnType.getDecimalType(1, 0)
            );
        }
    }

    // Helper methods
    private void createFunctionAndAssert(Function arg, long expectedValue, int expectedType) {
        args.clear();
        args.add(arg);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            // Since SignDecimalFunctionFactory always returns DECIMAL8, directly get the byte value
            byte result = func.getDecimal8(null);
            Assert.assertEquals((byte) expectedValue, result);
            Assert.assertEquals(expectedType, func.getType());
            Assert.assertEquals("sign", func.getName());
        }
    }

    private void createFunctionAndAssert(Function arg, long hh, long hl, long lh, long ll, int expectedType) {
        args.clear();
        args.add(arg);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            // Since SignDecimalFunctionFactory always returns DECIMAL8, directly get the byte value
            byte result = func.getDecimal8(null);
            // For DECIMAL8, only check the lowest byte (ll) since it's the only significant part
            Assert.assertEquals((byte) ll, result);
            Assert.assertEquals(expectedType, func.getType());
            Assert.assertEquals("sign", func.getName());
        }
    }

    private void createFunctionAndAssertNull(Function arg, int expectedType) {
        args.clear();
        args.add(arg);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            // Since SignDecimalFunctionFactory always returns DECIMAL8, directly get the byte value
            byte result = func.getDecimal8(null);
            Assert.assertEquals(Decimals.DECIMAL8_NULL, result);
            Assert.assertEquals(expectedType, func.getType());
            Assert.assertEquals("sign", func.getName());
        }
    }
}