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
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.AbsDecimalFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AbsDecimalFunctionFactoryTest extends AbstractCairoTest {
    private final ObjList<Function> args = new ObjList<>();
    private final AbsDecimalFunctionFactory factory = new AbsDecimalFunctionFactory();

    @Test
    public void testAbsDecimal128MaxValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Decimal128.MAX_VALUE.getHigh(), Decimal128.MAX_VALUE.getLow(), ColumnType.getDecimalType(37, 0)),
                0, 0, Decimal128.MAX_VALUE.getHigh(), Decimal128.MAX_VALUE.getLow(),
                ColumnType.getDecimalType(37, 0)
        );
    }

    @Test
    public void testAbsDecimal128MinValue() {
        // Test close to MIN_VALUE to avoid overflow issues
        createFunctionAndAssert(
                new Decimal128Constant(Decimal128.MIN_VALUE.getHigh(), Decimal128.MIN_VALUE.getLow() + 1, ColumnType.getDecimalType(37, 0)),
                0, 0, Decimal128.MAX_VALUE.getHigh(), Decimal128.MAX_VALUE.getLow() - 1,
                ColumnType.getDecimalType(37, 0)
        );
    }

    @Test
    public void testAbsDecimal128Negative() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                100,
                ColumnType.getDecimalType(20, 2)
        );
    }

    @Test
    public void testAbsDecimal128Positive() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                100,
                ColumnType.getDecimalType(20, 2)
        );
    }

    @Test
    public void testAbsDecimal128WithNull() {
        createFunctionAndAssertNull(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                ColumnType.getDecimalType(20, 0)
        );
    }

    @Test
    public void testAbsDecimal128Zero() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(20, 2)),
                0,
                ColumnType.getDecimalType(20, 2)
        );
    }

    @Test
    public void testAbsDecimal16MaxValue() {
        createFunctionAndAssert(
                new Decimal16Constant(Short.MAX_VALUE, ColumnType.getDecimalType(4, 0)),
                Short.MAX_VALUE,
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testAbsDecimal16MinValue() {
        createFunctionAndAssert(
                new Decimal16Constant((short) (Short.MIN_VALUE + 1), ColumnType.getDecimalType(4, 0)),
                -(Short.MIN_VALUE + 1),
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testAbsDecimal16Negative() {
        createFunctionAndAssert(
                new Decimal16Constant((short) -100, ColumnType.getDecimalType(4, 2)),
                100,
                ColumnType.getDecimalType(4, 2)
        );
    }

    @Test
    public void testAbsDecimal16Positive() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                100,
                ColumnType.getDecimalType(4, 2)
        );
    }

    @Test
    public void testAbsDecimal16WithNull() {
        createFunctionAndAssertNull(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testAbsDecimal16Zero() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                0,
                ColumnType.getDecimalType(4, 2)
        );
    }

    @Test
    public void testAbsDecimal256LargeValues() {
        // Test large positive value
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(76, 0)),
                0, 0, 1, 0,
                ColumnType.getDecimalType(76, 0)
        );
        // Test large negative value
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -2, 0, ColumnType.getDecimalType(76, 0)),
                0, 0, 2, 0,
                ColumnType.getDecimalType(76, 0)
        );
    }

    @Test
    public void testAbsDecimal256Negative() {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                0, 0, 0, 100,
                ColumnType.getDecimalType(40, 2)
        );
    }

    @Test
    public void testAbsDecimal256Positive() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                0, 0, 0, 100,
                ColumnType.getDecimalType(40, 2)
        );
    }

    @Test
    public void testAbsDecimal256WithCarry() {
        // Test a value that requires proper carry handling when negated
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -1, ColumnType.getDecimalType(40, 0)),
                0, 0, 0, 1,
                ColumnType.getDecimalType(40, 0)
        );
    }

    @Test
    public void testAbsDecimal256WithNull() {
        createFunctionAndAssertNull(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                ColumnType.getDecimalType(40, 0)
        );
    }

    @Test
    public void testAbsDecimal256Zero() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                0, 0, 0, 0,
                ColumnType.getDecimalType(40, 2)
        );
    }

    @Test
    public void testAbsDecimal32MaxValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MAX_VALUE, ColumnType.getDecimalType(9, 0)),
                Integer.MAX_VALUE,
                ColumnType.getDecimalType(9, 0)
        );
    }

    @Test
    public void testAbsDecimal32MinValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MIN_VALUE + 1, ColumnType.getDecimalType(9, 0)),
                -(Integer.MIN_VALUE + 1),
                ColumnType.getDecimalType(9, 0)
        );
    }

    @Test
    public void testAbsDecimal32Negative() {
        createFunctionAndAssert(
                new Decimal32Constant(-100, ColumnType.getDecimalType(8, 2)),
                100,
                ColumnType.getDecimalType(8, 2)
        );
    }

    @Test
    public void testAbsDecimal32Positive() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                100,
                ColumnType.getDecimalType(8, 2)
        );
    }

    @Test
    public void testAbsDecimal32WithNull() {
        createFunctionAndAssertNull(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                ColumnType.getDecimalType(8, 0)
        );
    }

    @Test
    public void testAbsDecimal32Zero() {
        createFunctionAndAssert(
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                0,
                ColumnType.getDecimalType(8, 2)
        );
    }

    @Test
    public void testAbsDecimal64LargeValues() {
        createFunctionAndAssert(
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                1000000,
                ColumnType.getDecimalType(15, 2)
        );
        createFunctionAndAssert(
                new Decimal64Constant(-1000000, ColumnType.getDecimalType(15, 2)),
                1000000,
                ColumnType.getDecimalType(15, 2)
        );
    }

    @Test
    public void testAbsDecimal64MaxValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MAX_VALUE, ColumnType.getDecimalType(18, 0)),
                Long.MAX_VALUE,
                ColumnType.getDecimalType(18, 0)
        );
    }

    @Test
    public void testAbsDecimal64MinValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MIN_VALUE + 1, ColumnType.getDecimalType(18, 0)),
                -(Long.MIN_VALUE + 1),
                ColumnType.getDecimalType(18, 0)
        );
    }

    @Test
    public void testAbsDecimal64Negative() {
        createFunctionAndAssert(
                new Decimal64Constant(-100, ColumnType.getDecimalType(10, 2)),
                100,
                ColumnType.getDecimalType(10, 2)
        );
    }

    @Test
    public void testAbsDecimal64Positive() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                100,
                ColumnType.getDecimalType(10, 2)
        );
    }

    @Test
    public void testAbsDecimal64WithNull() {
        createFunctionAndAssertNull(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                ColumnType.getDecimalType(10, 0)
        );
    }

    @Test
    public void testAbsDecimal64Zero() {
        createFunctionAndAssert(
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                0,
                ColumnType.getDecimalType(10, 2)
        );
    }

    @Test
    public void testAbsDecimal8MaxValue() {
        createFunctionAndAssert(
                new Decimal8Constant(Byte.MAX_VALUE, ColumnType.getDecimalType(2, 0)),
                Byte.MAX_VALUE,
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testAbsDecimal8MinValue() {
        // abs(MIN_VALUE) should work correctly, but note potential overflow
        createFunctionAndAssert(
                new Decimal8Constant((byte) (Byte.MIN_VALUE + 1), ColumnType.getDecimalType(2, 0)),
                -(Byte.MIN_VALUE + 1),
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testAbsDecimal8Negative() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) -10, ColumnType.getDecimalType(2, 1)),
                10,
                ColumnType.getDecimalType(2, 1)
        );
    }

    @Test
    public void testAbsDecimal8Positive() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                10,
                ColumnType.getDecimalType(2, 1)
        );
    }

    @Test
    public void testAbsDecimal8WithNull() {
        createFunctionAndAssertNull(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testAbsDecimal8Zero() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                0,
                ColumnType.getDecimalType(2, 1)
        );
    }

    // Precision boundary tests
    @Test
    public void testAbsPrecisionBoundaries() {
        // Test values at different precision boundaries

        // 2 digits (max for DECIMAL8)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 99, ColumnType.getDecimalType(2, 0)),
                99,
                ColumnType.getDecimalType(2, 0)
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) -99, ColumnType.getDecimalType(2, 0)),
                99,
                ColumnType.getDecimalType(2, 0)
        );

        // 4 digits (max for DECIMAL16)
        createFunctionAndAssert(
                new Decimal16Constant((short) 9999, ColumnType.getDecimalType(4, 0)),
                9999,
                ColumnType.getDecimalType(4, 0)
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) -9999, ColumnType.getDecimalType(4, 0)),
                9999,
                ColumnType.getDecimalType(4, 0)
        );

        // 9 digits (max for DECIMAL32)
        createFunctionAndAssert(
                new Decimal32Constant(999999999, ColumnType.getDecimalType(9, 0)),
                999999999,
                ColumnType.getDecimalType(9, 0)
        );
        createFunctionAndAssert(
                new Decimal32Constant(-999999999, ColumnType.getDecimalType(9, 0)),
                999999999,
                ColumnType.getDecimalType(9, 0)
        );
    }

    // Random values test across different scales
    @Test
    public void testAbsRandomValues() {
        for (int i = 1; i < 20; i++) {
            long val = i * 17;
            int scale = i % 4;

            // Test positive values
            createFunctionAndAssert(
                    new Decimal64Constant(val, ColumnType.getDecimalType(15, scale)),
                    val,
                    ColumnType.getDecimalType(15, scale)
            );

            // Test negative values
            createFunctionAndAssert(
                    new Decimal64Constant(-val, ColumnType.getDecimalType(15, scale)),
                    val,
                    ColumnType.getDecimalType(15, scale)
            );

            // Test with Decimal128
            createFunctionAndAssert(
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(25, scale)),
                    val,
                    ColumnType.getDecimalType(25, scale)
            );

            createFunctionAndAssert(
                    new Decimal128Constant(-1, -val, ColumnType.getDecimalType(25, scale)),
                    val,
                    ColumnType.getDecimalType(25, scale)
            );
        }
    }

    // Helper methods
    private void createFunctionAndAssert(Function arg, long expectedValue, int expectedType) {
        args.clear();
        args.add(arg);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            Decimal256 value = new Decimal256();
            DecimalUtil.load(value, Misc.getThreadLocalDecimal128(), func, null);
            if (expectedValue >= 0) {
                Assert.assertEquals(0, value.getHh());
                Assert.assertEquals(0, value.getHl());
                Assert.assertEquals(0, value.getLh());
                Assert.assertEquals(expectedValue, value.getLl());
            } else {
                Assert.assertEquals(-1, value.getHh());
                Assert.assertEquals(-1, value.getHl());
                Assert.assertEquals(-1, value.getLh());
                Assert.assertEquals(expectedValue, value.getLl());
            }
            Assert.assertEquals(expectedType, func.getType());
            Assert.assertEquals("abs", func.getName());
        }
    }

    private void createFunctionAndAssert(Function arg, long hh, long hl, long lh, long ll, int expectedType) {
        args.clear();
        args.add(arg);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            Decimal256 value = new Decimal256();
            DecimalUtil.load(value, Misc.getThreadLocalDecimal128(), func, null);
            Assert.assertEquals(hh, value.getHh());
            Assert.assertEquals(hl, value.getHl());
            Assert.assertEquals(lh, value.getLh());
            Assert.assertEquals(ll, value.getLl());
            Assert.assertEquals(expectedType, func.getType());
            Assert.assertEquals("abs", func.getName());
        }
    }

    private void createFunctionAndAssertNull(Function arg, int expectedType) {
        createFunctionAndAssert(
                arg,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }
}