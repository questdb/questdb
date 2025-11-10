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
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.math.RoundDecimalFunctionFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class RoundDecimalFunctionFactoryTest extends AbstractCairoTest {
    private final ObjList<Function> args = new ObjList<>();
    private final RoundDecimalFunctionFactory factory = new RoundDecimalFunctionFactory();

    @Test
    public void testRoundAllTypesDynamicNegativeScale() {
        // Test all decimal types with dynamic negative scales

        // DECIMAL8 with dynamic negative scale
        createDynamicFunctionAndAssert(
                new Decimal8Constant((byte) 67, ColumnType.getDecimalType(2, 0)),
                -1,
                70,
                ColumnType.getDecimalType(3, 0)
        );

        // DECIMAL16 with dynamic negative scale
        createDynamicFunctionAndAssert(
                new Decimal16Constant((short) 5678, ColumnType.getDecimalType(4, 0)),
                -2,
                5700,
                ColumnType.getDecimalType(5, 0)
        );

        // DECIMAL32 with dynamic negative scale
        createDynamicFunctionAndAssert(
                new Decimal32Constant(9876543, ColumnType.getDecimalType(7, 1)),
                -3,
                9880000,
                ColumnType.getDecimalType(8, 1)
        );
    }

    @Test
    public void testRoundAtPrecisionBoundaries() {
        // Test rounding at the exact precision boundaries of each type

        // DECIMAL8 maximum precision (2 digits)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 99, ColumnType.getDecimalType(2, 1)),
                0,
                10,
                ColumnType.getDecimalType(2, 0)
        );

        // DECIMAL16 maximum precision (4 digits)
        createFunctionAndAssert(
                new Decimal16Constant((short) 9999, ColumnType.getDecimalType(4, 2)),
                1,
                1000,
                ColumnType.getDecimalType(4, 1)
        );

        // DECIMAL32 maximum precision (9 digits)
        createFunctionAndAssert(
                new Decimal32Constant(999999999, ColumnType.getDecimalType(9, 3)),
                1,
                10000000,
                ColumnType.getDecimalType(8, 1)
        );
    }

    @Test
    public void testRoundComplexDecimalValues() {
        // Test complex decimal values with various scale combinations

        // High precision DECIMAL128 with complex rounding
        createFunctionAndAssert(
                new Decimal128Constant(0, 123456789123456789L, ColumnType.getDecimalType(30, 15)),
                10,
                0, 0, 0, 1234567891235L,
                ColumnType.getDecimalType(26, 10)
        );

        // DECIMAL256 with very high precision (2268949977855287194446)
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 123, 456789012345678L, ColumnType.getDecimalType(45, 20)),
                15,
                0, 0, 0, 22689499778552872L,
                ColumnType.getDecimalType(41, 15)
        );

        // Mixed precision scenarios
        createFunctionAndAssert(
                new Decimal64Constant(987654321098765L, ColumnType.getDecimalType(15, 8)),
                3,
                9876543211L,
                ColumnType.getDecimalType(11, 3)
        );
    }

    @Test
    public void testRoundCrossTypeBoundaries() {
        // Test rounding that might cross type boundaries due to precision changes

        // DECIMAL16 precision 4 -> reduced precision might stay DECIMAL16
        createFunctionAndAssert(
                new Decimal16Constant((short) 9876, ColumnType.getDecimalType(4, 2)),
                0,
                99,
                ColumnType.getDecimalType(3, 0)
        );

        // DECIMAL32 precision 9 -> reduced precision might become DECIMAL16
        createFunctionAndAssert(
                new Decimal32Constant(123456789, ColumnType.getDecimalType(9, 5)),
                1,
                12346,
                ColumnType.getDecimalType(6, 1)
        );

        // DECIMAL64 precision 18 -> reduced precision might become DECIMAL32
        createFunctionAndAssert(
                new Decimal64Constant(123456789012345678L, ColumnType.getDecimalType(18, 12)),
                2,
                12345679L,
                ColumnType.getDecimalType(9, 2)
        );
    }

    @Test
    public void testRoundDecimal128ConstantScale() {
        // Test rounding 123.456789 (scale=6) to scale 3 -> 123.457 (HALF_UP)
        createFunctionAndAssert(
                new Decimal128Constant(0, 123456789L, ColumnType.getDecimalType(19, 6)),
                3,
                0, 0, 0, 123457L,
                ColumnType.getDecimalType(17, 3)
        );
    }

    @Test
    public void testRoundDecimal128DynamicGreaterScale() {
        createDynamicFunctionAndAssert(
                new Decimal128Constant(0, 123456L, ColumnType.getDecimalType(20, 3)),
                5,
                0, 0, 0, 123456L,
                ColumnType.getDecimalType(21, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal128DynamicNegativeScale() {
        // Test 128-bit with dynamic scale
        createDynamicFunctionAndAssert(
                new Decimal128Constant(0, 123456L, ColumnType.getDecimalType(20, 3)),
                -2,
                0, 0, 0, 100000L,
                ColumnType.getDecimalType(21, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal128DynamicScale() {
        // Test 128-bit with dynamic scale
        createDynamicFunctionAndAssert(
                new Decimal128Constant(0, 123456L, ColumnType.getDecimalType(20, 3)),
                2,
                0, 0, 0, 123460L,
                ColumnType.getDecimalType(21, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal128HigherPrecision() {
        // Test rounding a high precision decimal (19-38 digits for DECIMAL128)
        // 1234567890123456789 with scale=10 to scale 5
        createFunctionAndAssert(
                new Decimal128Constant(0, 1234567890123456789L, ColumnType.getDecimalType(25, 10)),
                5,
                0, 0, 0, 12345678901235L,
                ColumnType.getDecimalType(21, 5)
        );
    }

    @Test
    public void testRoundDecimal128NegativeValue() {
        // Test rounding negative value -123.456 (scale=3) to scale 2
        createFunctionAndAssert(
                new Decimal128Constant(-1, -123456L, ColumnType.getDecimalType(26, 3)),
                2,
                -1, -1, -1, -12346L,
                ColumnType.getDecimalType(26, 2)
        );
    }

    @Test
    public void testRoundDecimal128WithNull() {
        createFunctionAndAssertNull(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                0,
                ColumnType.getDecimalType(20, 0)
        );
    }

    @Test
    public void testRoundDecimal16ConstantScale() {
        // Test rounding 12.45 (scale=2) to scale 1 -> 12.5 (HALF_UP)
        // targetPrecision = precision + roundingScale - scale + getRoundingModePrecisionImpact(HALF_UP)
        // = 4 + 1 - 2 + 1 = 4
        createFunctionAndAssert(
                new Decimal16Constant((short) 1245, ColumnType.getDecimalType(4, 2)),
                1,
                125,
                ColumnType.getDecimalType(4, 1)
        );
    }

    @Test
    public void testRoundDecimal16DynamicScale() {
        // Test DECIMAL16 with dynamic scale
        createDynamicFunctionAndAssert(
                new Decimal16Constant((short) 123, ColumnType.getDecimalType(3, 2)),
                1,
                120,
                ColumnType.getDecimalType(4, 2) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal16NegativeScale() {
        // Test rounding 1234 (scale=0) with rounding scale -1 -> round to tens
        // 1234 rounds to 1230, stored as 123 (since we divide by 10^1=10)
        createFunctionAndAssert(
                new Decimal16Constant((short) 1234, ColumnType.getDecimalType(4, 0)),
                -1,
                1230,
                ColumnType.getDecimalType(5, 0)
        );
    }

    @Test
    public void testRoundDecimal16NegativeValue() {
        // Test rounding -12.45 (scale=2) to scale 1 -> -12.5 (HALF_UP)
        // targetPrecision = precision + roundingScale - scale + 1 = 4 + 1 - 2 + 1 = 4
        createFunctionAndAssert(
                new Decimal16Constant((short) -1245, ColumnType.getDecimalType(4, 2)),
                1,
                -125,
                ColumnType.getDecimalType(4, 1)
        );
    }

    @Test
    public void testRoundDecimal16ThreeDigitWithScale() {
        // Test 3-digit precision with scale: 12.3 (scale=1) to scale 0 -> 12 (HALF_UP)
        createFunctionAndAssert(
                new Decimal16Constant((short) 123, ColumnType.getDecimalType(3, 1)),
                0,
                12,
                ColumnType.getDecimalType(3, 0)
        );
    }

    @Test
    public void testRoundDecimal16ThreePrecision() {
        // Test 3-digit precision: 123 (scale=0) with rounding scale -1 -> 120
        createFunctionAndAssert(
                new Decimal16Constant((short) 123, ColumnType.getDecimalType(3, 0)),
                -1,
                120,
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testRoundDecimal16WithNull() {
        createFunctionAndAssertNull(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                0,
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testRoundDecimal256ConstantScale() {
        // Test rounding large decimal value (39+ digits for DECIMAL256)
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 123456789L, ColumnType.getDecimalType(45, 6)),
                3,
                0, 0, 0, 123457L,
                ColumnType.getDecimalType(43, 3)
        );
    }

    @Test
    public void testRoundDecimal256DynamicGreaterScale() {
        createDynamicFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 123456L, ColumnType.getDecimalType(40, 3)),
                5,
                0, 0, 0, 123456L,
                ColumnType.getDecimalType(41, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal256DynamicNegativeScale() {
        // Test 256-bit with dynamic scale
        createDynamicFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 123456L, ColumnType.getDecimalType(40, 3)),
                -2,
                0, 0, 0, 100000L,
                ColumnType.getDecimalType(41, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal256DynamicScale() {
        createDynamicFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 123456L, ColumnType.getDecimalType(42, 3)),
                1,
                0, 0, 0, 123500L,
                ColumnType.getDecimalType(43, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal256HigherPrecision() {
        // Test rounding very high precision decimal (39+ digits for DECIMAL256)
        // Scale from 15 to 8
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 123456789012345678L, ColumnType.getDecimalType(50, 15)),
                8,
                0, 0, 0, 12345678901L,
                ColumnType.getDecimalType(44, 8)
        );
    }

    @Test
    public void testRoundDecimal256LargeValue() {
        // Test rounding very large value with high precision (39+ digits for DECIMAL256)
        // 2⁶⁴+123456789 = 18446744073833008405
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 1, 123456789L, ColumnType.getDecimalType(45, 10)),
                5,
                0, 0, 0, 184467440738330L,
                ColumnType.getDecimalType(41, 5)
        );
    }

    @Test
    public void testRoundDecimal256WithNull() {
        createFunctionAndAssertNull(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                0,
                ColumnType.getDecimalType(40, 0)
        );
    }

    @Test
    public void testRoundDecimal32ConstantScale() {
        // Test rounding 123.456 (scale=3) to scale 2 -> 123.46 (HALF_UP)
        // targetPrecision = precision + roundingScale - scale + getRoundingModePrecisionImpact(HALF_UP)
        // = 6 + 2 - 3 + 1 = 6 (removes 1 decimal place, adds 1 for potential rounding)
        createFunctionAndAssert(
                new Decimal32Constant(123456, ColumnType.getDecimalType(6, 3)),
                2,
                12346,
                ColumnType.getDecimalType(6, 2)
        );
    }

    @Test
    public void testRoundDecimal32DynamicScale() {
        // Test DECIMAL32 with dynamic scale
        createDynamicFunctionAndAssert(
                new Decimal32Constant(123456, ColumnType.getDecimalType(6, 3)),
                1,
                123500,
                ColumnType.getDecimalType(7, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal32EightPrecision() {
        // Test 8-digit precision: 12345.678 (scale=3) to scale 1 -> 12345.7
        createFunctionAndAssert(
                new Decimal32Constant(12345678, ColumnType.getDecimalType(8, 3)),
                1,
                123457,
                ColumnType.getDecimalType(7, 1)
        );
    }

    @Test
    public void testRoundDecimal32FivePrecision() {
        // Test 5-digit precision: 12345 (scale=0) with rounding scale -2 -> 12300
        createFunctionAndAssert(
                new Decimal32Constant(12345, ColumnType.getDecimalType(5, 0)),
                -2,
                12300,
                ColumnType.getDecimalType(6, 0)
        );
    }

    @Test
    public void testRoundDecimal32NegativeScale() {
        // Test rounding 12345 (scale=0) with rounding scale -2 -> round to hundreds
        // 12345 rounds to 12300
        createFunctionAndAssert(
                new Decimal32Constant(12345, ColumnType.getDecimalType(5, 0)),
                -2,
                12300,
                ColumnType.getDecimalType(6, 0)
        );
    }

    @Test
    public void testRoundDecimal32SixPrecision() {
        // Test 6-digit precision: 123.456 (scale=3) to scale 1 -> 123.5
        createFunctionAndAssert(
                new Decimal32Constant(123456, ColumnType.getDecimalType(6, 3)),
                1,
                1235,
                ColumnType.getDecimalType(5, 1)
        );
    }

    @Test
    public void testRoundDecimal32ToInteger() {
        // Test rounding 999.999 (scale=3) to scale 0 -> 1000 (HALF_UP)
        createFunctionAndAssert(
                new Decimal32Constant(999999, ColumnType.getDecimalType(6, 3)),
                0,
                1000,
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testRoundDecimal32WithNull() {
        createFunctionAndAssertNull(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(6, 0)),
                0,
                ColumnType.getDecimalType(6, 0)
        );
    }

    @Test
    public void testRoundDecimal64ConstantScale() {
        // Test rounding 123456.789 (scale=3) to scale 1 -> 123456.8 (HALF_UP)
        createFunctionAndAssert(
                new Decimal64Constant(123456789L, ColumnType.getDecimalType(10, 3)),
                1,
                1234568L,
                ColumnType.getDecimalType(9, 1)
        );
    }

    @Test
    public void testRoundDecimal64DynamicGreaterScale() {
        createDynamicFunctionAndAssert(
                new Decimal64Constant(123456L, ColumnType.getDecimalType(10, 3)),
                5,
                0, 0, 0, 123456L,
                ColumnType.getDecimalType(11, 3) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal64DynamicScale() {
        // Test with dynamic (non-constant) scale
        createDynamicFunctionAndAssert(
                new Decimal64Constant(12345L, ColumnType.getDecimalType(12, 2)),
                1,
                12350L,
                ColumnType.getDecimalType(13, 2) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal64FifteenPrecision() {
        // Test 15-digit precision: 123456789.012345 (scale=6) to scale 2 -> 123456789.01
        createFunctionAndAssert(
                new Decimal64Constant(123456789012345L, ColumnType.getDecimalType(15, 6)),
                2,
                12345678901L,
                ColumnType.getDecimalType(12, 2)
        );
    }

    @Test
    public void testRoundDecimal64HigherPrecision() {
        // Test rounding 1234567890 (123.4567890 with scale=7) to scale 3 -> 123.457
        // targetPrecision = precision + roundingScale - scale + 1 = 10 + 3 - 7 + 1 = 7
        createFunctionAndAssert(
                new Decimal64Constant(1234567890L, ColumnType.getDecimalType(10, 7)),
                3,
                123457L,
                ColumnType.getDecimalType(7, 3)
        );
    }

    @Test
    public void testRoundDecimal64LargeValue() {
        // Test rounding large value 999999999.5 (scale=1) to scale 0
        createFunctionAndAssert(
                new Decimal64Constant(9999999995L, ColumnType.getDecimalType(10, 1)),
                0,
                1000000000L,
                ColumnType.getDecimalType(10, 0)
        );
    }

    @Test
    public void testRoundDecimal64SeventeenPrecision() {
        // Test 17-digit precision: 12345678901234567 (scale=0) with rounding scale -4 -> 12345678901230000
        createFunctionAndAssert(
                new Decimal64Constant(12345678901234567L, ColumnType.getDecimalType(17, 0)),
                -4,
                12345678901230000L,
                ColumnType.getDecimalType(18, 0)
        );
    }

    @Test
    public void testRoundDecimal64TenPrecision() {
        // Test 10-digit precision: 1234567890 (scale=0) with rounding scale -3 -> 1234567000
        createFunctionAndAssert(
                new Decimal64Constant(1234567890L, ColumnType.getDecimalType(10, 0)),
                -3,
                1234568000L,
                ColumnType.getDecimalType(11, 0)
        );
    }

    @Test
    public void testRoundDecimal64TwelvePrecision() {
        // Test 12-digit precision: 123456.789012 (scale=6) to scale 3 -> 123456.789
        createFunctionAndAssert(
                new Decimal64Constant(123456789012L, ColumnType.getDecimalType(12, 6)),
                3,
                123456789L,
                ColumnType.getDecimalType(10, 3)
        );
    }

    @Test
    public void testRoundDecimal64WithNull() {
        createFunctionAndAssertNull(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                0,
                ColumnType.getDecimalType(10, 0)
        );
    }

    @Test
    public void testRoundDecimal8ConstantScale() {
        // Test rounding 1.5 (scale=1) to scale 0 -> 2 (HALF_UP)
        // targetPrecision = precision + roundingScale - scale + getRoundingModePrecisionImpact(HALF_UP)
        // = 2 + 0 - 1 + 1 = 2
        createFunctionAndAssert(
                new Decimal8Constant((byte) 15, ColumnType.getDecimalType(2, 1)),
                0,
                2,
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testRoundDecimal8DynamicScale() {
        // Test DECIMAL8 with dynamic scale
        createDynamicFunctionAndAssert(
                new Decimal8Constant((byte) 9, ColumnType.getDecimalType(1, 1)),
                0,
                10,
                ColumnType.getDecimalType(2, 1) // Dynamic keeps original scale
        );
    }

    @Test
    public void testRoundDecimal8NegativeScale() {
        // Test rounding 55 (scale=0) with rounding scale -1 -> round to tens
        // 55 rounds to 60, stored as 6 (since we divide by 10^1=10)
        // targetPrecision = precision - scale + getRoundingModePrecisionImpact(HALF_UP) = 2 - 0 + 1 = 3
        createFunctionAndAssert(
                new Decimal8Constant((byte) 55, ColumnType.getDecimalType(2, 0)),
                -1,
                60,
                ColumnType.getDecimalType(3, 0)
        );
    }

    @Test
    public void testRoundDecimal8NullScale() {
        createFunctionAndAssertNull(
                new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 0)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testRoundDecimal8OneDigitWithScale() {
        // Test 1-digit precision with scale: 0.9 (scale=1) to scale 0 -> 1 (HALF_UP)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 9, ColumnType.getDecimalType(1, 1)),
                0,
                1,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testRoundDecimal8OnePrecision() {
        // Test 1-digit precision: 9 (scale=0) to scale 0 -> 9 (no change)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 9, ColumnType.getDecimalType(1, 0)),
                0,
                9,
                ColumnType.getDecimalType(1, 0)
        );
    }

    @Test
    public void testRoundDecimal8ScaleGreaterThanOriginal() {
        // Test rounding 12 (scale=0) to scale 2 -> 12 (no change, scale >= original)
        Function value = new Decimal8Constant((byte) 12, ColumnType.getDecimalType(2, 0));
        Function scale = new IntConstant(2);
        args.clear();
        args.add(value);
        args.add(scale);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            Assert.assertEquals(value, func); // Should return original value
        }
    }

    @Test
    public void testRoundDecimal8ScaleZero() {
        // Test rounding 50 (scale=0) to scale 0 -> 50 (no change)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 0)),
                0,
                50,
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testRoundDecimal8WithNull() {
        createFunctionAndAssertNull(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                0,
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testRoundDynamicNullScale() {
        createDynamicFunctionAndAssertNull(
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(1, 1)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(2, 1)
        );
        createDynamicFunctionAndAssertNull(
                new Decimal16Constant((short) 12345, ColumnType.getDecimalType(3, 2)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(4, 2)
        );
        createDynamicFunctionAndAssertNull(
                new Decimal32Constant(12345, ColumnType.getDecimalType(5, 2)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(6, 2)
        );
        createDynamicFunctionAndAssertNull(
                new Decimal64Constant(12345, ColumnType.getDecimalType(15, 2)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(16, 2)
        );
        createDynamicFunctionAndAssertNull(
                new Decimal128Constant(0, 12345, ColumnType.getDecimalType(25, 2)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(26, 2)
        );
        createDynamicFunctionAndAssertNull(
                new Decimal256Constant(0, 0, 0, 12345, ColumnType.getDecimalType(45, 2)),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(46, 2)
        );
    }

    @Test
    public void testRoundDynamicNullValue() {
        createDynamicFunctionAndAssertNull(
                DecimalUtil.createNullDecimalConstant(1, 1),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(2, 1)
        );
        createDynamicFunctionAndAssertNull(
                DecimalUtil.createNullDecimalConstant(3, 2),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(4, 2)
        );
        createDynamicFunctionAndAssertNull(
                DecimalUtil.createNullDecimalConstant(5, 2),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(6, 2)
        );
        createDynamicFunctionAndAssertNull(
                DecimalUtil.createNullDecimalConstant(15, 2),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(16, 2)
        );
        createDynamicFunctionAndAssertNull(
                DecimalUtil.createNullDecimalConstant(25, 2),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(26, 2)
        );
        createDynamicFunctionAndAssertNull(
                DecimalUtil.createNullDecimalConstant(45, 2),
                Numbers.INT_NULL,
                ColumnType.getDecimalType(46, 2)
        );
    }

    @Test
    public void testRoundEdgeCases() {
        // Test rounding 0.5 (should round up to 1)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 5, ColumnType.getDecimalType(1, 1)),
                0,
                1,
                ColumnType.getDecimalType(1, 0)
        );

        // Test rounding -0.5 (should round to -1, HALF_UP rounds away from zero)
        createFunctionAndAssert(
                new Decimal8Constant((byte) -5, ColumnType.getDecimalType(1, 1)),
                0,
                -1,
                ColumnType.getDecimalType(1, 0)
        );

        // Test rounding 1.5 (should round up to 2)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 15, ColumnType.getDecimalType(2, 1)),
                0,
                2,
                ColumnType.getDecimalType(2, 0)
        );

        // Test rounding 2.5 (should round up to 3)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 25, ColumnType.getDecimalType(2, 1)),
                0,
                3,
                ColumnType.getDecimalType(2, 0)
        );
    }

    @Test
    public void testRoundExtremeNegativeScales() {
        // Test very large negative scales

        // Round to hundred millions place (scale -8)
        createFunctionAndAssert(
                new Decimal64Constant(123456789012L, ColumnType.getDecimalType(12, 0)),
                -8,
                123500000000L,
                ColumnType.getDecimalType(13, 0)
        );

        // Round to billions place (scale -9)
        createFunctionAndAssert(
                new Decimal64Constant(9876543210L, ColumnType.getDecimalType(10, 0)),
                -9,
                10000000000L,
                ColumnType.getDecimalType(11, 0)
        );

        // DECIMAL128 with scale -12 (round to trillions)
        createFunctionAndAssert(
                new Decimal128Constant(0, 123456789012345L, ColumnType.getDecimalType(25, 0)),
                -12,
                0, 0, 0, 123000000000000L,
                ColumnType.getDecimalType(26, 0)
        );
    }

    @Test
    public void testRoundHighPrecisionWithNegativeScales() {
        // Test high precision values with negative scales

        // DECIMAL256: Very large number with negative scale
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 123456789012345L, ColumnType.getDecimalType(39, 0)),
                -10,
                0, 0, 0, 123460000000000L,
                ColumnType.getDecimalType(40, 0)
        );

        // DECIMAL128: High precision with moderate negative scale
        createFunctionAndAssert(
                new Decimal128Constant(0, 987654321098765L, ColumnType.getDecimalType(25, 0)),
                -5,
                0, 0, 0, 987654321100000L,
                ColumnType.getDecimalType(26, 0)
        );
    }

    @Test
    public void testRoundLargeNegativeScales() {
        // Test rounding with large negative scales

        // DECIMAL32: 123456789 with scale -5 -> round to hundred thousands
        createFunctionAndAssert(
                new Decimal32Constant(123456789, ColumnType.getDecimalType(9, 0)),
                -5,
                123500000,
                ColumnType.getDecimalType(10, 0)
        );

        // DECIMAL64: 123456789012 with scale -6 -> round to millions
        createFunctionAndAssert(
                new Decimal64Constant(123456789012L, ColumnType.getDecimalType(12, 0)),
                -6,
                123457000000L,
                ColumnType.getDecimalType(13, 0)
        );

        // DECIMAL128: with scale -8 -> round to hundred millions
        createFunctionAndAssert(
                new Decimal128Constant(0, 12345678901234L, ColumnType.getDecimalType(25, 0)),
                -8,
                0, 0, 0, 12345700000000L,
                ColumnType.getDecimalType(26, 0)
        );
    }

    @Test
    public void testRoundLargeScaleReductions() {
        // Test rounding with large scale reductions (many decimal places removed)

        // From scale 10 to scale 2 (remove 8 decimal places)
        createFunctionAndAssert(
                new Decimal64Constant(12345678901234L, ColumnType.getDecimalType(14, 10)),
                2,
                123457L,
                ColumnType.getDecimalType(7, 2)
        );

        // From scale 15 to scale 3 (remove 12 decimal places)
        createFunctionAndAssert(
                new Decimal128Constant(0, 123456789012345678L, ColumnType.getDecimalType(30, 15)),
                3,
                0, 0, 0, 123457L,
                ColumnType.getDecimalType(19, 3)
        );

        // From scale 20 to scale 1 (remove 19 decimal places)
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 98765432109876543L, ColumnType.getDecimalType(55, 20)),
                1,
                0, 0, 0, 0,
                ColumnType.getDecimalType(37, 1)
        );
    }

    @Test
    public void testRoundMaximumPrecisionBoundaries() {
        // Test rounding at maximum precision boundaries for each decimal type

        // DECIMAL8 maximum precision (2 digits): 99
        createFunctionAndAssert(
                new Decimal8Constant((byte) 99, ColumnType.getDecimalType(2, 0)),
                -1,
                (byte) 100,
                ColumnType.getDecimalType(3, 0)
        );

        // DECIMAL16 maximum precision (4 digits): 9999
        createFunctionAndAssert(
                new Decimal16Constant((short) 9999, ColumnType.getDecimalType(4, 0)),
                -2,
                (short) 10000,
                ColumnType.getDecimalType(5, 0)
        );

        // DECIMAL32 maximum precision (9 digits): 999999999
        createFunctionAndAssert(
                new Decimal32Constant(999999999, ColumnType.getDecimalType(9, 0)),
                -3,
                1000000000,
                ColumnType.getDecimalType(10, 0)
        );
    }

    @Test
    public void testRoundMixedScaleScenarios() {
        // Test mixed scenarios: positive input scale, negative rounding scale

        // 123.456 (scale=3) rounded to tens place (scale=-1) -> 120.0 (scale=0)
        createFunctionAndAssert(
                new Decimal32Constant(123456, ColumnType.getDecimalType(6, 3)),
                -1,
                120,
                ColumnType.getDecimalType(4, 0)
        );

        // 12.3456789 (scale=7) rounded to units place (scale=0) -> 12 (scale=0)
        createFunctionAndAssert(
                new Decimal64Constant(123456789L, ColumnType.getDecimalType(10, 7)),
                0,
                12L,
                ColumnType.getDecimalType(4, 0)
        );

        // 999.999 (scale=3) rounded to tens place (scale=-1) -> 1000 (scale=0)
        createFunctionAndAssert(
                new Decimal32Constant(999999, ColumnType.getDecimalType(6, 3)),
                -1,
                1000,
                ColumnType.getDecimalType(4, 0)
        );
    }

    @Test
    public void testRoundNegativeScaleEdgeCases() {
        // Test edge cases with negative scales across all decimal types

        // DECIMAL8: Round 99 to tens place (scale -1)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 99, ColumnType.getDecimalType(2, 0)),
                -1,
                (byte) 100,
                ColumnType.getDecimalType(3, 0)
        );

        // DECIMAL16: Round 9999 to hundreds place (scale -2)
        createFunctionAndAssert(
                new Decimal16Constant((short) 9999, ColumnType.getDecimalType(4, 0)),
                -2,
                (short) 10000,
                ColumnType.getDecimalType(5, 0)
        );

        // DECIMAL32: Round 999999 to thousands place (scale -3)
        createFunctionAndAssert(
                new Decimal32Constant(999999, ColumnType.getDecimalType(6, 0)),
                -3,
                1000000,
                ColumnType.getDecimalType(7, 0)
        );

        // DECIMAL64: Round to ten millions place (scale -7)
        createFunctionAndAssert(
                new Decimal64Constant(999999999L, ColumnType.getDecimalType(10, 0)),
                -7,
                1000000000L,
                ColumnType.getDecimalType(11, 0)
        );
    }

    @Test
    public void testRoundPrecisionBoundaries() {
        // Test values at different precision boundaries

        // 2 digits (max for DECIMAL8)
        createFunctionAndAssert(
                new Decimal8Constant((byte) 95, ColumnType.getDecimalType(2, 1)),
                0,
                10,
                ColumnType.getDecimalType(2, 0)
        );

        // 4 digits (max for DECIMAL16)
        createFunctionAndAssert(
                new Decimal16Constant((short) 9995, ColumnType.getDecimalType(4, 1)),
                0,
                1000,
                ColumnType.getDecimalType(4, 0)
        );

        // 9 digits (max for DECIMAL32)
        createFunctionAndAssert(
                new Decimal32Constant(123456785, ColumnType.getDecimalType(9, 1)),
                0,
                12345679,
                ColumnType.getDecimalType(9, 0)
        );
    }

    @Test
    public void testRoundScaleEqualToOrGreaterThanOriginal() {
        // Test cases where rounding scale >= original scale (should return original function)

        // Scale 3 rounded to scale 3 (equal)
        createFunctionAndAssert(
                new Decimal32Constant(123456, ColumnType.getDecimalType(6, 3)),
                3,
                123456,
                ColumnType.getDecimalType(6, 3)
        );

        // Scale 2 rounded to scale 5 (greater - should pad with zeros)
        createFunctionAndAssert(
                new Decimal32Constant(12345, ColumnType.getDecimalType(5, 2)),
                5,
                12345,
                ColumnType.getDecimalType(5, 2)
        );

        // Scale 0 rounded to scale 3 (add decimal places)
        createFunctionAndAssert(
                new Decimal16Constant((short) 123, ColumnType.getDecimalType(3, 0)),
                3,
                123,
                ColumnType.getDecimalType(3, 0)
        );
    }

    @Test
    public void testRoundSmallValuesWithLargeNegativeScales() {
        // Test small values with large negative scales (should round to zero)

        // Round 123 with scale -5 -> 0 (rounds to hundred thousands, but 123 < 50000)
        createFunctionAndAssert(
                new Decimal32Constant(123, ColumnType.getDecimalType(5, 0)),
                -5,
                0,
                ColumnType.getDecimalType(6, 0)
        );

        // Round 999 with scale -4 -> 0 (rounds to ten thousands, but 999 < 5000)
        createFunctionAndAssert(
                new Decimal32Constant(999, ColumnType.getDecimalType(5, 0)),
                -4,
                0,
                ColumnType.getDecimalType(6, 0)
        );
    }

    @Test
    public void testRoundZeroWithNegativeScales() {
        // Test rounding zero values with negative scales (should remain zero)

        createFunctionAndAssert(
                new Decimal32Constant(0, ColumnType.getDecimalType(5, 0)),
                -5,
                0,
                ColumnType.getDecimalType(6, 0)
        );

        createFunctionAndAssert(
                new Decimal64Constant(0L, ColumnType.getDecimalType(10, 0)),
                -10,
                0L,
                ColumnType.getDecimalType(11, 0)
        );
    }

    private void createDynamicFunctionAndAssert(Function arg, int scale, long expectedValue, int expectedType) {
        args.clear();
        args.add(arg);
        args.add(new IntConstant(scale) {
            @Override
            public boolean isConstant() {
                return false;
            }
        });
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
            // Function name might be "round" or the original function name if scale >= original scale
            String funcName = func.getName();
            Assert.assertTrue("Expected 'round' or constant function name, got: " + funcName,
                    "round".equals(funcName) || funcName.contains("Constant"));
        }
    }

    private void createDynamicFunctionAndAssert(Function arg, int scale, long hh, long hl, long lh, long ll, int expectedType) {
        args.clear();
        args.add(arg);
        args.add(new IntConstant(scale) {
            @Override
            public boolean isConstant() {
                return false;
            }
        });
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            Decimal256 value = new Decimal256();
            DecimalUtil.load(value, Misc.getThreadLocalDecimal128(), func, null);
            Assert.assertEquals(hh, value.getHh());
            Assert.assertEquals(hl, value.getHl());
            Assert.assertEquals(lh, value.getLh());
            Assert.assertEquals(ll, value.getLl());
            Assert.assertEquals(expectedType, func.getType());
            // Function name might be "round" or the original function name if scale >= original scale
            String funcName = func.getName();
            Assert.assertTrue("Expected 'round' or constant function name, got: " + funcName,
                    "round".equals(funcName) || funcName.contains("Constant"));
        }
    }

    private void createDynamicFunctionAndAssertNull(Function arg, int scaleValue, int expectedType) {
        createDynamicFunctionAndAssert(
                arg,
                scaleValue,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }

    private void createFunctionAndAssert(Function arg, int scaleValue, long expectedValue, int expectedType) {
        args.clear();
        args.add(arg);
        args.add(new IntConstant(scaleValue));
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
            // Function name might be "round" or the original function name if scale >= original scale
            String funcName = func.getName();
            Assert.assertTrue("Expected 'round' or constant function name, got: " + funcName,
                    "round".equals(funcName) || funcName.contains("Constant"));
        }
    }

    private void createFunctionAndAssert(Function arg, int scaleValue, long hh, long hl, long lh, long ll, int expectedType) {
        args.clear();
        args.add(arg);
        args.add(new IntConstant(scaleValue));
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            Decimal256 value = new Decimal256();
            DecimalUtil.load(value, Misc.getThreadLocalDecimal128(), func, null);
            Assert.assertEquals(hh, value.getHh());
            Assert.assertEquals(hl, value.getHl());
            Assert.assertEquals(lh, value.getLh());
            Assert.assertEquals(ll, value.getLl());
            Assert.assertEquals(expectedType, func.getType());
            // Function name might be "round" or the original function name if scale >= original scale
            String funcName = func.getName();
            Assert.assertTrue("Expected 'round' or constant function name, got: " + funcName,
                    "round".equals(funcName) || funcName.contains("Constant"));
        }
    }

    private void createFunctionAndAssertNull(Function arg, int scaleValue, int expectedType) {
        createFunctionAndAssert(
                arg,
                scaleValue,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }
}