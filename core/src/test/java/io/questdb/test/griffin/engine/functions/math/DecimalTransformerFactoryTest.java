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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.math.DecimalTransformer;
import io.questdb.griffin.engine.functions.math.DecimalTransformerFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import org.junit.Assert;
import org.junit.Test;

public class DecimalTransformerFactoryTest {
    Decimal128 decimal128 = new Decimal128();
    Decimal256 decimal256 = new Decimal256();

    @Test
    public void testAddOneTransformerWithScale() {
        // Test AddOneTransformer to verify it adds 1 correctly with scale
        Function sourceFunc = new Decimal16Constant((short) 250, ColumnType.getDecimalType(4, 2)); // 2.50
        DecimalTransformer transformer = new AddOneTransformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);

        int transformedValue = result.getDecimal32(null);
        // Adding 1 with scale 2 means adding 100 (1.00)
        Assert.assertEquals(350, transformedValue); // 2.50 + 1.00 = 3.50 (stored as 350 with scale 2)
    }

    @Test
    public void testAllCombinations() {
        // This test ensures all 36 combinations work without throwing exceptions
        int[] decimalTypes = {
                ColumnType.DECIMAL8,
                ColumnType.DECIMAL16,
                ColumnType.DECIMAL32,
                ColumnType.DECIMAL64,
                ColumnType.DECIMAL128,
                ColumnType.DECIMAL256
        };

        Function[] sourceFunctions = {
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(3, 1)),
                new Decimal32Constant(1000, ColumnType.getDecimalType(5, 1)),
                new Decimal64Constant(10000L, ColumnType.getDecimalType(10, 1)),
                new Decimal128Constant(0, 100000, ColumnType.getDecimalType(19, 1)),
                new Decimal256Constant(0, 0, 0, 1000000, ColumnType.getDecimalType(39, 1))
        };

        DecimalTransformer transformer = new MultiplyBy2Transformer();

        for (int i = 0; i < decimalTypes.length; i++) {
            for (int j = 0; j < decimalTypes.length; j++) {
                Function sourceFunc = sourceFunctions[i];
                int targetType = decimalTypes[j];

                try {
                    DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, targetType, transformer);
                    Assert.assertNotNull("Result should not be null for source type " + i + " to target type " + j, result);

                    // Try to get a value to ensure the function works
                    switch (ColumnType.tagOf(targetType)) {
                        case ColumnType.DECIMAL8:
                            result.getDecimal8(null);
                            break;
                        case ColumnType.DECIMAL16:
                            result.getDecimal16(null);
                            break;
                        case ColumnType.DECIMAL32:
                            result.getDecimal32(null);
                            break;
                        case ColumnType.DECIMAL64:
                            result.getDecimal64(null);
                            break;
                        case ColumnType.DECIMAL128:
                            result.getDecimal128(null, decimal128);
                            break;
                        case ColumnType.DECIMAL256:
                            result.getDecimal256(null, decimal256);
                            break;
                    }
                } catch (Exception e) {
                    Assert.fail("Failed for source type index " + i + " to target type index " + j + ": " + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testCrossTypeTransformationAccuracy() {
        // Test transformation from smaller to larger type maintains accuracy
        Function sourceFunc = new Decimal16Constant((byte) 123, ColumnType.getDecimalType(3, 2)); // 1.23
        DecimalTransformer transformer = new AddOneTransformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);

        long transformedValue = result.getDecimal64(null);
        // Adding 1 with scale 2 means adding 100 (1.00)
        Assert.assertEquals(223, transformedValue); // 1.23 + 1.00 = 2.23 (stored as 223 with scale 2)
    }

    @Test
    public void testDecimal128To128() {
        Function sourceFunc = new Decimal128Constant(0, 999, ColumnType.getDecimalType(19, 5));
        DecimalTransformer transformer = new AddOneTransformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);

        result.getDecimal128(null, decimal128);
        Assert.assertEquals(0, decimal128.getHigh());
        // Adding 1 with scale 5 means adding 100000
        Assert.assertEquals(999 + 100000, decimal128.getLow());
    }

    @Test
    public void testDecimal128To64() {
        Function sourceFunc = new Decimal128Constant(0, 5000, ColumnType.getDecimalType(19, 4));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);

        long transformedValue = result.getDecimal64(null);
        Assert.assertEquals(10000L, transformedValue);
    }

    @Test
    public void testDecimal16To8() {
        Function sourceFunc = new Decimal16Constant((short) 50, ColumnType.getDecimalType(4, 2));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);

        byte transformedValue = result.getDecimal8(null);
        Assert.assertEquals(100, transformedValue);
    }

    @Test
    public void testDecimal256To128() {
        Function sourceFunc = new Decimal256Constant(0, 0, 0, 7500, ColumnType.getDecimalType(39, 5));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);

        result.getDecimal128(null, decimal128);
        Assert.assertEquals(0, decimal128.getHigh());
        Assert.assertEquals(15000, decimal128.getLow());
    }

    @Test
    public void testDecimal256To256() {
        Function sourceFunc = new Decimal256Constant(0, 0, 0, 2500, ColumnType.getDecimalType(39, 6));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);

        result.getDecimal256(null, decimal256);
        Assert.assertEquals(0, decimal256.getHh());
        Assert.assertEquals(0, decimal256.getHl());
        Assert.assertEquals(0, decimal256.getLh());
        Assert.assertEquals(5000, decimal256.getLl());
    }

    @Test
    public void testDecimal32To16() {
        Function sourceFunc = new Decimal32Constant(100, ColumnType.getDecimalType(5, 2));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);

        short transformedValue = result.getDecimal16(null);
        Assert.assertEquals(200, transformedValue);
    }

    @Test
    public void testDecimal64To32() {
        Function sourceFunc = new Decimal64Constant(1000L, ColumnType.getDecimalType(10, 3));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);

        int transformedValue = result.getDecimal32(null);
        Assert.assertEquals(2000, transformedValue);
    }

    @Test
    public void testDecimal8To128() {
        Function sourceFunc = new Decimal8Constant((byte) 42, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);

        result.getDecimal128(null, decimal128);
        Assert.assertEquals(0, decimal128.getHigh());
        Assert.assertEquals(84, decimal128.getLow());
    }

    @Test
    public void testDecimal8To16() {
        Function sourceFunc = new Decimal8Constant((byte) 42, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);

        short transformedValue = result.getDecimal16(null);
        Assert.assertEquals(84, transformedValue);
    }

    @Test
    public void testDecimal8To256() {
        Function sourceFunc = new Decimal8Constant((byte) 42, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);

        result.getDecimal256(null, decimal256);
        Assert.assertEquals(0, decimal256.getHh());
        Assert.assertEquals(0, decimal256.getHl());
        Assert.assertEquals(0, decimal256.getLh());
        Assert.assertEquals(84, decimal256.getLl());
    }

    @Test
    public void testDecimal8To32() {
        Function sourceFunc = new Decimal8Constant((byte) 42, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);

        int transformedValue = result.getDecimal32(null);
        Assert.assertEquals(84, transformedValue);
    }

    @Test
    public void testDecimal8To64() {
        Function sourceFunc = new Decimal8Constant((byte) 42, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);

        long transformedValue = result.getDecimal64(null);
        Assert.assertEquals(84L, transformedValue);
    }

    @Test
    public void testDecimal8To8() {
        Function sourceFunc = new Decimal8Constant((byte) 42, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);

        byte transformedValue = result.getDecimal8(null);
        Assert.assertEquals(84, transformedValue);
    }

    @Test
    public void testDownsizingTransformation() {
        // Test transformation from larger to smaller type
        Function sourceFunc = new Decimal64Constant(50000L, ColumnType.getDecimalType(10, 4)); // 5.0000
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);

        int transformedValue = result.getDecimal32(null);
        Assert.assertEquals(100000, transformedValue); // 5.0000 * 2 = 10.0000 (stored as 100000 with scale 4)
    }

    @Test
    public void testGetArg() {
        // Test that getArg() returns the original source function
        Function sourceFunc = new Decimal32Constant(100, ColumnType.getDecimalType(5, 2));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);

        Assert.assertSame(sourceFunc, ((UnaryFunction) result).getArg());
    }

    @Test
    public void testGetName() {
        // Test that getName() returns the transformer's name
        Function sourceFunc = new Decimal16Constant((short) 50, ColumnType.getDecimalType(3, 1));
        DecimalTransformer multiplyTransformer = new MultiplyBy2Transformer();
        DecimalTransformer addTransformer = new AddOneTransformer();

        DecimalFunction multiplyResult = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, multiplyTransformer);
        DecimalFunction addResult = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, addTransformer);

        Assert.assertEquals("multiplyBy2", multiplyResult.getName());
        Assert.assertEquals("addOne", addResult.getName());
    }

    @Test
    public void testHighPrecisionTransformation() {
        // Test with Decimal128 to ensure high precision transformations work
        Function sourceFunc = new Decimal128Constant(0, 999999999L, ColumnType.getDecimalType(19, 8)); // 9.99999999
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);

        result.getDecimal128(null, decimal128);
        Assert.assertEquals(0, decimal128.getHigh());
        Assert.assertEquals(1999999998L, decimal128.getLow()); // 9.99999999 * 2 = 19.99999998
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvalidTypeThrowsException() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalTransformerFactory.newInstance(IntConstant.ZERO, ColumnType.DECIMAL8, transformer);
    }

    @Test
    public void testMaxPrecisionTransformation() {
        // Test with Decimal256 to ensure maximum precision transformations work
        Function sourceFunc = new Decimal256Constant(0, 0, 0, 5000000000L, ColumnType.getDecimalType(39, 9)); // 5.000000000
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);

        result.getDecimal256(null, decimal256);
        Assert.assertEquals(0, decimal256.getHh());
        Assert.assertEquals(0, decimal256.getHl());
        Assert.assertEquals(0, decimal256.getLh());
        Assert.assertEquals(10000000000L, decimal256.getLl()); // 5.000000000 * 2 = 10.000000000
    }

    @Test
    public void testMixedScaleTransformations() {
        // Test various scale combinations to ensure robustness
        DecimalTransformer multiplyTransformer = new MultiplyBy2Transformer();

        // Scale 0 (integer)
        Function intSource = new Decimal32Constant(42, ColumnType.getDecimalType(5, 0));
        DecimalFunction intResult = DecimalTransformerFactory.newInstance(intSource, ColumnType.DECIMAL64, multiplyTransformer);
        Assert.assertEquals(84, intResult.getDecimal64(null)); // 42 * 2 = 84

        // Scale 1 (one decimal place)
        Function scale1Source = new Decimal32Constant(425, ColumnType.getDecimalType(5, 1)); // 42.5
        DecimalFunction scale1Result = DecimalTransformerFactory.newInstance(scale1Source, ColumnType.DECIMAL64, multiplyTransformer);
        Assert.assertEquals(850, scale1Result.getDecimal64(null)); // 42.5 * 2 = 85.0

        // Scale 5 (five decimal places)
        Function scale5Source = new Decimal64Constant(4250000L, ColumnType.getDecimalType(10, 5)); // 42.50000
        DecimalFunction scale5Result = DecimalTransformerFactory.newInstance(scale5Source, ColumnType.DECIMAL128, multiplyTransformer);
        scale5Result.getDecimal128(null, decimal128);
        Assert.assertEquals(0, decimal128.getHigh());
        Assert.assertEquals(8500000L, decimal128.getLow()); // 42.50000 * 2 = 85.00000
    }

    @Test
    public void testNegativeValueTransformation() {
        // Test that negative values are transformed correctly
        Function sourceFunc = new Decimal16Constant((short) -500, ColumnType.getDecimalType(4, 2)); // -5.00
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);

        int transformedValue = result.getDecimal32(null);
        Assert.assertEquals(-1000, transformedValue); // -5.00 * 2 = -10.00 (stored as -1000 with scale 2)
    }

    @Test
    public void testNullHandling() {
        // Test null handling for Decimal8 to Decimal16
        Function sourceFunc = new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 1));
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);

        short transformedValue = result.getDecimal16(null);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, transformedValue);
    }

    @Test
    public void testNullHandlingDecimal128ToAll() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        Function sourceFunc = new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(19, 1));

        // Test Decimal128 to Decimal8
        DecimalFunction result8 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);
        Assert.assertEquals(Decimals.DECIMAL8_NULL, result8.getDecimal8(null));

        // Test Decimal128 to Decimal16
        DecimalFunction result16 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, result16.getDecimal16(null));

        // Test Decimal128 to Decimal32
        DecimalFunction result32 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);
        Assert.assertEquals(Decimals.DECIMAL32_NULL, result32.getDecimal32(null));

        // Test Decimal128 to Decimal64
        DecimalFunction result64 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, result64.getDecimal64(null));

        // Test Decimal128 to Decimal128
        DecimalFunction result128 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);
        result128.getDecimal128(null, decimal128);
        Assert.assertTrue(decimal128.isNull());

        // Test Decimal128 to Decimal256
        DecimalFunction result256 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);
        result256.getDecimal256(null, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testNullHandlingDecimal16ToAll() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        Function sourceFunc = new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(3, 1));

        // Test Decimal16 to Decimal8
        DecimalFunction result8 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);
        Assert.assertEquals(Decimals.DECIMAL8_NULL, result8.getDecimal8(null));

        // Test Decimal16 to Decimal16
        DecimalFunction result16 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, result16.getDecimal16(null));

        // Test Decimal16 to Decimal32
        DecimalFunction result32 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);
        Assert.assertEquals(Decimals.DECIMAL32_NULL, result32.getDecimal32(null));

        // Test Decimal16 to Decimal64
        DecimalFunction result64 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, result64.getDecimal64(null));

        // Test Decimal16 to Decimal128
        DecimalFunction result128 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);
        result128.getDecimal128(null, decimal128);
        Assert.assertTrue(decimal128.isNull());

        // Test Decimal16 to Decimal256
        DecimalFunction result256 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);
        result256.getDecimal256(null, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testNullHandlingDecimal256ToAll() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        Function sourceFunc = new Decimal256Constant(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                ColumnType.getDecimalType(39, 1));

        // Test Decimal256 to Decimal8
        DecimalFunction result8 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);
        Assert.assertEquals(Decimals.DECIMAL8_NULL, result8.getDecimal8(null));

        // Test Decimal256 to Decimal16
        DecimalFunction result16 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, result16.getDecimal16(null));

        // Test Decimal256 to Decimal32
        DecimalFunction result32 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);
        Assert.assertEquals(Decimals.DECIMAL32_NULL, result32.getDecimal32(null));

        // Test Decimal256 to Decimal64
        DecimalFunction result64 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, result64.getDecimal64(null));

        // Test Decimal256 to Decimal128
        DecimalFunction result128 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);
        result128.getDecimal128(null, decimal128);
        Assert.assertTrue(decimal128.isNull());

        // Test Decimal256 to Decimal256
        DecimalFunction result256 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);
        result256.getDecimal256(null, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testNullHandlingDecimal32ToAll() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        Function sourceFunc = new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(5, 1));

        // Test Decimal32 to Decimal8
        DecimalFunction result8 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);
        Assert.assertEquals(Decimals.DECIMAL8_NULL, result8.getDecimal8(null));

        // Test Decimal32 to Decimal16
        DecimalFunction result16 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, result16.getDecimal16(null));

        // Test Decimal32 to Decimal32
        DecimalFunction result32 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);
        Assert.assertEquals(Decimals.DECIMAL32_NULL, result32.getDecimal32(null));

        // Test Decimal32 to Decimal64
        DecimalFunction result64 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, result64.getDecimal64(null));

        // Test Decimal32 to Decimal128
        DecimalFunction result128 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);
        result128.getDecimal128(null, decimal128);
        Assert.assertTrue(decimal128.isNull());

        // Test Decimal32 to Decimal256
        DecimalFunction result256 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);
        result256.getDecimal256(null, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testNullHandlingDecimal64ToAll() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        Function sourceFunc = new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 1));

        // Test Decimal64 to Decimal8
        DecimalFunction result8 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);
        Assert.assertEquals(Decimals.DECIMAL8_NULL, result8.getDecimal8(null));

        // Test Decimal64 to Decimal16
        DecimalFunction result16 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, result16.getDecimal16(null));

        // Test Decimal64 to Decimal32
        DecimalFunction result32 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);
        Assert.assertEquals(Decimals.DECIMAL32_NULL, result32.getDecimal32(null));

        // Test Decimal64 to Decimal64
        DecimalFunction result64 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, result64.getDecimal64(null));

        // Test Decimal64 to Decimal128
        DecimalFunction result128 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);
        result128.getDecimal128(null, decimal128);
        Assert.assertTrue(decimal128.isNull());

        // Test Decimal64 to Decimal256
        DecimalFunction result256 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);
        result256.getDecimal256(null, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testNullHandlingDecimal8ToAll() {
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        Function sourceFunc = new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 1));

        // Test Decimal8 to Decimal8
        DecimalFunction result8 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL8, transformer);
        Assert.assertEquals(Decimals.DECIMAL8_NULL, result8.getDecimal8(null));

        // Test Decimal8 to Decimal16
        DecimalFunction result16 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);
        Assert.assertEquals(Decimals.DECIMAL16_NULL, result16.getDecimal16(null));

        // Test Decimal8 to Decimal32
        DecimalFunction result32 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL32, transformer);
        Assert.assertEquals(Decimals.DECIMAL32_NULL, result32.getDecimal32(null));

        // Test Decimal8 to Decimal64
        DecimalFunction result64 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, result64.getDecimal64(null));

        // Test Decimal8 to Decimal128
        DecimalFunction result128 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL128, transformer);
        result128.getDecimal128(null, decimal128);
        Assert.assertTrue(decimal128.isNull());

        // Test Decimal8 to Decimal256
        DecimalFunction result256 = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL256, transformer);
        result256.getDecimal256(null, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testScaleHandlingInTransformation() {
        // Test with different scales to ensure proper handling
        Function sourceFunc = new Decimal32Constant(12345, ColumnType.getDecimalType(8, 3)); // 12.345
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);

        long transformedValue = result.getDecimal64(null);
        Assert.assertEquals(24690, transformedValue); // 12.345 * 2 = 24.690 (stored as 24690 with scale 3)
    }

    @Test
    public void testTransformationActuallyWorks() {
        // Test that transformations actually modify values correctly

        // Test simple multiplication with scale preservation
        Function sourceFunc = new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 1)); // 5.0
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL16, transformer);

        short transformedValue = result.getDecimal16(null);
        Assert.assertEquals(100, transformedValue); // 5.0 * 2 = 10.0 (stored as 100 with scale 1)
    }

    @Test
    public void testZeroValueTransformation() {
        // Test that zero values are transformed correctly
        Function sourceFunc = new Decimal32Constant(0, ColumnType.getDecimalType(5, 3)); // 0.000
        DecimalTransformer transformer = new MultiplyBy2Transformer();
        DecimalFunction result = DecimalTransformerFactory.newInstance(sourceFunc, ColumnType.DECIMAL64, transformer);

        long transformedValue = result.getDecimal64(null);
        Assert.assertEquals(0, transformedValue); // 0.000 * 2 = 0.000
    }

    // Test transformer that adds 1 to the value
    private static class AddOneTransformer implements DecimalTransformer {
        @Override
        public String getName() {
            return "addOne";
        }

        @Override
        public boolean transform(Decimal128 value, Record record) {
            long lo = value.getLow();
            int scale = value.getScale();
            long factor = 1;
            for (int i = 0; i < scale; i++) {
                factor *= 10;
            }
            value.of(0, lo + factor, scale);
            return true;
        }

        @Override
        public boolean transform(Decimal256 value, Record record) {
            long ll = value.getLl();
            int scale = value.getScale();
            long factor = 1;
            for (int i = 0; i < scale; i++) {
                factor *= 10;
            }
            value.ofLong(ll + factor, scale);
            return true;
        }

        @Override
        public boolean transform(Decimal64 value, Record record) {
            long v = value.getValue();
            int scale = value.getScale();
            long factor = 1;
            for (int i = 0; i < scale; i++) {
                factor *= 10;
            }
            value.of(v + factor, scale);
            return true;
        }
    }

    // Test transformer that multiplies the value by 2
    private static class MultiplyBy2Transformer implements DecimalTransformer {
        @Override
        public String getName() {
            return "multiplyBy2";
        }

        @Override
        public boolean transform(Decimal128 value, Record record) {
            // For simplicity, we'll just double the low part
            long lo = value.getLow();
            value.of(0, lo * 2, value.getScale());
            return true;
        }

        @Override
        public boolean transform(Decimal256 value, Record record) {
            // For simplicity, we'll just double the lowest part
            long ll = value.getLl();
            value.ofLong(ll * 2, value.getScale());
            return true;
        }

        @Override
        public boolean transform(Decimal64 value, Record record) {
            long v = value.getValue();
            value.of(v * 2, value.getScale());
            return true;
        }
    }
}