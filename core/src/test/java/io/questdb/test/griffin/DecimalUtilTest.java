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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DecimalUtilTest extends AbstractTest {

    @Test
    public void testCreateDecimalConstantDecimal128() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 123L, 456L, 20, 0);
        Assert.assertTrue(result instanceof Decimal128Constant);
    }

    @Test
    public void testCreateDecimalConstantDecimal16() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 9999, 4, 0);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(9999, result.getDecimal16(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal256() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(1L, 2L, 3L, 4L, 40, 0);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testCreateDecimalConstantDecimal32() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 123456789, 9, 0);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal64() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 1234567890123456L, 16, 0);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(1234567890123456L, result.getDecimal64(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal8() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 123, 2, 0);
        Assert.assertTrue(result instanceof Decimal8Constant);
        Assert.assertEquals(123, result.getDecimal8(null));
    }

    @Test
    public void testMaybeRescaleDecimalConstantLargerTargetScale() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 20;
        final int scale = 2;
        final int targetPrecision = 15;
        final int targetScale = 5;
        final ConstantFunction constantFunc = DecimalUtil.createDecimalConstant(0, 0, 0, 12345, precision, scale);
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, targetPrecision, targetScale);
        Assert.assertNotSame(constantFunc, result);
        Assert.assertTrue(result.isConstant());
        Assert.assertEquals(precision + targetScale - scale, ColumnType.getDecimalPrecision(result.getType()));
        Assert.assertEquals(targetScale, ColumnType.getDecimalScale(result.getType()));
    }

    @Test
    public void testMaybeRescaleDecimalConstantNonConstantFunction() {
        final Decimal256 decimal = new Decimal256();
        final Function nonConstantFunc = new TestNonConstantDecimalFunction();
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(nonConstantFunc, decimal, 10, 5);
        Assert.assertSame(nonConstantFunc, result);
    }

    @Test
    public void testMaybeRescaleDecimalConstantNull() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 13;
        final int scale = 2;
        final int targetPrecision = 20;
        final int targetScale = 5;
        final ConstantFunction constantFunc = new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(precision, scale));
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, targetPrecision, targetScale);
        Assert.assertNotSame(constantFunc, result);
        Assert.assertTrue(result.isConstant());
        Assert.assertTrue(result.isNullConstant());
        Assert.assertEquals(targetPrecision, ColumnType.getDecimalPrecision(result.getType()));
        Assert.assertEquals(targetScale, ColumnType.getDecimalScale(result.getType()));
    }

    @Test
    public void testMaybeRescaleDecimalConstantSameScale() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 10;
        final int scale = 5;
        final int targetPrecision = 10;
        final int targetScale = 5;
        final ConstantFunction constantFunc = DecimalUtil.createDecimalConstant(0, 0, 0, 12345, precision, scale);
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, targetPrecision, targetScale);
        Assert.assertSame(constantFunc, result);
    }

    @Test
    public void testMaybeRescaleDecimalConstantSmallerTargetScale() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 10;
        final int scale = 5;
        final int targetPrecision = 10;
        final int targetScale = 2;
        final ConstantFunction constantFunc = DecimalUtil.createDecimalConstant(0, 0, 0, 12345, precision, scale);
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, targetPrecision, targetScale);
        Assert.assertSame(constantFunc, result);
    }

    @Test
    public void testParseDecimalConstantComplexDecimalWithMSuffix() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "-0012.345M", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(-12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantDecimal128() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "12345678901234567890", -1, -1);
        Assert.assertTrue(result instanceof Decimal128Constant);
    }

    @Test
    public void testParseDecimalConstantDecimal16() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "9999", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(9999, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimal256() throws SqlException {
        String largeNumber = "1234567890123456789012345678901234567890";
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), largeNumber, -1, -1);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantDecimal32() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123456789", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantDecimal64() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "1234567890123456", -1, -1);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(1234567890123456L, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantDecimalAtEnd() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimalAtStart() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), ".123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantEmptyString() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsMaxPrecision() {
        String tooLarge = "1234567890123456789012345678901234567890123456789012345678901234567890123456789"; // 80 digits
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), tooLarge, -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed precision of 76");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsMaxScale() {
        // Create a number with 77 decimal places
        String number = "1." + new String(new char[77]).replace('\0', '1');
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), number, -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed scale of 76");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsPrecision() {
        // Try to parse "123456" with precision 5 (should fail)
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123456", 5, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '123456' requires precision of 6 but is limited to 5");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsScale() {
        // Try to parse "123.456" with scale 2 (should fail as it has 3 decimal places)
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.456", -1, 2);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '123.456' has 3 decimal places but scale is limited to 2");
        }
    }

    @Test
    public void testParseDecimalConstantIntegerWithScale() throws SqlException {
        // Parse integer "123" with scale 0
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123", -1, 0);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantInvalidCharacter() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123a45", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "contains invalid character 'a'");
        }
    }

    @Test
    public void testParseDecimalConstantMaxPrecisionBoundary() throws SqlException {
        String maxPrecision = new String(new char[76]).replace('\0', '9');
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), maxPrecision, -1, -1);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantMixedLeadingZerosAndSpaces() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "  00123.45", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantMultipleDots() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "12.34.56", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "contains invalid character '.'");
        }
    }

    @Test
    public void testParseDecimalConstantNegativeWithDecimal() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "-123.45", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(-12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantNegativeWithPrecisionAndScale() throws SqlException {
        // Parse "-123.45" with precision 10 and scale 2
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "-123.45", 10, 2);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(-12345, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantOnlyDecimalPoint() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), ".", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: '.' contains no digits");
        }
    }

    @Test
    public void testParseDecimalConstantOnlySign() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "-", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testParseDecimalConstantOnlySpaces() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "   ", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testParseDecimalConstantOnlyZeros() throws SqlException {
        // "0" is valid and should return 0
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "0", -1, -1);
        Assert.assertTrue(result instanceof Decimal8Constant);
        Assert.assertEquals(0, result.getDecimal8(null));
    }

    @Test
    public void testParseDecimalConstantPrecisionSmallerThanActualDigits() {
        // Try to parse "12345" with precision 3 (should fail)
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "12345", 3, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '12345' requires precision of 5 but is limited to 3");
        }
    }

    @Test
    public void testParseDecimalConstantSignAndDot() {
        try {
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "-.", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: '-.' contains no digits");
        }
    }

    @Test
    public void testParseDecimalConstantSmallNegative() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "-123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(-123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantSmallPositive() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantSpecificScaleExceedsMax() {
        try {
            // Try to parse with scale 77 (exceeds maximum)
            DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123", -1, 77);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed scale of 76");
        }
    }

    @Test
    public void testParseDecimalConstantWithBothPrecisionAndScale() throws SqlException {
        // Parse "123.45" with precision 10 and scale 2
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.45", 10, 2);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(12345, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantWithDecimalAndScale() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.456789", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithDecimalPoint() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.45", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithLeadingSpaces() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "  123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithLeadingZeros() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "00123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithPositiveSign() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "+123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithPrecisionAndScaleLimits() throws SqlException {
        // Test with max precision and scale
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "1.23", 76, 75);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantWithPrecisionForcesSmallerType() throws SqlException {
        // Large number but with small precision forces smaller type
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123", 4, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithScalePadsZeros() throws SqlException {
        // Parse "123" with scale 2 should be like "123.00"
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123", -1, 2);
        Assert.assertTrue(result instanceof Decimal32Constant);
        // The internal representation should be 12300 (123 * 10^2)
        Assert.assertEquals(12300, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithSpecifiedPrecision() throws SqlException {
        // Parse "123.45" with precision 10 (total digits allowed)
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.45", 10, -1);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(12345, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantWithSpecifiedScale() throws SqlException {
        // Parse "123.45" with scale 2 (decimal places)
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123.45", -1, 2);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithTrailingCapitalM() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithTrailingM() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "123m", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantZeroWithDecimal() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, new Decimal256(), "0.123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParsePrecisionEmpty() {
        try {
            DecimalUtil.parsePrecision(0, "", 0, 0);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid DECIMAL type, precision must be a number");
        }
    }

    @Test
    public void testParsePrecisionInvalidNonNumeric() {
        try {
            DecimalUtil.parsePrecision(0, "abc", 0, 3);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid DECIMAL type, precision must be a number");
        }
    }

    @Test
    public void testParsePrecisionLargeValue() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "76", 0, 2);
        Assert.assertEquals(76, precision);
    }

    @Test
    public void testParsePrecisionSingleDigit() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "5", 0, 1);
        Assert.assertEquals(5, precision);
    }

    @Test
    public void testParsePrecisionValid() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "10", 0, 2);
        Assert.assertEquals(10, precision);
    }

    @Test
    public void testParseScaleDoubleDigit() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "15", 0, 2);
        Assert.assertEquals(15, scale);
    }

    @Test
    public void testParseScaleEmpty() {
        try {
            DecimalUtil.parseScale(0, "", 0, 0);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid DECIMAL type, scale must be a number");
        }
    }

    @Test
    public void testParseScaleInvalidNonNumeric() {
        try {
            DecimalUtil.parseScale(0, "xyz", 0, 3);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid DECIMAL type, scale must be a number");
        }
    }

    @Test
    public void testParseScaleValid() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "5", 0, 1);
        Assert.assertEquals(5, scale);
    }

    @Test
    public void testParseScaleZero() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "0", 0, 1);
        Assert.assertEquals(0, scale);
    }

    private static class TestNonConstantDecimalFunction extends DecimalFunction {

        public TestNonConstantDecimalFunction() {
            super(ColumnType.getDecimalType(10, 2));
        }

        @Override
        public byte getDecimal8(Record rec) {
            return 0;
        }

        @Override
        public boolean isConstant() {
            return false;
        }
    }
}