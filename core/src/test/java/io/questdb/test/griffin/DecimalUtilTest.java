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

import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class DecimalUtilTest extends AbstractTest {

    @Test
    public void testParseDecimalConstantSmallPositive() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123", 3);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantSmallNegative() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("-123", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(-123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithDecimalPoint() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123.45", 6);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithTrailingM() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123m", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithTrailingCapitalM() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123M", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithLeadingSpaces() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("  123", 5);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithPositiveSign() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("+123", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithLeadingZeros() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("00123", 5);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimal16() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("9999", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(9999, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimal32() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123456789", 9);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantDecimal64() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("1234567890123456", 16);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(1234567890123456L, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantDecimal128() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("12345678901234567890", 20);
        Assert.assertTrue(result instanceof Decimal128Constant);
    }

    @Test
    public void testParseDecimalConstantDecimal256() throws NumericException {
        String largeNumber = "1234567890123456789012345678901234567890";
        ConstantFunction result = DecimalUtil.parseDecimalConstant(largeNumber, largeNumber.length());
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantWithDecimalAndScale() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123.456789", 10);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantNegativeWithDecimal() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("-123.45", 7);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(-12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantOnlyDecimalPoint() {
        try {
            DecimalUtil.parseDecimalConstant(".", 1);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantEmptyString() {
        try {
            DecimalUtil.parseDecimalConstant("", 0);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantOnlySpaces() {
        try {
            DecimalUtil.parseDecimalConstant("   ", 3);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantInvalidCharacter() {
        try {
            DecimalUtil.parseDecimalConstant("123a45", 6);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantMultipleDots() {
        try {
            DecimalUtil.parseDecimalConstant("12.34.56", 8);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantExceedsMaxPrecision() {
        String tooLarge = "1234567890123456789012345678901234567890123456789012345678901234567890123456789"; // 80 digits
        try {
            DecimalUtil.parseDecimalConstant(tooLarge, tooLarge.length());
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantOnlySign() {
        try {
            DecimalUtil.parseDecimalConstant("-", 1);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantSignAndDot() {
        try {
            DecimalUtil.parseDecimalConstant("-.", 2);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantZeroWithDecimal() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("0.123", 5);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimalAtStart() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(".123", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimalAtEnd() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("123.", 4);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal8() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 123, 2, 0);
        Assert.assertTrue(result instanceof Decimal8Constant);
        Assert.assertEquals(123, result.getDecimal8(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal16() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 9999, 4, 0);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(9999, result.getDecimal16(null));
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
    public void testCreateDecimalConstantDecimal128() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 123L, 456L, 20, 0);
        Assert.assertTrue(result instanceof Decimal128Constant);
    }

    @Test
    public void testCreateDecimalConstantDecimal256() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(1L, 2L, 3L, 4L, 40, 0);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParsePrecisionValid() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "10", 0, 2);
        Assert.assertEquals(10, precision);
    }

    @Test
    public void testParsePrecisionSingleDigit() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "5", 0, 1);
        Assert.assertEquals(5, precision);
    }

    @Test
    public void testParsePrecisionLargeValue() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "76", 0, 2);
        Assert.assertEquals(76, precision);
    }

    @Test
    public void testParsePrecisionInvalidNonNumeric() {
        try {
            DecimalUtil.parsePrecision(0, "abc", 0, 3);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("invalid DECIMAL precision"));
        }
    }

    @Test
    public void testParsePrecisionEmpty() {
        try {
            DecimalUtil.parsePrecision(0, "", 0, 0);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("invalid DECIMAL precision"));
        }
    }

    @Test
    public void testParseScaleValid() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "5", 0, 1);
        Assert.assertEquals(5, scale);
    }

    @Test
    public void testParseScaleDoubleDigit() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "15", 0, 2);
        Assert.assertEquals(15, scale);
    }

    @Test
    public void testParseScaleZero() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "0", 0, 1);
        Assert.assertEquals(0, scale);
    }

    @Test
    public void testParseScaleInvalidNonNumeric() {
        try {
            DecimalUtil.parseScale(0, "xyz", 0, 3);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("invalid DECIMAL scale"));
        }
    }

    @Test
    public void testParseScaleEmpty() {
        try {
            DecimalUtil.parseScale(0, "", 0, 0);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("invalid DECIMAL scale"));
        }
    }

    @Test
    public void testParseDecimalConstantMaxPrecisionBoundary() throws NumericException {
        String maxPrecision = new String(new char[76]).replace('\0', '9');
        ConstantFunction result = DecimalUtil.parseDecimalConstant(maxPrecision, maxPrecision.length());
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantComplexDecimalWithMSuffix() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("-0012.345M", 10);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(-12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantOnlyZeros() {
        try {
            DecimalUtil.parseDecimalConstant("0000", 4);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testParseDecimalConstantMixedLeadingZerosAndSpaces() throws NumericException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant("  00123.45", 10);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }
}