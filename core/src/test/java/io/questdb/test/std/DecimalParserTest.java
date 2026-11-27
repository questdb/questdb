/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.std;

import io.questdb.std.Decimal;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.DecimalParser;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DecimalParserTest {

    @Test
    public void testExponentAtIntLimits() throws NumericException {
        // an exponent can be anything an int holds; moving the dot by it must not wrap round, so a value out
        // of range is rejected with the digits it needs, a zero stays zero and a lossy parse truncates to zero
        for (Decimal decimal : new Decimal[]{new Decimal64(), new Decimal128(), new Decimal256()}) {
            // 1e2147483647 has 2147483648 integer digits, two more for the scale
            assertNotParsed(decimal, "decimal '1e2147483647' requires precision of 2147483650 but is limited to 10", "1e2147483647", 10, 2);
            assertNotParsed(decimal, "decimal '-1e2147483647' requires precision of 2147483650 but is limited to 10", "-1e2147483647", 10, 2);
            assertNotParsed(decimal, "decimal '9.5e2147483647' requires precision of 2147483650 but is limited to 10", "9.5e2147483647", 10, 2);
            assertNotParsed(decimal, "decimal '1e2147483646' requires precision of 2147483649 but is limited to 10", "1e2147483646", 10, 2);
            assertNotParsed(decimal, "decimal '1e2147483647' exceeds maximum allowed precision of " + decimal.getMaxPrecision(), "1e2147483647", -1, -1);
            assertNotParsed(decimal, "decimal '1e2147483646' exceeds maximum allowed precision of " + decimal.getMaxPrecision(), "1e2147483646", -1, -1);
            // 1e-2147483648 and 1.5e-2147483647 have 2147483648 decimal places
            assertNotParsed(decimal, "decimal '1e-2147483648' has 2147483648 decimal places but scale is limited to 2", "1e-2147483648", 10, 2);
            assertNotParsed(decimal, "decimal '-1e-2147483648' has 2147483648 decimal places but scale is limited to 2", "-1e-2147483648", 10, 2);
            assertNotParsed(decimal, "decimal '1.5e-2147483647' has 2147483648 decimal places but scale is limited to 2", "1.5e-2147483647", 10, 2);
            assertNotParsed(decimal, "decimal '9e-2147483647' has 2147483647 decimal places but scale is limited to 2", "9e-2147483647", 10, 2);
            assertNotParsed(decimal, "decimal '0e-2147483648' has 2147483648 decimal places but scale is limited to 2", "0e-2147483648", 10, 2);
            assertNotParsed(decimal, "decimal '1e-2147483648' exceeds maximum allowed scale of " + decimal.getMaxScale(), "1e-2147483648", -1, -1);
            assertNotParsed(decimal, "decimal '1.5e-2147483647' exceeds maximum allowed scale of " + decimal.getMaxScale(), "1.5e-2147483647", -1, -1);
            assertNotParsed(decimal, "decimal '9e-2147483647' exceeds maximum allowed scale of " + decimal.getMaxScale(), "9e-2147483647", -1, -1);
        }
        assertParsed("0.00", 2, 2, "0e2147483647", 10, 2);
        assertParsed("0.00", 2, 2, "-0e2147483647", 10, 2);
        assertParsed("0", 1, 0, "0e2147483647", -1, -1);
        assertParsed("0.00", 2, 2, "1e-2147483648", 10, 2, true, true);
        assertParsed("0.00", 2, 2, "-1e-2147483648", 10, 2, true, true);
        assertParsed("0.00", 2, 2, "1.5e-2147483647", 10, 2, true, true);
        assertParsed("0.00", 2, 2, "0e-2147483648", 10, 2, true, true);
    }

    @Test
    public void testExponentBelowOneFitsFullScale() throws NumericException {
        // 1e-5 is 0.00001, five fraction digits and no integer digit
        assertParsed("0.00001", 5, 5, "1e-5", 5, 5);
        assertParsed("0.00001", 5, 5, "0.00001", 5, 5);
        assertParsed("0.00001", 5, 5, "1e-5", -1, -1);
    }

    @Test
    public void testExponentZeroFitsFullScale() throws NumericException {
        // the exponent of a zero mantissa shifts nothing, it must not inflate the precision either
        assertParsed("0", 1, 0, "0e5", -1, -1);
        assertParsed("0", 1, 0, "0.0e10", -1, -1);
        assertParsed("0.00000", 5, 5, "0e-5", -1, -1);
        assertParsed("0.000", 3, 3, "0e5", 3, 3);
        assertParsed("0.000", 3, 3, "0.0e10", 3, 3);
        assertParsed("0.000", 3, 3, "0e-3", 3, 3);
        assertParsed("0.000", 3, 3, "-0e5", 3, 3);
    }

    @Test
    public void testFullScaleAcceptsValueBelowOne() throws NumericException {
        assertParsed("0.5", 1, 1, "0.5", 1, 1);
        assertParsed("0.5", 1, 1, ".5", 1, 1);
        assertParsed("-0.5", 1, 1, "-.5", 1, 1);
        assertParsed("0.125", 3, 3, "0.125", 3, 3);
        assertParsed("0.125", 3, 3, ".125", 3, 3);
        assertParsed("-0.125", 3, 3, "-0.125", 3, 3);
        assertParsed("0.125", 3, 3, "0.125m", 3, 3);

        final String d38 = "0." + "1".repeat(38);
        assertParsed(d38, 38, 38, d38, 38, 38);
        final String d76 = "0." + "9".repeat(76);
        assertParsed(d76, 76, 76, d76, 76, 76);
        assertParsed("-" + d76, 76, 76, "-" + d76, 76, 76);
    }

    @Test
    public void testFullScaleInfersTightestPrecision() throws NumericException {
        // no precision or scale given, the leading zero must not inflate the inferred precision
        assertParsed("0.5", 1, 1, "0.5m", -1, -1);
        assertParsed("0.125", 3, 3, "0.125m", -1, -1);
        assertParsed("-0.125", 3, 3, "-0.125m", -1, -1);
        final String d76 = "0." + "9".repeat(76);
        assertParsed(d76, 76, 76, d76 + "m", -1, -1);
    }

    @Test
    public void testFullScaleRejectsValueOfOneOrMore() {
        assertNotParsed("decimal '1.5' requires precision of 4 but is limited to 3", "1.5", 3, 3);
        assertNotParsed("decimal '1.0' requires precision of 2 but is limited to 1", "1.0", 1, 1);
        assertNotParsed("decimal '-1.125' requires precision of 4 but is limited to 3", "-1.125", 3, 3);
        assertNotParsed("decimal '9.999' requires precision of 4 but is limited to 3", "9.999", 3, 3);
        // integer digits are counted even when the fraction is shorter than the scale
        assertNotParsed("decimal '12' requires precision of 5 but is limited to 3", "12", 3, 3);
        assertNotParsed("decimal '10' requires precision of 5 but is limited to 3", "10", 3, 3);
        assertNotParsed("decimal '1' requires precision of 2 but is limited to 1", "1", 1, 1);
        // a value below one still has to fit the scale
        assertNotParsed("decimal '0.0001' has 4 decimal places but scale is limited to 3", "0.0001", 3, 3);
    }

    @Test
    public void testLeadingZeroesAreNotSignificant() throws NumericException {
        // the exponent lifts the digits past the leading zeroes, which stop counting towards the precision
        assertParsed("1", 1, 0, "0.01e2", -1, -1);
        assertParsed("0.5", 1, 1, "0.05e1", -1, -1);
        assertParsed("0.001", 3, 3, "0.1e-2", -1, -1);
        assertParsed("123", 3, 0, "00.123e3", -1, -1);
        // without an exponent the scale already accounts for them
        assertParsed("0.001", 3, 3, "0.001", -1, -1);
        assertParsed("0.050", 3, 3, "0.05", 3, 3);
    }

    @Test
    public void testNanAndInfinityParseToNull() throws NumericException {
        assertNullParsed("NaN", 3, 3);
        assertNullParsed("-NaN", 1, 1);
        assertNullParsed("Infinity", 76, 76);
        assertNullParsed("-Infinity", -1, -1);
    }

    @Test
    public void testPrecisionIsAtLeastOne() throws NumericException {
        // trailing zeroes are stripped, leaving no digit at all
        assertParsed("0", 1, 0, "0", -1, -1, false, false);
        assertParsed("0", 1, 0, "0.0", -1, -1, false, false);
        assertParsed("0", 1, 0, "0.000", -1, -1, false, false);
        assertParsed("0", 1, 0, "0.", -1, -1, false, false);
        assertParsed("0", 1, 0, ".0", -1, -1, false, false);
        assertParsed("0", 1, 0, "0.0", 1, 0, false, false);
    }

    @Test
    public void testScaleSmallerThanPrecisionUnchanged() throws NumericException {
        assertParsed("123.45", 5, 2, "123.45", -1, -1);
        assertParsed("123.45", 5, 2, "123.45", 5, 2);
        assertParsed("123.45", 5, 2, "123.45", 10, 2);
        assertParsed("-123.45", 5, 2, "-123.45", 5, 2);
        assertParsed("0.5", 1, 1, "0.5", 10, 1);
        assertParsed("1.5", 2, 1, "1.5", -1, -1);
        assertParsed("123000", 6, 0, "1.23e5", -1, -1);
        assertNotParsed("decimal '123.45' requires precision of 5 but is limited to 4", "123.45", 4, 2);
    }

    @Test
    public void testTrailingZeroesPaddedToTargetScale() throws NumericException {
        // the value is widened to the target scale, the padding zeroes count towards the precision
        assertParsed("0.500", 3, 3, "0.5", 3, 3);
        assertParsed("0.100", 3, 3, ".1", 3, 3);
        assertParsed("0.000", 3, 3, "0.0", 3, 3);
    }

    @Test
    public void testTruncatedValueFitsFullScale() throws NumericException {
        // lossy truncation drops the digits beyond the target scale
        assertParsed("0.12", 2, 2, "0.129", 2, 2, false, true);
        assertParsed("0", 1, 0, "0.5", 1, 0, false, true);
        // truncation can cut into the leading zeroes, leaving no significant digit at all
        assertParsed("0.00", 2, 2, "0.0001", 2, 2, false, true);
        assertParsed("0.0", 1, 1, "0.0999", 1, 1, false, true);
    }

    @Test
    public void testZeroFitsEveryFullScale() throws NumericException {
        // zero has no significant digit, so its text form fits a DECIMAL(p,p) of any width
        assertParsed("0.0", 1, 1, "0", 1, 1);
        assertParsed("0.0", 1, 1, "-0", 1, 1);
        assertParsed("0.000", 3, 3, "0", 3, 3);
        assertParsed("0.000", 3, 3, "-0", 3, 3);
        assertParsed("0.000", 3, 3, "+0", 3, 3);
        assertParsed("0.000", 3, 3, "0.", 3, 3);
        assertParsed("0.000", 3, 3, ".0", 3, 3);
        assertParsed("0.000", 3, 3, "000", 3, 3);
        assertParsed("0.000", 3, 3, "00.00", 3, 3);
        assertParsed("0.000", 3, 3, "0.000", 3, 3);
        assertParsed("0.000", 3, 3, "0.0", 3, 3, false, false);
        assertParsed("0.000", 3, 3, "0m", 3, 3, false, false);

        final String z38 = "0." + "0".repeat(38);
        assertParsed(z38, 38, 38, "0", 38, 38);
        assertParsed(z38, 38, 38, "-0", 38, 38);
        final String z76 = "0." + "0".repeat(76);
        assertParsed(z76, 76, 76, "0", 76, 76);
        assertParsed(z76, 76, 76, "-0", 76, 76);
        assertParsed(z76, 76, 76, "000", 76, 76);
    }

    private static void assertNotParsed(Decimal decimal, String expectedMessage, String value, int precision, int scale) {
        try {
            DecimalParser.parse(decimal, value, 0, value.length(), precision, scale, true, false);
            Assert.fail("expected '" + value + "' to be rejected by " + decimal.getClass().getSimpleName());
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
    }

    private static void assertNotParsed(String expectedMessage, String value, int precision, int scale) {
        try {
            new Decimal256().ofString(value, 0, value.length(), precision, scale, true, false);
            Assert.fail("expected '" + value + "' to be rejected");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
    }

    private static void assertNullParsed(String value, int precision, int scale) throws NumericException {
        Decimal256 decimal256 = new Decimal256();
        Assert.assertEquals(0, decimal256.ofString(value, 0, value.length(), precision, scale, true, false));
        Assert.assertTrue(decimal256.isNull());
    }

    private static void assertParsed(
            String expectedValue,
            int expectedPrecision,
            int expectedScale,
            String value,
            int precision,
            int scale
    ) throws NumericException {
        assertParsed(expectedValue, expectedPrecision, expectedScale, value, precision, scale, true, false);
    }

    private static void assertParsed(
            String expectedValue,
            int expectedPrecision,
            int expectedScale,
            String value,
            int precision,
            int scale,
            boolean strict,
            boolean lossy
    ) throws NumericException {
        Decimal256 decimal256 = new Decimal256();
        long meta = decimal256.ofString(value, 0, value.length(), precision, scale, strict, lossy);
        Assert.assertEquals(expectedValue, decimal256.toString());
        Assert.assertEquals(expectedPrecision, Numbers.decodeLowInt(meta));
        Assert.assertEquals(expectedScale, Numbers.decodeHighInt(meta));

        if (expectedPrecision <= Decimal64.MAX_PRECISION) {
            Decimal64 decimal64 = new Decimal64();
            long meta64 = DecimalParser.parse(decimal64, value, 0, value.length(), precision, scale, strict, lossy);
            Assert.assertEquals(expectedValue, decimal64.toString());
            Assert.assertEquals(expectedPrecision, Numbers.decodeLowInt(meta64));
            Assert.assertEquals(expectedScale, Numbers.decodeHighInt(meta64));
        }
    }
}
