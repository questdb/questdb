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
    public void testExponentBelowOneFitsFullScale() throws NumericException {
        // 1e-5 is 0.00001, five fraction digits and no integer digit
        assertParsed("0.00001", 5, 5, "1e-5", 5, 5);
        assertParsed("0.00001", 5, 5, "0.00001", 5, 5);
        assertParsed("0.00001", 5, 5, "1e-5", -1, -1);
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
    }

    private static void assertNotParsed(String expectedMessage, String value, int precision, int scale) {
        try {
            new Decimal256().ofString(value, 0, value.length(), precision, scale, true, false);
            Assert.fail("expected '" + value + "' to be rejected");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
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
