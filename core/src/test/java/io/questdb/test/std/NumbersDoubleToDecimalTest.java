/*******************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class NumbersDoubleToDecimalTest {
    private final Decimal128 sink128 = new Decimal128();
    private final Decimal256 sink256 = new Decimal256();
    private final Decimal64 sink64 = new Decimal64();
    private final StringSink ss = new StringSink();

    @Test
    public void testAutoBasicNegativeValues() {
        assertAutoInference(-1.0, "-1", 1, 0);
        assertAutoInference(-0.5, "-0.5", 1, 1);
        assertAutoInference(-67.89, "-67.89", 4, 2);
        assertAutoInference(-100.0, "-100", 3, 0);
        assertAutoInference(-999.999, "-999.999", 6, 3);
        assertAutoInference(-0.001, "-0.001", 3, 3);
    }

    @Test
    public void testAutoBasicPositiveValues() {
        assertAutoInference(1.0, "1", 1, 0);
        assertAutoInference(2.0, "2", 1, 0);
        assertAutoInference(9.0, "9", 1, 0);
        assertAutoInference(10.0, "10", 2, 0);
        assertAutoInference(99.99, "99.99", 4, 2);
        assertAutoInference(100.0, "100", 3, 0);
        assertAutoInference(123.45, "123.45", 5, 2);
        assertAutoInference(999.0, "999", 3, 0);
    }

    @Test
    public void testAutoCommonDecimalFractions() {
        assertAutoInference(0.1, "0.1", 1, 1);
        assertAutoInference(0.2, "0.2", 1, 1);
        assertAutoInference(0.3, "0.3", 1, 1);
        assertAutoInference(0.7, "0.7", 1, 1);
        assertAutoInference(0.9, "0.9", 1, 1);
        assertAutoInference(0.01, "0.01", 2, 2);
        assertAutoInference(0.001, "0.001", 3, 3);
        assertAutoInference(0.0001, "0.0001", 4, 4);
    }

    @Test
    public void testAutoConsistentWithNumbersAppend() {
        double[] values = {
                0.1, 0.2, 0.3, 1.5, 123.456, -789.012,
                1e10, 1e-10, Math.PI, Math.E,
                42.0, 0.0001, 99_999.99999
        };
        StringSink appendSink = new StringSink();
        for (double v : values) {
            int type = Numbers.doubleToDecimal(v, sink64, sink128, sink256);
            if (type != 0) {
                String decStr = decimalToString(type);
                double fromDecimal = Double.parseDouble(decStr);

                appendSink.clear();
                Numbers.append(appendSink, v);
                double fromAppend = Double.parseDouble(appendSink.toString());

                Assert.assertEquals(
                        "decimal and append should represent same double for " + v,
                        fromAppend, fromDecimal, 0.0
                );
            }
        }
    }

    @Test
    public void testAutoDecimal128RangeOfExponents() {
        for (int exp = 19; exp <= 37; exp++) {
            double v = Math.pow(10, exp);
            int type = Numbers.doubleToDecimal(v, sink64, sink128, sink256);
            Assert.assertNotEquals("1e" + exp + " should fit", 0, type);
            int tag = ColumnType.tagOf(type);
            Assert.assertTrue(
                    "1e" + exp + " should need at least Decimal128, got tag=" + tag,
                    tag >= ColumnType.DECIMAL128
            );
        }
    }

    @Test
    public void testAutoDecimal128Required() {
        // 1e20 → 21 integer digits → precision > 18 → Decimal128
        int type = Numbers.doubleToDecimal(1e20, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(ColumnType.DECIMAL128, ColumnType.tagOf(type));
        assertAutoRoundTrip(1e20);
    }

    @Test
    public void testAutoDecimal256RangeOfExponents() {
        for (int exp = 39; exp <= 75; exp++) {
            double v = Math.pow(10, exp);
            int type = Numbers.doubleToDecimal(v, sink64, sink128, sink256);
            Assert.assertNotEquals("1e" + exp + " should fit", 0, type);
            Assert.assertEquals(
                    "1e" + exp + " should need Decimal256",
                    ColumnType.DECIMAL256,
                    ColumnType.tagOf(type)
            );
        }
    }

    @Test
    public void testAutoDecimal256Required() {
        int type = Numbers.doubleToDecimal(1e40, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(ColumnType.DECIMAL256, ColumnType.tagOf(type));
        assertAutoRoundTrip(1e40);
    }

    @Test
    public void testAutoDecimal64Selection() {
        // Single digit → Decimal64
        int type = Numbers.doubleToDecimal(5.0, sink64, sink128, sink256);
        Assert.assertTrue(ColumnType.tagOf(type) <= ColumnType.DECIMAL64);

        // 16 significant digits → still Decimal64
        type = Numbers.doubleToDecimal(1.234567890123456, sink64, sink128, sink256);
        Assert.assertTrue(ColumnType.tagOf(type) <= ColumnType.DECIMAL64);
        assertAutoRoundTrip(1.234567890123456);

        // Large integer with 16 digits → Decimal64
        type = Numbers.doubleToDecimal(1e15, sink64, sink128, sink256);
        Assert.assertTrue(ColumnType.tagOf(type) <= ColumnType.DECIMAL64);
    }

    @Test
    public void testAutoExactIntegerBoundary() {
        assertAutoInference(9_007_199_254_740_992.0, "9007199254740992", 16, 0);
        assertAutoInference(9_007_199_254_740_991.0, "9007199254740991", 16, 0);
    }

    @Test
    public void testAutoExceedsDecimal256DoubleMinMax() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.MIN_VALUE, sink64, sink128, sink256));
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.MIN_NORMAL, sink64, sink128, sink256));
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.MAX_VALUE, sink64, sink128, sink256));
        Assert.assertEquals(0, Numbers.doubleToDecimal(-Double.MAX_VALUE, sink64, sink128, sink256));
    }

    @Test
    public void testAutoExceedsDecimal256LargeExponent() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(1e77, sink64, sink128, sink256));
        Assert.assertEquals(0, Numbers.doubleToDecimal(1e100, sink64, sink128, sink256));
        Assert.assertEquals(0, Numbers.doubleToDecimal(1e308, sink64, sink128, sink256));
    }

    @Test
    public void testAutoExceedsDecimal256SmallExponent() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(1e-77, sink64, sink128, sink256));
        Assert.assertEquals(0, Numbers.doubleToDecimal(1e-200, sink64, sink128, sink256));
    }

    @Test
    public void testAutoLargeIntegers() {
        assertAutoInference(1000.0, "1000", 4, 0);
        assertAutoInference(10_000.0, "10000", 5, 0);
        assertAutoInference(100_000.0, "100000", 6, 0);
        assertAutoInference(1_000_000.0, "1000000", 7, 0);
        assertAutoInference(1e15, "1000000000000000", 16, 0);
        assertAutoRoundTrip(3_000_000_000.0);
        assertAutoRoundTrip(4_294_967_296.0); // 2^32
        assertAutoRoundTrip(1_000_000_000_000.0);
    }

    @Test
    public void testAutoMathE() {
        assertAutoRoundTrip(Math.E);
    }

    @Test
    public void testAutoMathPi() {
        assertAutoRoundTrip(Math.PI);
    }

    @Test
    public void testAutoMathSqrt2() {
        assertAutoRoundTrip(Math.sqrt(2));
    }

    @Test
    public void testAutoMaxDecimal256FitLargePositive() {
        int type = Numbers.doubleToDecimal(1e75, sink64, sink128, sink256);
        Assert.assertNotEquals("1e75 should fit in Decimal256", 0, type);
        assertAutoRoundTrip(1e75);
    }

    @Test
    public void testAutoMaxDecimal256FitSmallPositive() {
        int type = Numbers.doubleToDecimal(1e-76, sink64, sink128, sink256);
        Assert.assertNotEquals("1e-76 should fit in Decimal256", 0, type);
        assertAutoRoundTrip(1e-76);
    }

    @Test
    public void testAutoMixedValues() {
        assertAutoInference(1.5, "1.5", 2, 1);
        assertAutoInference(2.5, "2.5", 2, 1);
        assertAutoInference(10.1, "10.1", 3, 1);
        assertAutoInference(100.001, "100.001", 6, 3);
        assertAutoInference(1234.5678, "1234.5678", 8, 4);
    }

    @Test
    public void testAutoNaN() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.NaN, sink64, sink128, sink256));
    }

    @Test
    public void testAutoNearPowerOf10Boundaries() {
        assertAutoRoundTrip(0.9999999999999998);
        assertAutoRoundTrip(1.0000000000000002);
        assertAutoRoundTrip(9.999999999999998);
        assertAutoRoundTrip(10.000000000000002);
        assertAutoRoundTrip(99.99999999999999);
        assertAutoRoundTrip(100.00000000000001);
        assertAutoRoundTrip(999.9999999999999);
        assertAutoRoundTrip(1000.0000000000001);
    }

    @Test
    public void testAutoNegativeInfinity() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.NEGATIVE_INFINITY, sink64, sink128, sink256));
    }

    @Test
    public void testAutoNegativeLargeValues() {
        int type = Numbers.doubleToDecimal(-1e20, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        assertAutoRoundTrip(-1e20);

        type = Numbers.doubleToDecimal(-1e50, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        assertAutoRoundTrip(-1e50);
    }

    @Test
    public void testAutoNegativePowersOfTwo() {
        assertAutoInference(-0.5, "-0.5", 1, 1);
        assertAutoInference(-0.25, "-0.25", 2, 2);
        assertAutoInference(-2.0, "-2", 1, 0);
        assertAutoInference(-1024.0, "-1024", 4, 0);
    }

    @Test
    public void testAutoNegativeSmallValues() {
        assertAutoRoundTrip(-0.001);
        assertAutoRoundTrip(-1e-10);
        assertAutoRoundTrip(-1e-50);
    }

    @Test
    public void testAutoNegativeZero() {
        int type = Numbers.doubleToDecimal(-0.0, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(1, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals(0, ColumnType.getDecimalScale(type));
        assertDecimalString(type, "0");
    }

    @Test
    public void testAutoPositiveInfinity() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.POSITIVE_INFINITY, sink64, sink128, sink256));
    }

    @Test
    public void testAutoPowersOfTwoFractions() {
        assertAutoInference(0.5, "0.5", 1, 1);
        assertAutoInference(0.25, "0.25", 2, 2);
        assertAutoInference(0.125, "0.125", 3, 3);
        assertAutoInference(0.0625, "0.0625", 4, 4);
        assertAutoInference(0.03125, "0.03125", 5, 5);
    }

    @Test
    public void testAutoPowersOfTwoIntegers() {
        assertAutoInference(2.0, "2", 1, 0);
        assertAutoInference(4.0, "4", 1, 0);
        assertAutoInference(8.0, "8", 1, 0);
        assertAutoInference(16.0, "16", 2, 0);
        assertAutoInference(32.0, "32", 2, 0);
        assertAutoInference(64.0, "64", 2, 0);
        assertAutoInference(128.0, "128", 3, 0);
        assertAutoInference(256.0, "256", 3, 0);
        assertAutoInference(512.0, "512", 3, 0);
        assertAutoInference(1024.0, "1024", 4, 0);
        assertAutoInference(4096.0, "4096", 4, 0);
        assertAutoInference(65_536.0, "65536", 5, 0);
    }

    @Test
    public void testAutoRoundTripInterestingValues() {
        double[] values = {
                1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9,
                1.23, 12.3, 0.123,
                1.234567890123456,
                9.999999999999998,
                1.7976931348623157E10,
                2.2250738585072014E-10,
                -3.14159, -2.71828,
                0.333333333333333, 0.666666666666666,
                1.0 / 3.0, 2.0 / 3.0, 1.0 / 7.0, 1.0 / 11.0, 1.0 / 13.0,
                Math.sqrt(2), Math.sqrt(3), Math.sqrt(5),
                42.0, 137.036, 6.022e23,
        };
        for (double v : values) {
            assertAutoRoundTrip(v);
        }
    }

    @Test
    public void testAutoRoundTripPowersOfTen() {
        for (int exp = -75; exp <= 75; exp++) {
            double v = Math.pow(10, exp);
            int type = Numbers.doubleToDecimal(v, sink64, sink128, sink256);
            if (type != 0) {
                String decStr = decimalToString(type);
                double parsed = Double.parseDouble(decStr);
                Assert.assertEquals("round-trip for 1e" + exp, v, parsed, 0.0);
            }
        }
    }

    @Test
    public void testAutoRoundTripPowersOfTwo() {
        for (int exp = -50; exp <= 50; exp++) {
            assertAutoRoundTrip(Math.pow(2, exp));
        }
    }

    @Test
    public void testAutoRoundTripSmallIntegers() {
        for (int i = -100; i <= 100; i++) {
            assertAutoRoundTrip(i);
        }
    }

    @Test
    public void testAutoSmallExponentValues() {
        assertAutoRoundTrip(1e-20);
        assertAutoRoundTrip(1e-30);
        assertAutoRoundTrip(1e-50);
        assertAutoRoundTrip(1e-70);
        assertAutoRoundTrip(-1e-20);
        assertAutoRoundTrip(-1e-50);
    }

    @Test
    public void testAutoSmallFractionsDecimal128() {
        int type = Numbers.doubleToDecimal(1e-19, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        int tag = ColumnType.tagOf(type);
        Assert.assertTrue(tag >= ColumnType.DECIMAL128);
        assertAutoRoundTrip(1e-19);
    }

    @Test
    public void testAutoSmallFractionsDecimal256() {
        int type = Numbers.doubleToDecimal(1e-50, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(ColumnType.DECIMAL256, ColumnType.tagOf(type));
        assertAutoRoundTrip(1e-50);
    }

    @Test
    public void testAutoSmallIntegers1Through20() {
        for (int i = 1; i <= 20; i++) {
            assertAutoInference(i, String.valueOf(i), String.valueOf(i).length(), 0);
        }
    }

    @Test
    public void testAutoSmallNegativeIntegers() {
        for (int i = -20; i <= -1; i++) {
            String expected = String.valueOf(i);
            // Precision does not count the sign
            assertAutoInference(i, expected, String.valueOf(-i).length(), 0);
        }
    }

    @Test
    public void testAutoZero() {
        int type = Numbers.doubleToDecimal(0.0, sink64, sink128, sink256);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(1, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals(0, ColumnType.getDecimalScale(type));
        assertDecimalString(type, "0");
    }

    @Test
    public void testTargetBasicNegativeValues() throws NumericException {
        assertTargetCast64(-67.89, 5, 2, "-67.89");
        assertTargetCast64(-1.0, 3, 2, "-1.00");
        assertTargetCast64(-0.5, 3, 2, "-0.50");
        assertTargetCast64(-100.0, 5, 2, "-100.00");
        assertTargetCast64(-42.42, 4, 2, "-42.42");
    }

    @Test
    public void testTargetBasicPositiveValues() throws NumericException {
        assertTargetCast64(123.45, 5, 2, "123.45");
        assertTargetCast64(1.0, 3, 2, "1.00");
        assertTargetCast64(9.9, 3, 1, "9.9");
        assertTargetCast64(100.0, 5, 2, "100.00");
        assertTargetCast64(42.0, 4, 2, "42.00");
        assertTargetCast64(0.5, 3, 2, "0.50");
    }

    @Test
    public void testTargetBulkPositiveFractions() throws NumericException {
        Decimal64 d = new Decimal64();
        for (int i = 1; i <= 99; i++) {
            double v = i / 100.0;
            Numbers.doubleToDecimal(v, d, 18, 2, true);
            ss.clear();
            d.toSink(ss);
            double parsed = Double.parseDouble(ss.toString());
            Assert.assertEquals("round-trip for " + v, v, parsed, 0.0);
        }
    }

    @Test
    public void testTargetBulkRoundTrip() throws NumericException {
        Decimal64 d = new Decimal64();
        for (int i = -50; i <= 50; i++) {
            Numbers.doubleToDecimal(i, d, 18, 2, true);
            ss.clear();
            d.toSink(ss);
            double parsed = Double.parseDouble(ss.toString());
            Assert.assertEquals("round-trip for " + i, i, parsed, 0.0);
        }
    }

    @Test
    public void testTargetCommonFractions() throws NumericException {
        assertTargetCast64(0.1, 3, 2, "0.10");
        assertTargetCast64(0.2, 3, 2, "0.20");
        assertTargetCast64(0.3, 3, 2, "0.30");
    }

    @Test
    public void testTargetDecimal128Basic() throws NumericException {
        Decimal128 d = new Decimal128();
        Numbers.doubleToDecimal(123.45, d, 25, 2, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test
    public void testTargetDecimal128Negative() throws NumericException {
        Decimal128 d = new Decimal128();
        Numbers.doubleToDecimal(-789.012, d, 25, 4, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("-789.0120", ss.toString());
    }

    @Test
    public void testTargetDecimal128TrailingZeros() throws NumericException {
        Decimal128 d = new Decimal128();
        Numbers.doubleToDecimal(1.0, d, 25, 10, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("1.0000000000", ss.toString());
    }

    @Test
    public void testTargetDecimal128Zero() throws NumericException {
        Decimal128 d = new Decimal128();
        Numbers.doubleToDecimal(0.0, d, 25, 5, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("0.00000", ss.toString());
    }

    @Test
    public void testTargetDecimal256Basic() throws NumericException {
        Decimal256 d = new Decimal256();
        Numbers.doubleToDecimal(123.45, d, 55, 2, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test
    public void testTargetDecimal256LargeScale() throws NumericException {
        Decimal256 d = new Decimal256();
        Numbers.doubleToDecimal(42.0, d, 55, 20, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("42.00000000000000000000", ss.toString());
    }

    @Test
    public void testTargetDecimal256Negative() throws NumericException {
        Decimal256 d = new Decimal256();
        Numbers.doubleToDecimal(-999.999, d, 55, 5, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("-999.99900", ss.toString());
    }

    @Test
    public void testTargetDecimal256Zero() throws NumericException {
        Decimal256 d = new Decimal256();
        Numbers.doubleToDecimal(0.0, d, 55, 10, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("0.0000000000", ss.toString());
    }

    @Test
    public void testTargetLossyTruncateAllFractionalDigits() throws NumericException {
        Decimal64 d = new Decimal64();
        // 123.456 with target scale=0 → truncate to 123
        Numbers.doubleToDecimal(123.456, d, 3, 0, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123", ss.toString());
    }

    @Test
    public void testTargetLossyTruncateExcessExceedsPow10Table() throws NumericException {
        Decimal64 d = new Decimal64();
        // 1e-25 has naturalScale=25 (Ryu: output=1, e10=-1).
        // With scale=2, excess=naturalScale-scale can exceed the pow10 table size.
        // The entire significand is shifted out, so the result must be 0.
        Numbers.doubleToDecimal(1e-25, d, 18, 2, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("0.00", ss.toString());
    }

    @Test
    public void testTargetLossyTruncatePartialFraction() throws NumericException {
        Decimal64 d = new Decimal64();
        // 0.999 with target scale=1, lossy → truncate to 0.9
        Numbers.doubleToDecimal(0.999, d, 5, 1, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("0.9", ss.toString());
    }

    @Test
    public void testTargetLossyTruncateSmallFraction() throws NumericException {
        Decimal64 d = new Decimal64();
        // 0.999 with target scale=0, lossy → 0 (truncation, not rounding)
        Numbers.doubleToDecimal(0.999, d, 5, 0, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("0", ss.toString());
    }

    @Test
    public void testTargetLossyTruncateToZero() throws NumericException {
        Decimal64 d = new Decimal64();
        // 0.001 with target scale=0, lossy → all fractional digits removed → 0
        Numbers.doubleToDecimal(0.001, d, 5, 0, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("0", ss.toString());
    }

    @Test
    public void testTargetLossyTruncation() throws NumericException {
        Decimal64 d = new Decimal64();
        // 123.456 with target scale=2 → truncate to 123.45
        Numbers.doubleToDecimal(123.456, d, 5, 2, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test
    public void testTargetLossyTruncationNegative() throws NumericException {
        Decimal64 d = new Decimal64();
        // -123.456 with target scale=2 → truncate to -123.45
        Numbers.doubleToDecimal(-123.456, d, 5, 2, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("-123.45", ss.toString());
    }

    @Test(expected = NumericException.class)
    public void testTargetNaN() throws NumericException {
        Decimal64 d = new Decimal64();
        Numbers.doubleToDecimal(Double.NaN, d, 5, 2, true);
    }

    @Test(expected = NumericException.class)
    public void testTargetNegativeInfinity() throws NumericException {
        Decimal64 d = new Decimal64();
        Numbers.doubleToDecimal(Double.NEGATIVE_INFINITY, d, 5, 2, true);
    }

    @Test
    public void testTargetNegativeZero() throws NumericException {
        assertTargetCast64(-0.0, 5, 2, "0.00");
        assertTargetCast64(-0.0, 5, 0, "0");
    }

    @Test
    public void testTargetNonLossyExactFit() throws NumericException {
        Decimal64 d = new Decimal64();
        // 123.45 has naturalScale=2, target scale=2 → exact fit
        Numbers.doubleToDecimal(123.45, d, 5, 2, false);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test
    public void testTargetNonLossyIntegerValue() throws NumericException {
        Decimal64 d = new Decimal64();
        // 42.0 has naturalScale=0, target scale=3 → exact fit, trailing zeros
        Numbers.doubleToDecimal(42.0, d, 5, 3, false);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("42.000", ss.toString());
    }

    @Test(expected = NumericException.class)
    public void testTargetNonLossyScaleOverflow() throws NumericException {
        Decimal64 d = new Decimal64();
        // 0.001 has naturalScale=3, target scale=2, lossy=false → throws
        Numbers.doubleToDecimal(0.001, d, 4, 2, false);
    }

    @Test(expected = NumericException.class)
    public void testTargetNonLossyScaleOverflowManyDigits() throws NumericException {
        Decimal64 d = new Decimal64();
        // 123.456 has naturalScale=3, target scale=2, lossy=false → throws
        Numbers.doubleToDecimal(123.456, d, 5, 2, false);
    }

    @Test(expected = NumericException.class)
    public void testTargetPositiveInfinity() throws NumericException {
        Decimal64 d = new Decimal64();
        Numbers.doubleToDecimal(Double.POSITIVE_INFINITY, d, 5, 2, true);
    }

    @Test
    public void testTargetPowersOfTwo() throws NumericException {
        assertTargetCast64(0.5, 3, 2, "0.50");
        assertTargetCast64(0.25, 4, 3, "0.250");
        assertTargetCast64(0.125, 4, 3, "0.125");
        assertTargetCast64(2.0, 3, 1, "2.0");
        assertTargetCast64(4.0, 3, 1, "4.0");
        assertTargetCast64(8.0, 3, 1, "8.0");
    }

    @Test
    public void testTargetPrecisionExactBoundary() throws NumericException {
        Decimal64 d = new Decimal64();
        // 999.0 → integerDigits=3, requiredPrecision=3+2=5 == 5 → success
        Numbers.doubleToDecimal(999.0, d, 5, 2, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("999.00", ss.toString());
    }

    @Test(expected = NumericException.class)
    public void testTargetPrecisionOverflow() throws NumericException {
        Decimal64 d = new Decimal64();
        // 1000.0 → integerDigits=4, requiredPrecision=4+2=6 > 4 → throws
        Numbers.doubleToDecimal(1000.0, d, 4, 2, true);
    }

    @Test(expected = NumericException.class)
    public void testTargetPrecisionOverflowLargeValue() throws NumericException {
        Decimal64 d = new Decimal64();
        // 99999.0 → integerDigits=5, requiredPrecision=5+2=7 > 5 → throws
        Numbers.doubleToDecimal(99_999.0, d, 5, 2, true);
    }

    @Test(expected = NumericException.class)
    public void testTargetPrecisionOverflowNegative() throws NumericException {
        Decimal64 d = new Decimal64();
        // -10000.0 → integerDigits=5, requiredPrecision=5+2=7 > 5 → throws
        Numbers.doubleToDecimal(-10_000.0, d, 5, 2, true);
    }

    @Test
    public void testTargetScaleZero() throws NumericException {
        assertTargetCast64(42.0, 5, 0, "42");
        assertTargetCast64(100.0, 5, 0, "100");
        assertTargetCast64(1.0, 5, 0, "1");
        assertTargetCast64(0.0, 5, 0, "0");
    }

    @Test
    public void testTargetScaleZeroLossy() throws NumericException {
        Decimal64 d = new Decimal64();
        // 42.7 → scale=0, lossy → truncate fractional → 42
        Numbers.doubleToDecimal(42.7, d, 5, 0, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("42", ss.toString());
    }

    @Test(expected = NumericException.class)
    public void testTargetScaleZeroNonLossyFraction() throws NumericException {
        Decimal64 d = new Decimal64();
        // 42.7 → scale=0, non-lossy, has fractional part → throws
        Numbers.doubleToDecimal(42.7, d, 5, 0, false);
    }

    @Test
    public void testTargetTrailingZeros() throws NumericException {
        assertTargetCast64(123.45, 8, 5, "123.45000");
        assertTargetCast64(1.0, 10, 8, "1.00000000");
        assertTargetCast64(42.0, 8, 5, "42.00000");
        assertTargetCast64(0.5, 5, 4, "0.5000");
    }

    @Test
    public void testTargetZero() throws NumericException {
        assertTargetCast64(0.0, 5, 2, "0.00");
        assertTargetCast64(0.0, 5, 0, "0");
        assertTargetCast64(0.0, 10, 5, "0.00000");
    }

    private void assertAutoInference(double value, String expected, int expectedPrecision, int expectedScale) {
        int type = Numbers.doubleToDecimal(value, sink64, sink128, sink256);
        Assert.assertNotEquals("should succeed for " + value, 0, type);
        Assert.assertEquals("precision for " + value, expectedPrecision, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals("scale for " + value, expectedScale, ColumnType.getDecimalScale(type));
        assertDecimalString(type, expected);
    }

    private void assertAutoRoundTrip(double value) {
        int type = Numbers.doubleToDecimal(value, sink64, sink128, sink256);
        if (type == 0) {
            // Value doesn't fit — not a failure for round-trip, just skip
            return;
        }
        String decStr = decimalToString(type);
        double parsed = Double.parseDouble(decStr);
        Assert.assertEquals("round-trip for " + value, value, parsed, 0.0);
    }

    private void assertDecimalString(int type, String expected) {
        ss.clear();
        int tag = ColumnType.tagOf(type);
        if (tag >= ColumnType.DECIMAL8 && tag <= ColumnType.DECIMAL64) {
            sink64.toSink(ss);
        } else if (tag == ColumnType.DECIMAL128) {
            sink128.toSink(ss);
        } else {
            sink256.toSink(ss);
        }
        Assert.assertEquals(expected, ss.toString());
    }

    private void assertTargetCast64(double value, int precision, int scale, String expected) throws NumericException {
        Decimal64 d = new Decimal64();
        Numbers.doubleToDecimal(value, d, precision, scale, true);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals(expected, ss.toString());
    }

    private String decimalToString(int type) {
        ss.clear();
        int tag = ColumnType.tagOf(type);
        if (tag >= ColumnType.DECIMAL8 && tag <= ColumnType.DECIMAL64) {
            sink64.toSink(ss);
        } else if (tag == ColumnType.DECIMAL128) {
            sink128.toSink(ss);
        } else {
            sink256.toSink(ss);
        }
        return ss.toString();
    }
}
