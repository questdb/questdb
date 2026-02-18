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
    private final char[] digits = new char[21];
    private final Decimal128 sink128 = new Decimal128();
    private final Decimal256 sink256 = new Decimal256();
    private final Decimal64 sink64 = new Decimal64();
    private final StringSink ss = new StringSink();

    @Test
    public void testAutoInferenceBasicValues() {
        assertAutoInference(123.45, "123.45", 5, 2);
        assertAutoInference(-67.89, "-67.89", 4, 2);
        assertAutoInference(1.0, "1", 1, 0);
        assertAutoInference(0.1, "0.1", 1, 1);
        assertAutoInference(0.5, "0.5", 1, 1);
        assertAutoInference(99.99, "99.99", 4, 2);
    }

    @Test
    public void testAutoInferenceInfinity() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.POSITIVE_INFINITY, sink64, sink128, sink256, digits));
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.NEGATIVE_INFINITY, sink64, sink128, sink256, digits));
    }

    @Test
    public void testAutoInferenceIntegerDoubles() {
        assertAutoInference(1000.0, "1000", 4, 0);
        assertAutoInference(1e15, "1000000000000000", 16, 0);
    }

    @Test
    public void testAutoInferenceLargeValues() {
        int type = Numbers.doubleToDecimal(1e20, sink64, sink128, sink256, digits);
        Assert.assertNotEquals(0, type);
        int type2 = Numbers.doubleToDecimal(1e50, sink64, sink128, sink256, digits);
        Assert.assertNotEquals(0, type2);
    }

    @Test
    public void testAutoInferenceNaN() {
        Assert.assertEquals(0, Numbers.doubleToDecimal(Double.NaN, sink64, sink128, sink256, digits));
    }

    @Test
    public void testAutoInferenceNegativeZero() {
        int type = Numbers.doubleToDecimal(-0.0, sink64, sink128, sink256, digits);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(1, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals(0, ColumnType.getDecimalScale(type));
        ss.clear();
        sink64.toSink(ss);
        Assert.assertEquals("0", ss.toString());
    }

    @Test
    public void testAutoInferenceSmallFractions() {
        assertAutoInference(0.001, "0.001", 3, 3);
    }

    @Test
    public void testAutoInferenceVeryLargeDoesNotFit() {
        // 1e308 has precision > 76
        Assert.assertEquals(0, Numbers.doubleToDecimal(1e308, sink64, sink128, sink256, digits));
    }

    @Test
    public void testAutoInferenceVerySmallDoesNotFit() {
        // 5e-324 needs scale > 76
        Assert.assertEquals(0, Numbers.doubleToDecimal(5e-324, sink64, sink128, sink256, digits));
    }

    @Test
    public void testAutoInferenceZero() {
        int type = Numbers.doubleToDecimal(0.0, sink64, sink128, sink256, digits);
        Assert.assertNotEquals(0, type);
        Assert.assertEquals(1, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals(0, ColumnType.getDecimalScale(type));
        ss.clear();
        sink64.toSink(ss);
        Assert.assertEquals("0", ss.toString());
    }

    @Test
    public void testTargetCastBasicValues() throws NumericException {
        assertTargetCast(123.45, 5, 2, "123.45");
        assertTargetCast(-67.89, 5, 2, "-67.89");
        assertTargetCast(0.0, 5, 2, "0.00");
        assertTargetCast(100.0, 5, 2, "100.00");
    }

    @Test
    public void testTargetCastLargeScale() throws NumericException {
        // 123.45 stored with scale=5 should have trailing zeros
        assertTargetCast(123.45, 8, 5, "123.45000");
    }

    @Test
    public void testTargetCastLossy() throws NumericException {
        // 123.456 with target scale=2 and lossy=true should truncate
        Decimal64 d = new Decimal64();
        Numbers.doubleToDecimal(123.456, d, 5, 2, true, digits);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test(expected = NumericException.class)
    public void testTargetCastPrecisionOverflow() throws NumericException {
        Decimal64 d = new Decimal64();
        // 1000.0 requires precision 6 for scale=2 but only 4 allowed
        Numbers.doubleToDecimal(1000.0, d, 4, 2, true, digits);
    }

    @Test(expected = NumericException.class)
    public void testTargetCastScaleOverflow() throws NumericException {
        Decimal64 d = new Decimal64();
        // 0.001 has 3 decimal places but scale limited to 2, not lossy
        Numbers.doubleToDecimal(0.001, d, 4, 2, false, digits);
    }

    @Test
    public void testTargetCastWithDecimal128() throws NumericException {
        Decimal128 d = new Decimal128();
        Numbers.doubleToDecimal(123.45, d, 25, 2, true, digits);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test
    public void testTargetCastWithDecimal256() throws NumericException {
        Decimal256 d = new Decimal256();
        Numbers.doubleToDecimal(123.45, d, 55, 2, true, digits);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals("123.45", ss.toString());
    }

    @Test
    public void testTargetCastZero() throws NumericException {
        assertTargetCast(0.0, 5, 2, "0.00");
        assertTargetCast(-0.0, 5, 2, "0.00");
    }

    private void assertAutoInference(double value, String expected, int expectedPrecision, int expectedScale) {
        int type = Numbers.doubleToDecimal(value, sink64, sink128, sink256, digits);
        Assert.assertNotEquals("should succeed for " + value, 0, type);
        Assert.assertEquals("precision for " + value, expectedPrecision, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals("scale for " + value, expectedScale, ColumnType.getDecimalScale(type));
        // Verify the string representation
        ss.clear();
        int tag = ColumnType.tagOf(type);
        if (tag >= ColumnType.DECIMAL8 && tag <= ColumnType.DECIMAL64) {
            sink64.toSink(ss);
        } else if (tag == ColumnType.DECIMAL128) {
            sink128.toSink(ss);
        } else {
            sink256.toSink(ss);
        }
        Assert.assertEquals("value for " + value, expected, ss.toString());
    }

    private void assertTargetCast(double value, int precision, int scale, String expected) throws NumericException {
        Decimal64 d = new Decimal64();
        Numbers.doubleToDecimal(value, d, precision, scale, true, digits);
        ss.clear();
        d.toSink(ss);
        Assert.assertEquals(expected, ss.toString());
    }
}
