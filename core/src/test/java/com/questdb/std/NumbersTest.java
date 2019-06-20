/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NumbersTest {

    private final StringSink sink = new StringSink();

    private Rnd rnd;

    @Test(expected = NumericException.class)
    public void parseExplicitDouble2() throws Exception {
        Numbers.parseDouble("1234dx");
    }

    @Test
    public void parseExplicitLong() throws Exception {
        Assert.assertEquals(10000L, Numbers.parseLong("10000L"));
    }

    @Test(expected = NumericException.class)
    public void parseExplicitLong2() throws Exception {
        Numbers.parseLong("10000LL");
    }

    @Before
    public void setUp() {
        rnd = new Rnd();
        sink.clear();
    }

    @Test
    public void testCeilPow2() {
        Assert.assertEquals(16, Numbers.ceilPow2(15));
        Assert.assertEquals(16, Numbers.ceilPow2(16));
        Assert.assertEquals(32, Numbers.ceilPow2(17));
    }

    @Test(expected = NumericException.class)
    public void testEmptyDouble() throws Exception {
        Numbers.parseDouble("D");
    }

    @Test(expected = NumericException.class)
    public void testEmptyFloat() throws Exception {
        Numbers.parseFloat("f");
    }

    @Test(expected = NumericException.class)
    public void testEmptyLong() throws Exception {
        Numbers.parseLong("L");
    }

    @Test
    public void testFormatByte() {
        for (int i = 0; i < 1000; i++) {
            byte n = (byte) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Byte.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatChar() {
        for (int i = 0; i < 1000; i++) {
            char n = (char) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatDouble() {
        Numbers.append(sink, Double.POSITIVE_INFINITY, 3);
        Assert.assertEquals(Double.toString(Double.POSITIVE_INFINITY), sink.toString());

        sink.clear();
        Numbers.append(sink, Double.NEGATIVE_INFINITY, 3);
        Assert.assertEquals(Double.toString(Double.NEGATIVE_INFINITY), sink.toString());

        sink.clear();
        Numbers.append(sink, Double.NaN, 3);
        Assert.assertEquals(Double.toString(Double.NaN), sink.toString());

        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextPositiveInt() % 10;
            double d = rnd.nextDouble2() * Math.pow(10, n);
            sink.clear();
            Numbers.append(sink, d, 8);
            String actual = sink.toString();
            String expected = Double.toString(d);
            Assert.assertEquals(Double.parseDouble(expected), Double.parseDouble(actual), 0.000001);
        }
    }

    @Test
    public void testFormatDoubleNoPadding() {
        sink.clear();
        Numbers.appendTrim(sink, 40.2345d, 12);
        Assert.assertEquals("40.2345", sink.toString());

        sink.clear();
        Numbers.appendTrim(sink, 4000, 12);
        Assert.assertEquals("4000.0", sink.toString());
    }

    @Test
    public void testFormatFloat() {
        Numbers.append(sink, Float.POSITIVE_INFINITY, 3);
        Assert.assertEquals(Float.toString(Float.POSITIVE_INFINITY), sink.toString());

        sink.clear();
        Numbers.append(sink, Float.NEGATIVE_INFINITY, 3);
        Assert.assertEquals(Float.toString(Float.NEGATIVE_INFINITY), sink.toString());

        sink.clear();
        Numbers.append(sink, Float.NaN, 3);
        Assert.assertEquals(Float.toString(Float.NaN), sink.toString());

        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextPositiveInt() % 10;
            float f = rnd.nextFloat2() * (float) Math.pow(10, n);
            sink.clear();
            Numbers.append(sink, f, 8);
            String actual = sink.toString();
            String expected = Float.toString(f);
            Assert.assertEquals(Float.parseFloat(expected), Float.parseFloat(actual), 0.00001);
        }
    }

    @Test
    public void testFormatInt() {
        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextInt();
            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatLong() {
        for (int i = 0; i < 1000; i++) {
            long n = rnd.nextLong();
            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Long.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatShort() {
        for (int i = 0; i < 1000; i++) {
            short n = (short) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Short.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatSpecialDouble() {
        double d = -1.040218505859375E10d;
        Numbers.append(sink, d, 8);
        Assert.assertEquals(Double.toString(d), sink.toString());

        sink.clear();
        d = -1.040218505859375E-10d;
        Numbers.append(sink, d, 18);
        Assert.assertEquals(Double.toString(d), sink.toString());
    }

    @Test
    public void testHexInt() throws Exception {
        Assert.assertEquals('w', (char) Numbers.parseHexInt("77"));
        Assert.assertEquals(0xf0, Numbers.parseHexInt("F0"));
        Assert.assertEquals(0xac, Numbers.parseHexInt("ac"));
    }

    @Test
    public void testIntEdge() throws Exception {
        Numbers.append(sink, Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, Numbers.parseInt(sink));

        sink.clear();

        Numbers.append(sink, Integer.MIN_VALUE);
        Assert.assertEquals(Integer.MIN_VALUE, Numbers.parseIntQuiet(sink));
    }

    @Test
    public void testLong() throws Exception {
        Rnd rnd = new Rnd();
        StringSink sink = new StringSink();
        for (int i = 0; i < 100; i++) {
            long l1 = rnd.nextLong();
            long l2 = rnd.nextLong();
            sink.clear();

            Numbers.append(sink, l1);
            int p = sink.length();
            Numbers.append(sink, l2);
            Assert.assertEquals(l1, Numbers.parseLong(sink, 0, p));
            Assert.assertEquals(l2, Numbers.parseLong(sink, p, sink.length()));
        }
    }

    @Test
    public void testLongEdge() throws Exception {
        Numbers.append(sink, Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, Numbers.parseLong(sink));

        sink.clear();

        Numbers.append(sink, Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, Numbers.parseLongQuiet(sink));
    }

    @Test
    public void testLongToString() {
        Numbers.append(sink, 6103390276L);
        TestUtils.assertEquals("6103390276", sink);
    }

    @Test
    public void testParseDouble() throws Exception {

        String s9 = "0.33458980809808359835083490580348503845E203";
        Assert.assertEquals(Double.parseDouble(s9), Numbers.parseDouble(s9), 0.000000001);

        String s0 = "0.33458980809808359835083490580348503845";
        Assert.assertEquals(Double.parseDouble(s0), Numbers.parseDouble(s0), 0.000000001);


        String s1 = "0.45677888912387699";
        Assert.assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "1.459983E35";
        Assert.assertEquals(Double.parseDouble(s2) / 1e35d, Numbers.parseDouble(s2) / 1e35d, 0.00001);


        String s3 = "0.000000023E-30";
        Assert.assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "0.000000023E-204";
        Assert.assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);

        String s5 = "0.0000E-204";
        Assert.assertEquals(Double.parseDouble(s5), Numbers.parseDouble(s5), 0.000000001);

        String s6 = "200E2";
        Assert.assertEquals(Double.parseDouble(s6), Numbers.parseDouble(s6), 0.000000001);

        String s7 = "NaN";
        Assert.assertEquals(Double.parseDouble(s7), Numbers.parseDouble(s7), 0.000000001);

        String s8 = "-Infinity";
        Assert.assertEquals(Double.parseDouble(s8), Numbers.parseDouble(s8), 0.000000001);

    }

    @Test
    public void testParseExplicitDouble() throws Exception {
        Assert.assertEquals(1234.123d, Numbers.parseDouble("1234.123d"), 0.000001);
    }

    @Test
    public void testParseExplicitFloat() throws Exception {
        Assert.assertEquals(12345.02f, Numbers.parseFloat("12345.02f"), 0.0001f);
    }

    @Test(expected = NumericException.class)
    public void testParseExplicitFloat2() throws Exception {
        Numbers.parseFloat("12345.02fx");
    }

    @Test
    public void testParseFloat() throws Exception {
        String s1 = "0.45677899234";
        Assert.assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "1.459983E35";
        Assert.assertEquals(Float.parseFloat(s2) / 1e35d, Numbers.parseFloat(s2) / 1e35d, 0.00001);

        String s3 = "0.000000023E-30";
        Assert.assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        String s4 = "0.000000023E-38";
        Assert.assertEquals(Float.parseFloat(s4), Numbers.parseFloat(s4), 0.000000001);

        String s5 = "0.0000E-204";
        Assert.assertEquals(Float.parseFloat(s5), Numbers.parseFloat(s5), 0.000000001);

        String s6 = "200E2";
        Assert.assertEquals(Float.parseFloat(s6), Numbers.parseFloat(s6), 0.000000001);

        String s7 = "NaN";
        Assert.assertEquals(Float.parseFloat(s7), Numbers.parseFloat(s7), 0.000000001);

        String s8 = "-Infinity";
        Assert.assertEquals(Float.parseFloat(s8), Numbers.parseFloat(s8), 0.000000001);
    }

    @Test
    public void testParseInt() throws Exception {
        Assert.assertEquals(567963, Numbers.parseInt("567963"));
        Assert.assertEquals(-23346346, Numbers.parseInt("-23346346"));
    }

    @Test(expected = NumericException.class)
    public void testParseIntEmpty() throws Exception {
        Numbers.parseInt("");
    }

    @Test(expected = NumericException.class)
    public void testParseIntNull() throws Exception {
        Numbers.parseInt(null);
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow1() throws Exception {
        String i1 = "12345566787";
        Numbers.parseInt(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow2() throws Exception {
        Numbers.parseInt("2147483648");
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow3() throws Exception {
        Numbers.parseInt("5000000000");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSignOnly() throws Exception {
        Numbers.parseInt("-");
    }

    @Test
    public void testParseIntToDelim() throws Exception {
        String in = "1234x5";
        long val = Numbers.parseIntSafely(in, 0, in.length());
        Assert.assertEquals(1234, Numbers.decodeLowInt(val));
        Assert.assertEquals(4, Numbers.decodeHighInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParseIntToDelimEmpty() throws Exception {
        String in = "x";
        Numbers.parseIntSafely(in, 0, in.length());
    }

    @Test
    public void testParseIntToDelimNoChar() throws Exception {
        String in = "12345";
        long val = Numbers.parseIntSafely(in, 0, in.length());
        Assert.assertEquals(12345, Numbers.decodeLowInt(val));
        Assert.assertEquals(5, Numbers.decodeHighInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParseIntWrongChars() throws Exception {
        Numbers.parseInt("123ab");
    }

    @Test(expected = NumericException.class)
    public void testParseLongEmpty() throws Exception {
        Numbers.parseLong("");
    }

    @Test(expected = NumericException.class)
    public void testParseLongNull() throws Exception {
        Numbers.parseLong(null);
    }

    @Test(expected = NumericException.class)
    public void testParseLongNull2() throws Exception {
        Numbers.parseLong(null, 0, 10);
    }

    @Test(expected = NumericException.class)
    public void testParseLongOverflow1() throws Exception {
        String i1 = "1234556678723234234234234234234";
        Numbers.parseLong(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseLongOverflow2() throws Exception {
        Numbers.parseLong("9223372036854775808");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSignOnly() throws Exception {
        Numbers.parseLong("-");
    }

    @Test(expected = NumericException.class)
    public void testParseLongWrongChars() throws Exception {
        Numbers.parseLong("123ab");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeFail() throws Exception {
        Numbers.parseIntSize("5Kb");
    }

    @Test
    public void testParseIntSizeKb() throws Exception {
        Assert.assertEquals(5 * 1024, Numbers.parseIntSize("5K"));
        Assert.assertEquals(5 * 1024, Numbers.parseIntSize("5k"));
    }

    @Test
    public void testParseIntSizeMb() throws Exception {
        Assert.assertEquals(5 * 1024 * 1024, Numbers.parseIntSize("5M"));
        Assert.assertEquals(5 * 1024 * 1024, Numbers.parseIntSize("5m"));
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeOverflowAtK() throws Exception {
        Numbers.parseIntSize("4194304K");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeOverflowAtM() throws Exception {
        Numbers.parseIntSize("10240M");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeOverflowNoQualifier() throws Exception {
        Numbers.parseIntSize("10737418240");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeFail() throws Exception {
        Numbers.parseLongSize("5Kb");
    }

    @Test
    public void testParseLongSizeGb() throws Exception {
        Assert.assertEquals(7 * 1024 * 1024L * 1024L, Numbers.parseLongSize("7G"));
        Assert.assertEquals(7 * 1024 * 1024L * 1024L, Numbers.parseLongSize("7g"));
    }

    @Test
    public void testParseLongSizeKb() throws Exception {
        Assert.assertEquals(5 * 1024L, Numbers.parseLongSize("5K"));
        Assert.assertEquals(5 * 1024L, Numbers.parseLongSize("5k"));
    }

    @Test
    public void testParseLongSizeMb() throws Exception {
        Assert.assertEquals(5 * 1024 * 1024L, Numbers.parseLongSize("5M"));
        Assert.assertEquals(5 * 1024 * 1024L, Numbers.parseLongSize("5m"));
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowAtG() throws Exception {
        Numbers.parseLongSize("4503599627370496G");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowAtK() throws Exception {
        Numbers.parseLongSize("45035996273704960000K");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowAtM() throws Exception {
        Numbers.parseLongSize("450359962737049600M");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowNoQualifier() throws Exception {
        Numbers.parseLongSize("45035996273704960000000");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHexInt() throws Exception {
        Numbers.parseHexInt("0N");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongNan() throws Exception {
        Numbers.parseDouble("NaN1");
    }

    @Test
    public void testRoundDown() throws Exception {
        Rnd rnd = new Rnd();
        for (int i = 0; i < 1000; i++) {
            double d = rnd.nextDouble2();
            double n = Numbers.roundDown(d, 8);
            Assert.assertTrue(d + " " + n + " " + (d - n - 1E-8), d - n - 1E-8 < Numbers.TOLERANCE);
        }
    }

    @Test
    public void testRoundHalfDown() throws Exception {
        Assert.assertEquals(-1.235, Numbers.roundHalfDown(-1.2346, 3), Numbers.TOLERANCE);
        Assert.assertEquals(-1.23489, Numbers.roundHalfDown(-1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.23489, Numbers.roundHalfDown(1.234895, 5), Numbers.TOLERANCE);
    }

    @Test
    public void testRoundHalfEven() throws Exception {
        Assert.assertEquals(-1.235, Numbers.roundHalfEven(-1.2346, 3), Numbers.TOLERANCE);
        Assert.assertEquals(-1.2349, Numbers.roundHalfEven(-1.234899, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfEven(1.234899, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfEven(1.2348995, 6), Numbers.TOLERANCE);

        Assert.assertEquals(-1.2349, Numbers.roundHalfEven(-1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfEven(1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.0008, Numbers.roundHalfEven(1.00075, 4), Numbers.TOLERANCE);
        Assert.assertEquals(1.0008, Numbers.roundHalfEven(1.00085, 4), Numbers.TOLERANCE);
    }

    @Test
    public void testRoundHalfUp() throws Exception {
        Assert.assertEquals(-1.235, Numbers.roundHalfUp(-1.2346, 3), Numbers.TOLERANCE);
        Assert.assertEquals(-1.2349, Numbers.roundHalfUp(-1.234899, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfUp(1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.0009, Numbers.roundHalfUp(1.00091, 4), Numbers.TOLERANCE);
        Assert.assertEquals(-1.0009, Numbers.roundHalfUp(-1.00091, 4), Numbers.TOLERANCE);
    }

    @Test
    public void testRoundUp() throws Exception {
        Assert.assertEquals(-0.2345678098023, Numbers.roundUp(-0.234567809802242442424242423122388, 13), 1E-14);
        Assert.assertEquals(0.2345678098023, Numbers.roundUp(0.234567809802242442424242423122388, 13), 1E-14);

        Rnd rnd = new Rnd();
        for (int i = 0; i < 1000; i++) {
            double d = rnd.nextDouble2();
            double n = Numbers.roundUp(d, 8);
            Assert.assertTrue(d + " " + n + " " + (n - d - 1E-8), n - d - 1E-8 < Numbers.TOLERANCE);
        }
    }
}
