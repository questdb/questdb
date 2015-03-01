/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.io.sink.StringSink;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NumbersTest {

    private final StringSink sink = new StringSink();

    private Rnd rnd;

    @Before
    public void setUp() throws Exception {
        rnd = new Rnd();
        sink.clear();
    }

    @Test
    public void testFormatByte() throws Exception {
        for (int i = 0; i < 1000; i++) {
            byte n = (byte) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Byte.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatChar() throws Exception {
        for (int i = 0; i < 1000; i++) {
            char n = (char) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatDouble() throws Exception {
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
            double d = rnd.nextDouble() * Math.pow(10, n);
            sink.clear();
            Numbers.append(sink, d, 8);
            String actual = sink.toString();
            String expected = Double.toString(d);
            Assert.assertEquals(Double.parseDouble(expected), Double.parseDouble(actual), 0.000001);
        }
    }

    @Test
    public void testFormatDoubleNoPadding() throws Exception {
        sink.clear();
        Numbers.appendTrim(sink, 40.2345d, 12);
        Assert.assertEquals("40.2345", sink.toString());

        sink.clear();
        Numbers.appendTrim(sink, 4000, 12);
        Assert.assertEquals("4000.0", sink.toString());
    }

    @Test
    public void testFormatFloat() throws Exception {
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
            float f = rnd.nextFloat() * (float) Math.pow(10, n);
            sink.clear();
            Numbers.append(sink, f, 8);
            String actual = sink.toString();
            String expected = Float.toString(f);
            Assert.assertEquals(Float.parseFloat(expected), Float.parseFloat(actual), 0.00001);
        }
    }

    @Test
    public void testFormatInt() throws Exception {
        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextInt();
            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatLong() throws Exception {
        for (int i = 0; i < 1000; i++) {
            long n = rnd.nextLong();
            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Long.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatShort() throws Exception {
        for (int i = 0; i < 1000; i++) {
            short n = (short) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Short.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatSpecialDouble() throws Exception {
        double d = -1.040218505859375E10d;
        Numbers.append(sink, d, 8);
        Assert.assertEquals(Double.toString(d), sink.toString());

        sink.clear();
        d = -1.040218505859375E-10d;
        Numbers.append(sink, d, 18);
        Assert.assertEquals(Double.toString(d), sink.toString());
    }

    @Test
    public void testIntEdge() throws Exception {
        Numbers.append(sink, Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, Numbers.parseInt(sink));

        sink.clear();

        Numbers.append(sink, Integer.MIN_VALUE);
        Assert.assertEquals(Integer.MIN_VALUE, Numbers.parseInt(sink));
    }

    @Test
    public void testLongEdge() throws Exception {
        Numbers.append(sink, Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, Numbers.parseLong(sink));

        sink.clear();

        Numbers.append(sink, Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, Numbers.parseLong(sink));
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

    @Test(expected = NumberFormatException.class)
    public void testParseIntEmpty() throws Exception {
        Numbers.parseInt("");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseIntNull() throws Exception {
        Numbers.parseInt(null);
    }

    @Test(expected = NumberFormatException.class)
    public void testParseIntOverflow1() throws Exception {
        String i1 = "12345566787";
        Numbers.parseInt(i1);
    }

    @Test(expected = NumberFormatException.class)
    public void testParseIntOverflow2() throws Exception {
        Numbers.parseInt("2147483648");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseIntSignOnly() throws Exception {
        Numbers.parseInt("-");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseIntWrongChars() throws Exception {
        Numbers.parseInt("123ab");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongEmpty() throws Exception {
        Numbers.parseLong("");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongNull() throws Exception {
        Numbers.parseLong(null);
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongNull2() throws Exception {
        Numbers.parseLong(null, 0, 10);
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongOverflow1() throws Exception {
        String i1 = "1234556678723234234234234234234";
        Numbers.parseLong(i1);
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongOverflow2() throws Exception {
        Numbers.parseLong("9223372036854775808");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongSignOnly() throws Exception {
        Numbers.parseLong("-");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseLongWrongChars() throws Exception {
        Numbers.parseLong("123ab");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongNan() throws Exception {
        Numbers.parseDouble("NaN1");
    }

}
