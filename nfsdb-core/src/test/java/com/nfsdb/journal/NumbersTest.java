/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.journal;

import com.nfsdb.journal.test.tools.TestCharSink;
import com.nfsdb.journal.utils.Numbers;
import com.nfsdb.journal.utils.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NumbersTest {

    private final TestCharSink sink = new TestCharSink();
    
    private Rnd rnd;

    @Before
    public void setUp() throws Exception {
        rnd = new Rnd();
        sink.clear();
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
    public void testFormatShort() throws Exception {
        for (int i = 0; i < 1000; i++) {
            short n = (short) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Short.toString(n), sink.toString());
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
    public void testParseInt() throws Exception {
        Assert.assertEquals(567963, Numbers.parseInt("567963"));
        Assert.assertEquals(-23346346, Numbers.parseInt("-23346346"));
    }
}
