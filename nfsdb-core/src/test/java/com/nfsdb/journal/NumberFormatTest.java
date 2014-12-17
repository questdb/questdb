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

package com.nfsdb.journal;

import com.nfsdb.journal.utils.NumberFormat;
import com.nfsdb.journal.utils.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NumberFormatTest {

    private final StringBuilder b = new StringBuilder();
    private Rnd rnd;

    @Before
    public void setUp() throws Exception {
        rnd = new Rnd();
        b.setLength(0);
    }

    @Test
    public void testFormatSpecialDouble() throws Exception {
        double d = -1.040218505859375E10d;
        NumberFormat.append(b, d, 8);
        Assert.assertEquals(Double.toString(d), b.toString());

        b.setLength(0);
        d = -1.040218505859375E-10d;
        NumberFormat.append(b, d, 18);
        Assert.assertEquals(Double.toString(d), b.toString());
    }

    @Test
    public void testFormatDouble() throws Exception {
        NumberFormat.append(b, Double.POSITIVE_INFINITY, 3);
        Assert.assertEquals(Double.toString(Double.POSITIVE_INFINITY), b.toString());

        b.setLength(0);
        NumberFormat.append(b, Double.NEGATIVE_INFINITY, 3);
        Assert.assertEquals(Double.toString(Double.NEGATIVE_INFINITY), b.toString());

        b.setLength(0);
        NumberFormat.append(b, Double.NaN, 3);
        Assert.assertEquals(Double.toString(Double.NaN), b.toString());

        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextPositiveInt() % 10;
            double d = rnd.nextDouble() * Math.pow(10, n);
            b.setLength(0);
            NumberFormat.append(b, d, 8);
            String actual = b.toString();
            String expected = Double.toString(d);
            Assert.assertEquals(Double.parseDouble(expected), Double.parseDouble(actual), 0.000001);
        }
    }

    @Test
    public void testFormatFloat() throws Exception {
        NumberFormat.append(b, Float.POSITIVE_INFINITY, 3);
        Assert.assertEquals(Float.toString(Float.POSITIVE_INFINITY), b.toString());

        b.setLength(0);
        NumberFormat.append(b, Float.NEGATIVE_INFINITY, 3);
        Assert.assertEquals(Float.toString(Float.NEGATIVE_INFINITY), b.toString());

        b.setLength(0);
        NumberFormat.append(b, Float.NaN, 3);
        Assert.assertEquals(Float.toString(Float.NaN), b.toString());

        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextPositiveInt() % 10;
            float f = rnd.nextFloat() * (float) Math.pow(10, n);
            b.setLength(0);
            NumberFormat.append(b, f, 8);
            String actual = b.toString();
            String expected = Float.toString(f);
            Assert.assertEquals(Float.parseFloat(expected), Float.parseFloat(actual), 0.00001);
        }
    }

    @Test
    public void testFormatInt() throws Exception {
        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextInt();
            b.setLength(0);
            NumberFormat.append(b, n);
            Assert.assertEquals(Integer.toString(n), b.toString());
        }
    }

    @Test
    public void testFormatByte() throws Exception {
        for (int i = 0; i < 1000; i++) {
            byte n = (byte) rnd.nextInt();

            b.setLength(0);
            NumberFormat.append(b, n);
            Assert.assertEquals(Byte.toString(n), b.toString());
        }
    }

    @Test
    public void testFormatChar() throws Exception {
        for (int i = 0; i < 1000; i++) {
            char n = (char) rnd.nextInt();

            b.setLength(0);
            NumberFormat.append(b, n);
            Assert.assertEquals(Integer.toString(n), b.toString());
        }
    }

    @Test
    public void testFormatShort() throws Exception {
        for (int i = 0; i < 1000; i++) {
            short n = (short) rnd.nextInt();

            b.setLength(0);
            NumberFormat.append(b, n);
            Assert.assertEquals(Short.toString(n), b.toString());
        }
    }

    @Test
    public void testFormatLong() throws Exception {
        for (int i = 0; i < 1000; i++) {
            long n = rnd.nextLong();
            b.setLength(0);
            NumberFormat.append(b, n);
            Assert.assertEquals(Long.toString(n), b.toString());
        }
    }

}
