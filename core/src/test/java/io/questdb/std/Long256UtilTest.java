/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class Long256UtilTest {
    private static final BigInteger BI_MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final long LONG_ALL_BITS_SET = -1;
    private final StringSink ss = new StringSink();

    @Test
    public void testDecodeDec_randomized() {
        Long256Impl l = new Long256Impl();
        Random rand = new Random();

        for (int i = 0; i < 10_000; i++) {
            BigInteger bigInteger = new BigInteger(256, rand);
            String decStr = bigInteger.toString();
            Long256Util.decodeDec(decStr, 0, decStr.length(), l);

            assertLongEquals("", bigInteger, l);
        }
    }

    @Test
    public void testDivideByInt_randomized() {
        Rnd rnd = new Rnd();
        Long256Impl l = new Long256Impl();

        for (int i = 0; i < 10_000; i++) {
            l.setAll(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            BigInteger b = toBigInteger(l);

            int divisor = rnd.nextPositiveInt();
            int actualRemainder = Long256Util.divideByInt(l, divisor);
            BigInteger[] res = b.divideAndRemainder(BigInteger.valueOf(divisor));
            b = trimTo256bits(res[0]);
            long expectedRemainder = res[1].longValue();
            assertEquals(expectedRemainder, actualRemainder);

            assertLongEquals("error when dividing by " + divisor, b, l);
        }
    }

    @Test
    public void testLShift_by0() {
        Long256Impl l0 = new Long256Impl();
        Long256Impl l1 = new Long256Impl();
        String hexString = "ff";
        Long256FromCharSequenceDecoder.decodeHex(hexString, 0, hexString.length(), l0);
        l1.copyFrom(l0);

        Long256Util.leftShift(l0, 0);
        assertEquals(l1, l0);
    }

    @Test
    public void testLeftShift_randomized() {
        Rnd rnd = new Rnd();
        Long256Impl l = new Long256Impl();

        for (int i = 0; i < 100000; i++) {
            l.setAll(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            ss.clear();
            Numbers.appendLong256Hex(l.getLong0(), l.getLong1(), l.getLong2(), l.getLong3(), ss);
            BigInteger b = toBigInteger(l);

            int bits = rnd.nextPositiveInt() % 300;
            Long256Util.leftShift(l, bits);
            b = b.shiftLeft(bits);
            b = trimTo256bits(b);

            assertLongEquals("error when rotating by " + bits + " bits ", b, l);
        }
    }

    @Test
    public void testMultipleBy10_randomized() {
        Rnd rnd = new Rnd();
        Long256Impl l = new Long256Impl();

        for (int i = 0; i < 100_000; i++) {
            l.setAll(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            ss.clear();
            Numbers.appendLong256Hex(l.getLong0(), l.getLong1(), l.getLong2(), l.getLong3(), ss);
            BigInteger b = toBigInteger(l);

            Long256Util.multipleBy10(l);
            b = b.multiply(BigInteger.TEN);
            b = trimTo256bits(b);

            assertLongEquals("error when multiplying by 10 ", b, l);
        }
    }

    @Test
    public void testMultipleByInt_edgeCases() {
        Long256Impl l = new Long256Impl();

        l.setAll(LONG_ALL_BITS_SET, LONG_ALL_BITS_SET, LONG_ALL_BITS_SET, LONG_ALL_BITS_SET);
        BigInteger bi = toBigInteger(l);

        Long256Util.multipleByInt(l, Integer.MAX_VALUE);
        BigInteger expected = trimTo256bits(bi.multiply(BI_MAX_INT));

        assertLongEquals("", expected, l);
    }

    @Test
    public void testMultipleByInt_randomized() {
        Long256Impl l = new Long256Impl();
        Random rand = new Random();

        for (int i = 0; i < 10_000; i++) {
            BigInteger bigInteger = new BigInteger(256, rand);
            String decStr = bigInteger.toString();
            Long256Util.decodeDec(decStr, 0, decStr.length(), l);

            int multiplier = rand.nextInt(Integer.MAX_VALUE);
            Long256Util.multipleByInt(l, multiplier);
            BigInteger expected = trimTo256bits(bigInteger.multiply(BigInteger.valueOf(multiplier)));

            assertLongEquals("", expected, l);
        }
    }

    @NotNull
    private static BigInteger trimTo256bits(BigInteger b) {
        for (int i = 256; i < b.bitLength(); i++) {
            b = b.clearBit(i);
        }
        return b;
    }

    private void assertLongEquals(String s, BigInteger b0, Long256Impl l0) {
        ss.clear();
        Numbers.appendLong256Hex(l0.getLong0(), l0.getLong1(), l0.getLong2(), l0.getLong3(), ss);
        String hex = ss.toString().substring(2);
        while (hex.startsWith("0") && hex.length() > 1) {
            hex = hex.substring(1);
        }

        assertEquals(s, b0.toString(16), hex);
    }

    private BigInteger toBigInteger(Long256Impl l) {
        ss.clear();
        Numbers.appendLong256Hex(l.getLong0(), l.getLong1(), l.getLong2(), l.getLong3(), ss);
        return new BigInteger(ss.toString().substring(2), 16);
    }

}
