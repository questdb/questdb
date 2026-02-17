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

package io.questdb.test.std.fastdouble;

import io.questdb.std.fastdouble.FastDoubleSwar;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.fail;

public class ParseSignificandWithSwarTest {
    /**
     * Matches a decimal point at any of the eight possible characters.
     * The mask is eight times the '.' character 0x2e: "........";
     */
    private final static long DECIMAL_POINT_MASK = 0x2e_2e_2e_2e_2e_2e_2e_2eL;
    private final static double[] POWER_OF_TEN = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5,
            1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
            1e12, 1e13, 1e14, 1e15, 1e16, 1e17,
            1e18, 1e19, 1e20, 1e21, 1e22};

    public void doIllegalTest(String s) {
        try {
            significandToDouble(s.getBytes(StandardCharsets.ISO_8859_1));
            fail();
        } catch (NumberFormatException e) {
            //success
        }
    }

    public void doLegalTest(String s) {
        double actual = significandToDouble(s.getBytes(StandardCharsets.ISO_8859_1));
        double expected = Double.parseDouble(s);
        System.out.println(expected + " == " + actual);
        Assert.assertEquals(expected, actual, 0.001);
    }

    @Test
    public void dynamicTestsIllegalInput() {
        String str = "12345678";
        for (int i = 0; i < 7; i++) {
            byte[] bytes = str.getBytes(StandardCharsets.ISO_8859_1);
            bytes[i] = '.';
            bytes[i + 1] = '.';
            String s = new String(bytes, StandardCharsets.ISO_8859_1);
            doIllegalTest(s);
        }
    }

    @Test
    public void dynamicTestsLegalInput() {
        String str = "12345678";
        for (int i = -1; i < 8; i++) {
            byte[] bytes = str.getBytes(StandardCharsets.ISO_8859_1);
            if (i >= 0) {
                bytes[i] = '.';
            }
            String s = new String(bytes, StandardCharsets.ISO_8859_1);
            doLegalTest(s);
        }
    }

    /**
     * Parses a {@code DecSignificand} that is exactly 8 characters long:
     * <blockquote>
     * <dl>
     * <dt><i>DecSignificand:</i>
     * <dd><i>Digits {@code .} [Digits]</i>
     * <dd><i>{@code .} Digits</i>
     * <dd><i>Digits</i>
     * </dl>
     * </blockquote>
     * <p>
     * References:
     * <dl>
     *     <dt>Eichenherz,  Attempt at creating a fast float parser #22 </dt>
     *     <dd><a href="https://github.com/lemire/fast_double_parser/issues/22">github/fast_double_parser</a></dd>
     *     <dd><a href="https://gist.github.com/Eichenherz/657b1d794325310f8eafa5af6375f673">gist.github</a></dd>
     * </dl>
     *
     * @param str a string
     * @return the parsed value
     */
    double significandToDouble(byte[] str) {
        // Byte masking : https://richardstartin.github.io/posts/finding-bytes
        // std::memcpy to avoid strict-aliasing violations
        // as mentioned here  https://kholdstare.github.io/technical/2020/05/26/faster-integer-parsing.html


        final long asciiFloat = FastDoubleSwar.getChunk(str, 0);

        // Create a mask, that contains 0x80 at every '.' character and 0x00 everywhere else.
        long masked = asciiFloat ^ DECIMAL_POINT_MASK;
        long tmp = (masked & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        masked = ~(tmp | masked | 0x7F7F7F7F7F7F7F7FL);

        int lzCount = Long.numberOfLeadingZeros(masked);
        int tzCount = Long.numberOfTrailingZeros(masked) - 7;


        int exp = (lzCount / 8) & 7;
        long integer;

        if (exp != 0) {
            // We have a decimal point somewhere before the last digit:
            // -> keep fraction in place; move int part to the right; fill in '0' character.
            long fraction = asciiFloat & (-1L << (tzCount + 8));
            integer = exp == 7 ? 0 : (asciiFloat << (exp + 1) * 8) >>> exp * 8;
            integer = 0x30L | fraction | integer;
        } else if (tzCount == 56) {
            // We have a decimal point right at the end:
            // -> replace '.' character with '0' character.
            integer = 0x1e000000_00000000L ^ asciiFloat;
            exp = 1;
        } else {
            integer = asciiFloat;
        }

        // Check if integer only contains the ascii characters from '0' to '9.
        long val = integer - 0x3030303030303030L;
        long det = ((integer + 0x4646464646464646L) | val) &
                0x8080808080808080L;
        if (det != 0L) {
            throw new NumberFormatException();
        }

        // From ascii to num.
        // The last 2 multiplications are independent of each other.
        val = (val * 0xa_01L) >>> 8;// 1+(10<<8)
        val = (((val & 0x000000FF_000000FFL) * 0x000F4240_00000064L)//100 + (1000000 << 32)
                + (((val >>> 16) & 0x000000FF_000000FFL) * 0x00002710_00000001L)) >>> 32;// 1 + (10000 << 32)
        integer = (int) val;


        // FAST PATH uint64 to float64
        // Inspired by https://github.com/lemire/fast_double_parser/
        // Only use fast path because our floats have 7-8 bytes

        // If 0 <= s < 2^53 and if 10^0 <= p <= 10^22 then
        // 1) Both s and p can be represented exactly as 64-bit floats
        // 2) Because s and p can be represented exactly as floats,
        // then s * p and s / p will produce correctly rounded values
        return (double) (integer) / POWER_OF_TEN[exp];
    }
}
