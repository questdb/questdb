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
import io.questdb.std.DecimalKnuthDivider;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Random;

public class DecimalKnuthDividerTest {
    private static final RoundingMode[] ROUNDING_MODES = {
            RoundingMode.UP, RoundingMode.DOWN, RoundingMode.CEILING, RoundingMode.FLOOR,
            RoundingMode.HALF_UP, RoundingMode.HALF_DOWN, RoundingMode.HALF_EVEN
    };
    private static final long SEED = 0x9E3779B97F4A7C15L;
    private final DecimalKnuthDivider divider = new DecimalKnuthDivider();
    private final Decimal256 sink = new Decimal256();

    @Test
    public void testAddBackUsesUnsignedLimbs() {
        // 0xE8E4FC9F3D6A981D79B3D502FFFFFC5C / 0xE8E4FC9F3D6A981D79B3D503: the add-back of step D6
        // walks limbs whose top bit is set.
        final BigInteger dividend = new BigInteger("309569862489351833281376464545576385628");
        final BigInteger divisor = new BigInteger("72077350339235699102603007235");
        assertNarrow(dividend, divisor, RoundingMode.DOWN, "4294967295");
        assertNarrow(dividend, divisor, RoundingMode.HALF_UP, "4294967296");
        assertWide(dividend, divisor, RoundingMode.DOWN, "4294967295");
        assertWide(dividend, divisor, RoundingMode.HALF_UP, "4294967296");
    }

    @Test
    public void testExactDivisionIsNotRoundedAway() {
        // Six limbs over five. Neither the D3 clamp nor the D6 carry alone rounds this one away,
        // both have to be missing, which is how the divider stood before this change.
        final BigInteger dividend = new BigInteger("815926766585445776450849652337708598160020065799465637765");
        final BigInteger divisor = new BigInteger("189972754282741465310936588246525541508231686267");
        assertWide(dividend, divisor, RoundingMode.DOWN, "4294967295");
        assertWide(dividend, divisor, RoundingMode.UP, "4294967295");
        assertWide(dividend, divisor, RoundingMode.HALF_UP, "4294967295");
        assertWide(dividend, divisor, RoundingMode.CEILING, "4294967295");
    }

    @Test
    public void testExactDivisionWithAddBackBelowTheTopDigit() {
        // 0x7FFFFFFFFFFFFFFD80000002000000007FFFFFFF / 0x800000007FFFFFFE80000001, an exact division
        // whose add-back runs below the topmost quotient digit, so its carry lands in a limb the
        // remainder scan still reads. Without the carry that limb keeps 0xFFFFFFFF and reads back as
        // a non-zero remainder, rounding the exact quotient away from zero.
        final BigInteger dividend = new BigInteger("730750818665451458903772010109374153993672982527");
        final BigInteger divisor = new BigInteger("39614081266355540827184300033");
        final String quotient = "18446744069414584319";
        assertWide(dividend, divisor, RoundingMode.DOWN, quotient);
        assertWide(dividend, divisor, RoundingMode.UP, quotient);
        assertWide(dividend, divisor, RoundingMode.CEILING, quotient);
        // a phantom remainder makes UNNECESSARY throw on a division that has none
        assertWide(dividend, divisor, RoundingMode.UNNECESSARY, quotient);
        // FLOOR rounds a negative exact result away from zero the same way
        Assert.assertEquals(
                new BigInteger(quotient),
                divide(dividend, divisor, true, RoundingMode.FLOOR, true)
        );
    }

    @Test
    public void testQuotientDigitClampedToBase() {
        // 0x80000000FFFFFFFE00000000 / 0x80000000FFFFFFFF: the top limb of the partial remainder
        // equals the top limb of the divisor, so the estimated digit is 2^32 + 1.
        final BigInteger dividend = new BigInteger("39614081275578912861891592192");
        final BigInteger divisor = new BigInteger("9223372041149743103");
        assertNarrow(dividend, divisor, RoundingMode.DOWN, "4294967295");
        assertNarrow(dividend, divisor, RoundingMode.HALF_UP, "4294967296");
        assertWide(dividend, divisor, RoundingMode.DOWN, "4294967295");
        assertWide(dividend, divisor, RoundingMode.HALF_UP, "4294967296");
    }

    @Test
    public void testRandomAgainstBigInteger() {
        final Random rnd = new Random(SEED);
        for (int i = 0; i < 8000; i++) {
            final boolean wide = (i & 1) == 0;
            final int maxLimbs = wide ? 8 : 4;
            final int divisorLimbs = 1 + rnd.nextInt(maxLimbs - 1);
            final int quotientLimbs = 1 + rnd.nextInt(maxLimbs - divisorLimbs);
            final BigInteger divisor = randomMagnitude(rnd, divisorLimbs);
            final BigInteger quotient = randomMagnitude(rnd, quotientLimbs);
            final BigInteger remainder = new BigInteger(divisorLimbs * 32, rnd).mod(divisor);
            final BigInteger dividend = quotient.multiply(divisor).add(remainder);
            final int targetScale = rnd.nextInt(21);
            final int divisorScale = rnd.nextInt(21);
            // Scales are picked so that no pre-scaling of the operands is required.
            final int dividendScale = targetScale + divisorScale;
            final boolean negative = rnd.nextBoolean();
            final RoundingMode mode = ROUNDING_MODES[rnd.nextInt(ROUNDING_MODES.length)];

            final BigDecimal expected = new BigDecimal(negative ? dividend.negate() : dividend, dividendScale)
                    .divide(new BigDecimal(divisor, divisorScale), targetScale, mode);
            final BigInteger magnitude = divide(dividend, divisor, negative, mode, wide);
            final BigDecimal actual = new BigDecimal(negative ? magnitude.negate() : magnitude, targetScale);

            Assert.assertEquals(
                    dividend + "e-" + dividendScale + " / " + divisor + "e-" + divisorScale
                            + " scale=" + targetScale + " " + mode + (wide ? " 256bit" : " 128bit"),
                    expected,
                    actual
            );
        }
    }

    @Test
    public void testReusedInstanceDoesNotLeakTopLimb() {
        // The first division leaves a remainder of 2^128, whose top limb sits just above the second
        // division's dividend. The divisors are already normalized, so nothing shifts it out.
        Assert.assertEquals(
                BigInteger.ONE,
                divide(BigInteger.ONE.shiftLeft(159).add(BigInteger.ONE.shiftLeft(128)),
                        BigInteger.ONE.shiftLeft(159), false, RoundingMode.DOWN, true)
        );
        Assert.assertEquals(
                BigInteger.ONE,
                divide(BigInteger.ONE.shiftLeft(127), BigInteger.ONE.shiftLeft(127), false, RoundingMode.DOWN, false)
        );
    }

    private static long randomLimb(Random rnd, boolean nonZero) {
        switch (rnd.nextInt(6)) {
            case 0:
                return 0xFFFFFFFFL;
            case 1:
                return 0xFFFFFFFEL;
            case 2:
                return 0x80000000L;
            case 3:
                return 1L;
            case 4:
                return nonZero ? 0x7FFFFFFFL : 0L;
            default:
                final long v = rnd.nextInt() & 0xFFFFFFFFL;
                return nonZero && v == 0 ? 1L : v;
        }
    }

    private static BigInteger randomMagnitude(Random rnd, int limbs) {
        BigInteger v = BigInteger.ZERO;
        for (int i = 0; i < limbs; i++) {
            v = v.or(BigInteger.valueOf(randomLimb(rnd, i == limbs - 1)).shiftLeft(i * 32));
        }
        return v;
    }

    private static long word(BigInteger value, int index) {
        return value.shiftRight(index * 64).longValue();
    }

    private void assertNarrow(BigInteger dividend, BigInteger divisor, RoundingMode mode, String expected) {
        Assert.assertEquals(new BigInteger(expected), divide(dividend, divisor, false, mode, false));
    }

    private void assertWide(BigInteger dividend, BigInteger divisor, RoundingMode mode, String expected) {
        Assert.assertEquals(new BigInteger(expected), divide(dividend, divisor, false, mode, true));
    }

    private BigInteger divide(BigInteger dividend, BigInteger divisor, boolean negative, RoundingMode mode, boolean wide) {
        divider.clear();
        if (wide) {
            divider.ofDividend(word(dividend, 3), word(dividend, 2), word(dividend, 1), word(dividend, 0));
            divider.ofDivisor(word(divisor, 3), word(divisor, 2), word(divisor, 1), word(divisor, 0));
        } else {
            divider.ofDividend(word(dividend, 1), word(dividend, 0));
            divider.ofDivisor(word(divisor, 1), word(divisor, 0));
        }
        divider.divide(negative, mode);
        divider.sink(sink, 0);
        BigInteger result = BigInteger.ZERO;
        result = result.shiftLeft(64).or(unsigned(sink.getHh()));
        result = result.shiftLeft(64).or(unsigned(sink.getHl()));
        result = result.shiftLeft(64).or(unsigned(sink.getLh()));
        return result.shiftLeft(64).or(unsigned(sink.getLl()));
    }

    private BigInteger unsigned(long value) {
        return BigInteger.valueOf(value).and(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE));
    }
}
