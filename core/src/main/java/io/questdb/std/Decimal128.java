package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Decimal128 - A mutable decimal number implementation.
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 128-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 * </p>
 * <p>
 * This type tries to but doesn't follow IEEE 754; one of the main goals is using 128 bits to store
 * the sign and trailing significant field (T).
 * Using 1 bit for the sign, we have 127 bits for T, which gives us 38 digits of precision.
 * </p>
 */
public class Decimal128 implements Sinkable {
    /**
     * Maximum allowed scale (number of decimal places)
     */
    public static final int MAX_SCALE = 38;
    public static final Decimal128 MAX_VALUE = new Decimal128(Long.MAX_VALUE, Long.MIN_VALUE, 0);
    public static final Decimal128 MIN_VALUE = new Decimal128(Long.MIN_VALUE, Long.MIN_VALUE, 0);
    static final long LONG_MASK = 0xffffffffL;
    private static final long B = (long) 1 << Integer.SIZE;
    private static final long INFLATED = Long.MIN_VALUE;
    // Cache for common small values
    private static final Decimal128[] SMALL_VALUES_CACHE = new Decimal128[101]; // Cache 0-100
    private static final long[] TEN_POWERS_TABLE_HIGH = { // High 64-bit part of the ten powers table from 10^20 to 10^38
            5L, // 10^20
            54L, // 10^21
            542L, // 10^22
            5421L, // 10^23
            54210L, // 10^24
            542101L, // 10^25
            5421010L, // 10^26
            54210108L, // 10^27
            542101086L, // 10^28
            5421010862L, // 10^29
            54210108624L, // 10^30
            542101086242L, // 10^31
            5421010862427L, // 10^32
            54210108624275L, // 10^33
            542101086242752L, // 10^34
            5421010862427522L, // 10^35
            54210108624275221L, // 10^36
            542101086242752217L, // 10^37
            5421010862427522170L, // 10^38
    };
    private static final long[] TEN_POWERS_TABLE_LOW = { // Low 64-bit part of the ten powers table from 10^0 to 10^38
            1L, // 10^0
            10L, // 10^1
            100L, // 10^2
            1000L, // 10^3
            10000L, // 10^4
            100000L, // 10^5
            1000000L, // 10^6
            10000000L, // 10^7
            100000000L, // 10^8
            1000000000L, // 10^9
            10000000000L, // 10^10
            100000000000L, // 10^11
            1000000000000L, // 10^12
            10000000000000L, // 10^13
            100000000000000L, // 10^14
            1000000000000000L, // 10^15
            10000000000000000L, // 10^16
            100000000000000000L, // 10^17
            1000000000000000000L, // 10^18
            -8446744073709551616L, // 10^19
            7766279631452241920L, // 10^20
            3875820019684212736L, // 10^21
            1864712049423024128L, // 10^22
            200376420520689664L, // 10^23
            2003764205206896640L, // 10^24
            1590897978359414784L, // 10^25
            -2537764290115403776L, // 10^26
            -6930898827444486144L, // 10^27
            4477988020393345024L, // 10^28
            7886392056514347008L, // 10^29
            5076944270305263616L, // 10^30
            -4570789518076018688L, // 10^31
            -8814407033341083648L, // 10^32
            4089650035136921600L, // 10^33
            4003012203950112768L, // 10^34
            3136633892082024448L, // 10^35
            -5527149226598858752L, // 10^36
            68739955140067328L, // 10^37
            687399551400673280L, // 10^38
    };
    // High 64-bit part of the maximum value that 10^x can multiply without overflowing
    private static final long[] TEN_POWERS_TABLE_THRESHOLD_HIGH = {
            9223372036854775807L, // 10^0
            922337203685477580L, // 10^1
            92233720368547758L, // 10^2
            9223372036854775L, // 10^3
            922337203685477L, // 10^4
            92233720368547L, // 10^5
            9223372036854L, // 10^6
            922337203685L, // 10^7
            92233720368L, // 10^8
            9223372036L, // 10^9
            922337203L, // 10^10
            92233720L, // 10^11
            9223372L, // 10^12
            922337L, // 10^13
            92233L, // 10^14
            9223L, // 10^15
            922L, // 10^16
            92L, // 10^17
            9L, // 10^18
    };
    // Low 64-bit part of the maximum value that 10^x can multiply without overflowing
    private static final long[] TEN_POWERS_TABLE_THRESHOLD_LOW = {
            -9223372036854775808L, // 10^0
            -4611686018427387904L, // 10^1
            1383505805528216371L, // 10^2
            -3550998234189088687L, // 10^3
            -7733797452902729516L, // 10^4
            -4462728560032183275L, // 10^5
            -4135621670745128651L, // 10^6
            8809809869780262942L, // 10^7
            -8342391049876749514L, // 10^8
            -2678913512358630113L, // 10^9
            -5801914573348728497L, // 10^10
            6798506172148947796L, // 10^11
            679850617214894779L, // 10^12
            3757333876463399801L, // 10^13
            -5158289834466525505L, // 10^14
            6862868646037168095L, // 10^15
            6220310086716582294L, // 10^16
            4311379823413568552L, // 10^17
            4120486797083267178L, // 10^18
            -1432625727662628444L, // 10^19
            1701411834604692317L, // 10^20
            170141183460469231L, // 10^21
            17014118346046923L, // 10^22
            1701411834604692L, // 10^23
            170141183460469L, // 10^24
            17014118346046L, // 10^25
            1701411834604L, // 10^26
            170141183460L, // 10^27
            17014118346L, // 10^28
            1701411834L, // 10^29
            170141183L, // 10^30
            17014118L, // 10^31
            1701411L, // 10^32
            170141L, // 10^33
            17014L, // 10^34
            1701L, // 10^35
            170L, // 10^36
            17L, // 10^37
            1L, // 10^38
    };
    private transient long compact;  // Compact representation for values fitting in a signed long
    private long high;  // High 64 bits
    private long low;   // Low 64 bits
    private int scale;  // Number of decimal places

    /**
     * Default constructor - creates zero with scale 0
     */
    public Decimal128() {
        this.high = 0;
        this.low = 0;
        this.scale = 0;
        this.compact = 0;
    }

    /**
     * Constructor with initial values.
     *
     * @param high  the high 64 bits of the decimal value
     * @param low   the low 64 bits of the decimal value
     * @param scale the number of decimal places
     * @throws IllegalArgumentException if scale is invalid
     */
    public Decimal128(long high, long low, int scale) {
        validateScale(scale);
        this.high = high;
        this.low = low;
        this.scale = scale;
        this.compact = (high == 0 || (high == -1 && low < 0)) ? low : INFLATED;
    }

    /**
     * Copy constructor for cached values.
     *
     * @param other the Decimal128 to copy from
     */
    private Decimal128(Decimal128 other) {
        this.high = other.high;
        this.low = other.low;
        this.scale = other.scale;
        this.compact = other.compact;
    }

    /**
     * Add two Decimal128 numbers and store the result in sink
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink Destination for the result
     */
    public static void add(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.add(b);
    }

    /**
     * Divide two Decimal128 numbers and store the result in sink (a / b -> sink)
     * Uses optimal precision calculation up to MAX_SCALE
     *
     * @param a            First operand (dividend)
     * @param b            Second operand (divisor)
     * @param sink         Destination for the result
     * @param scale        Result scale
     * @param roundingMode Rounding Mode used to round the result
     */
    public static void divide(Decimal128 a, Decimal128 b, Decimal128 sink, int scale, RoundingMode roundingMode) {
        sink.copyFrom(a);
        sink.divide(b, scale, roundingMode);
    }

    /**
     * Performs a division using Knuth 4.3.1D algorithm, storing the quotient in dividend.
     *
     * @param dividend     Decimal128 that will be divided.
     * @param divisorHigh  High 64-bit part of the divisor.
     * @param divisorLow   Low 64-bit part of the divisor.
     * @param roundingMode How the quotient will be rounded if a remainder is present.
     */
    public static void divideKnuth(Decimal128 dividend, long divisorHigh, long divisorLow, boolean negResult, RoundingMode roundingMode) {
        long dividendHigh = dividend.high;
        long dividendLow = dividend.low;

        // We're going to switch on the closest case possible to reduce the number of operations/branches.
        // We're unrolling the D2-D7 loop, Knuth relies on narrowing division ("64-bit/32-bit" giving a 32-bit quotient and 32-bit reminder).
        // To match this, we would need to split our long in ints at each step.
        // here m = 0 and n = 4
        if (divisorHigh != 0) {
            if (divisorHigh >= B || divisorHigh < 0) {
                divideKnuthXx128(dividend, dividendHigh, dividendLow, divisorHigh, divisorLow, negResult, roundingMode);
            } else {
                divideKnuthXx96(dividend, dividendHigh, dividendLow, divisorHigh, divisorLow, negResult, roundingMode);
            }
        } else {
            if (divisorLow >= B || divisorLow < 0) {
                divideKnuthXxLong(dividend, dividendHigh, dividendLow, divisorLow, negResult, roundingMode);
            } else {
                divideKnuthXxWord(dividend, dividendHigh, dividendLow, divisorLow, negResult, roundingMode);
            }
        }
    }

    /**
     * Create a Decimal128 from a BigDecimal value.
     *
     * @param value the BigDecimal value to convert
     * @return a new Decimal128 representing the BigDecimal value
     * @throws IllegalArgumentException if scale is invalid
     */
    public static Decimal128 fromBigDecimal(BigDecimal value) {
        int scale = value.scale();
        long hi;
        long lo;
        BigInteger bi = value.unscaledValue();
        if (scale < 0) {
            // We don't support negative scale, we must transform the value to match
            // our format.
            bi = bi.multiply(new BigInteger("10").pow(-scale));
            scale = 0;
        }
        lo = bi.longValue();
        hi = bi.shiftRight(64).longValue();
        validateScale(scale);
        return new Decimal128(hi, lo, scale);
    }

    /**
     * Create a Decimal128 from a double value, using HALF_UP when rounding is needed.
     *
     * @param value the double value to convert
     * @param scale the number of decimal places
     * @return a new Decimal128 representing the double value
     * @throws IllegalArgumentException if scale is invalid
     */
    public static Decimal128 fromDouble(double value, int scale) {
        validateScale(scale);
        BigDecimal bd = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        return fromBigDecimal(bd);
    }

    /**
     * Create a Decimal128 from a long value.
     *
     * @param value the long value to convert
     * @param scale the number of decimal places
     * @return a new Decimal128 representing the long value
     * @throws IllegalArgumentException if scale is invalid
     */
    public static Decimal128 fromLong(long value, int scale) {
        validateScale(scale);

        // Use cached values for common small values with scale 0
        if (scale == 0 && value >= 0 && value <= 100) {
            return new Decimal128(SMALL_VALUES_CACHE[(int) value]);
        }

        long h = value < 0 ? -1L : 0L;
        return new Decimal128(h, value, scale);
    }

    /**
     * Calculate modulo of two Decimal128 numbers and store the result in sink (a % b -> sink)
     *
     * @param a    First operand (dividend)
     * @param b    Second operand (divisor)
     * @param sink Destination for the result
     */
    public static void modulo(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.modulo(b);
    }

    /**
     * Multiply two Decimal128 numbers and store the result in sink
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink Destination for the result
     */
    public static void multiply(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.multiply(b);
    }

    /**
     * Negate a Decimal128 number and store the result in sink
     *
     * @param a    Input operand to negate
     * @param sink Destination for the result
     */
    public static void negate(Decimal128 a, Decimal128 sink) {
        sink.copyFrom(a);
        sink.negate();
    }

    /**
     * Subtract two Decimal128 numbers and store the result in sink (a - b -> sink)
     *
     * @param a    First operand (minuend)
     * @param b    Second operand (subtrahend)
     * @param sink Destination for the result
     */
    public static void subtract(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.subtract(b);
    }

    /**
     * Add another Decimal128 to this one (in-place)
     *
     * @param other The Decimal128 to add
     */
    public void add(Decimal128 other) {
        add(this, this.high, this.low, this.scale, other.high, other.low, other.scale);
    }

    /**
     * Compare this to another Decimal128 (handles different scales).
     *
     * @param other the Decimal128 to compare with
     * @return -1 if this decimal is less than other, 0 if equal, 1 if greater than other
     */
    public int compareTo(Decimal128 other) {
        if (this.scale == other.scale) {
            // Same scale - direct comparison
            if (this.high != other.high) {
                return Long.compare(this.high, other.high);
            }
            return Long.compareUnsigned(this.low, other.low);
        }

        boolean aNeg = isNegative();
        boolean bNeg = other.isNegative();
        if (aNeg != bNeg) {
            return aNeg ? -1 : 1;
        }

        // Stores the coefficient to apply to the response, if both numbers are negative, then
        // we have to reverse the result
        int diffQ = 1;

        // We need to make both operands positive to detect overflows when scaling them
        long aH = this.high;
        long aL = this.low;
        long bH = other.high;
        long bL = other.low;
        if (aNeg) {
            diffQ = -1;
            long oldLow = aL;

            // Two's complement: invert all bits and add 1
            aL = ~aL + 1;
            aH = ~aH;

            // Check for carry from low
            if (aL == 0 && oldLow != 0) {
                aH += 1;
            }

            // Negate b
            oldLow = bL;

            // Two's complement: invert all bits and add 1
            bL = ~bL + 1;
            bH = ~bH;

            // Check for carry from low
            if (bL == 0 && oldLow != 0) {
                bH += 1;
            }
        }

        // Different scales - need to align for comparison
        // We'll scale up the one with smaller scale
        if (this.scale < other.scale) {
            // Scale up this to match other's scale
            int scaleDiff = other.scale - this.scale;

            // Simple iterative scaling using multiply-by-10
            for (int i = 0; i < scaleDiff; i++) {
                if ((aH >>> 3) != 0) {
                    throw NumericException.instance().put("Overflow");
                }

                // Multiply by 10: (8x + 2x)
                long high8 = (aH << 3) | (aL >>> 61);
                long low8 = aL << 3;
                long high2 = (aH << 1) | (aL >>> 63);
                long low2 = aL << 1;

                aL = low8 + low2;
                long carry = hasCarry(low8, low2, aL) ? 1 : 0;
                try {
                    aH = Math.addExact(high8, Math.addExact(high2, carry));
                } catch (ArithmeticException e) {
                    throw NumericException.instance().put("Overflow");
                }
            }

            // Compare scaled this with other
            if (aH != bH) {
                return Long.compare(aH, bH) * diffQ;
            }
            return Long.compareUnsigned(aL, bL) * diffQ;
        } else {
            // Scale up other to match this scale
            int scaleDiff = this.scale - other.scale;

            // Simple iterative scaling using multiply-by-10
            for (int i = 0; i < scaleDiff; i++) {
                if ((bH >>> 3) != 0) {
                    throw NumericException.instance().put("Overflow");
                }

                // Multiply by 10: (8x + 2x)
                long high8 = (bH << 3) | (bL >>> 61);
                long low8 = bL << 3;
                long high2 = (bH << 1) | (bL >>> 63);
                long low2 = bL << 1;

                bL = low8 + low2;
                long carry = hasCarry(low8, low2, bL) ? 1 : 0;
                try {
                    bH = Math.addExact(high8, Math.addExact(high2, carry));
                } catch (ArithmeticException e) {
                    throw NumericException.instance().put("Overflow");
                }
            }

            // Compare this with scaled other
            if (aH != bH) {
                return Long.compare(aH, bH) * diffQ;
            }
            return Long.compareUnsigned(aL, bL) * diffQ;
        }
    }

    /**
     * Copy values from another Decimal128
     */
    public void copyFrom(Decimal128 source) {
        this.high = source.high;
        this.low = source.low;
        this.scale = source.scale;
        this.compact = source.compact;
    }


    /**
     * Divide this Decimal128 by another (in-place) with optimal precision
     *
     * @param divisor      The Decimal128 to divide by
     * @param scale        The decimal place
     * @param roundingMode The Rounding mode to use if the remainder is non-zero
     */
    public void divide(Decimal128 divisor, int scale, RoundingMode roundingMode) {
        divide(divisor.high, divisor.low, divisor.scale, scale, roundingMode);
    }

    /**
     * Divide this Decimal128 by another (in-place) with optimal precision
     *
     * @param scale        The decimal place
     * @param roundingMode The Rounding mode to use if the remainder is non-zero
     */
    public void divide(long divisorHigh, long divisorLow, int divisorScale, int scale, RoundingMode roundingMode) {
        validateScale(scale);
        // Compute the delta: how much power of 10 we should raise either the dividend or divisor.
        int delta = scale + (divisorScale - this.scale);

        // Fail early if we're sure to overflow.
        if (delta > 0 && (this.scale + delta) > MAX_SCALE) {
            throw NumericException.instance().put("Overflow");
        } else if (delta < 0 && (divisorScale + delta) > MAX_SCALE) {
            throw NumericException.instance().put("Overflow");
        }

        final boolean negResult = (divisorHigh < 0) ^ isNegative();

        // We're allowed to modify dividend as it will contain our result
        if (isNegative()) {
            negate();
        }

        // We cannot do the same for divisor, so we must reuse negate logic directly in our code to avoid
        // allocations.
        if (divisorHigh < 0 && divisorLow != 0) {
            divisorLow = ~divisorLow + 1;
            divisorHigh = ~divisorHigh;
            if (divisorLow == 0) {
                divisorHigh += 1;
            }
        }

        if (delta > 0) {
            // raise dividend to 10^delta
            multiplyByPowerOf10InPlace(delta);
        } else if (delta < 0) {
            // raise divisor to 10^(-delta), as we cannot modify the divisor, we use dividend to do it.
            long dividendHigh = this.high;
            long dividendLow = this.low;
            long compact = this.compact;
            this.high = divisorHigh;
            this.low = divisorLow;
            multiplyByPowerOf10InPlace(-delta);
            divisorHigh = this.high;
            divisorLow = this.low;
            this.high = dividendHigh;
            this.low = dividendLow;
            this.compact = compact;
        }
        divideKnuth(this, divisorHigh, divisorLow, negResult, roundingMode);
        this.scale = scale;
        this.updateCompact();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Decimal128)) return false;
        Decimal128 other = (Decimal128) obj;
        return this.high == other.high &&
                this.low == other.low &&
                this.scale == other.scale;
    }

    /**
     * Gets the high 64 bits of the 128-bit decimal value.
     *
     * @return the high 64 bits of the decimal value
     */
    public long getHigh() {
        return high;
    }

    /**
     * Gets the low 64 bits of the 128-bit decimal value.
     *
     * @return the low 64 bits of the decimal value
     */
    public long getLow() {
        return low;
    }

    /**
     * Gets the scale (number of decimal places) of this decimal value.
     *
     * @return the scale of this decimal value
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns a hash code for this decimal value.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Long.hashCode(high) ^ Long.hashCode(low) ^ Integer.hashCode(scale);
    }

    /**
     * Check if this number is negative
     */
    public boolean isNegative() {
        return high < 0;
    }

    /**
     * Check if this number is zero
     */
    public boolean isZero() {
        return high == 0 && low == 0;
    }

    /**
     * Calculate modulo in-place.
     *
     * @param divisor the divisor
     * @throws NumericException if divisor is zero
     */
    public void modulo(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw NumericException.instance().put("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Use simple repeated subtraction for modulo: a % b = a - (a / b) * b
        // First compute integer division (a / b)
        // We store this for later usage
        long thisH = this.high;
        long thisL = this.low;
        int thisScale = this.scale;

        this.divide(divisor, 0, RoundingMode.DOWN);

        // Now compute this * divisor
        this.multiply(divisor);

        long qH = this.high;
        long qL = this.low;
        int qScale = this.scale;
        // restore this as a
        this.set(thisH, thisL, thisScale);
        // Finally compute remainder: a - (a / b) * b
        this.subtract(qH, qL, qScale);

        // Handle scale adjustment
        if (this.scale != resultScale) {
            if (this.scale < resultScale) {
                int scaleUp = resultScale - this.scale;
                boolean isNegative = isNegative();
                if (isNegative) {
                    negate();
                }
                multiplyByPowerOf10InPlace(scaleUp);
                if (isNegative) {
                    negate();
                }
            } else {
                int scaleDown = this.scale - resultScale;
                for (int i = 0; i < scaleDown; i++) {
                    divideBy10InPlace();
                }
            }
            this.scale = resultScale;
        }
    }

    /**
     * Multiply this Decimal128 by another (in-place)
     *
     * @param other The Decimal128 to multiply by
     */
    public void multiply(Decimal128 other) {
        multiply(other.high, other.low, other.scale);
    }

    /**
     * Negate this number in-place
     */
    public void negate() {
        // Special case: negating zero should remain zero
        if (this.high == 0 && this.low == 0) {
            return;
        }

        long oldLow = this.low;

        // Two's complement: invert all bits and add 1
        this.low = ~this.low + 1;
        this.high = ~this.high;

        // Check for carry from low
        if (this.low == 0 && oldLow != 0) {
            this.high += 1;
        }
    }

    /**
     * Round this Decimal128 to the specified scale using the given rounding mode.
     * This method performs in-place rounding without requiring a divisor.
     *
     * @param targetScale  the desired scale (number of decimal places)
     * @param roundingMode the rounding mode to use
     * @throws IllegalArgumentException if targetScale is invalid
     * @throws NumericException      if roundingMode is UNNECESSARY and rounding is required
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        validateScale(targetScale);

        // UNNECESSARY mode should be a complete no-op
        if (roundingMode == RoundingMode.UNNECESSARY) {
            return;
        }

        // Handle zero specially
        if (isZero()) {
            this.scale = targetScale;
            return;
        }


        if (this.scale < targetScale) {
            boolean isNegative = isNegative();
            if (isNegative) {
                negate();
            }

            // Need to increase scale (add trailing zeros)
            int scaleIncrease = targetScale - this.scale;
            multiplyByPowerOf10InPlace(scaleIncrease);
            this.scale = targetScale;

            if (isNegative) {
                negate();
            }
            return;
        }

        divide(0, 1, 0, targetScale, roundingMode);
    }

    /**
     * Set values directly.
     *
     * @param high  the high 64 bits of the decimal value
     * @param low   the low 64 bits of the decimal value
     * @param scale the number of decimal places
     */
    public void set(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
        updateCompact();
    }

    /**
     * Set from a long value.
     *
     * @param value the long value to set
     * @param scale the number of decimal places
     */
    public void setFromLong(long value, int scale) {
        validateScale(scale);
        this.high = value < 0 ? -1L : 0L;
        this.low = value;
        this.scale = scale;
        updateCompact();
    }

    /**
     * Sets the high 64 bits of the decimal value.
     *
     * @param high the high 64 bits to set
     */
    public void setHigh(long high) {
        this.high = high;
        updateCompact();
    }

    /**
     * Sets the low 64 bits of the decimal value.
     *
     * @param low the low 64 bits to set
     */
    public void setLow(long low) {
        this.low = low;
        updateCompact();
    }

    /**
     * Sets the scale (number of decimal places).
     *
     * @param scale the number of decimal places
     * @throws IllegalArgumentException if scale is invalid
     */
    public void setScale(int scale) {
        validateScale(scale);
        this.scale = scale;
    }

    /**
     * Subtract another Decimal128 from this one (in-place)
     *
     * @param other The Decimal128 to subtract
     */
    public void subtract(Decimal128 other) {
        subtract(other.high, other.low, other.scale);
    }

    /**
     * Subtract another Decimal128 from this one (in-place)
     *
     * @param bH     the high 64 bits of the other Decimal128
     * @param bL     the low 64 bits of the other Decimal128
     * @param bScale the number of decimal places of the other Decimal128
     */
    public void subtract(long bH, long bL, int bScale) {
        // Negate other and perform addition
        if (bH != 0 || bL != 0) {
            long oldLow = bL;

            // Two's complement: invert all bits and add 1
            bL = ~bL + 1;
            bH = ~bH;

            // Check for carry from low
            if (bL == 0 && oldLow != 0) {
                bH += 1;
            }

            add(this, this.high, this.low, this.scale, bH, bL, bScale);
        }
    }

    /**
     * Convert to BigDecimal with full precision
     *
     * @return BigDecimal representation of this Decimal128
     */
    public java.math.BigDecimal toBigDecimal() {
        // Direct BigInteger construction is more efficient than string conversion
        byte[] bytes = new byte[16];
        // Convert to big-endian byte array
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (this.high >>> (56 - 8 * i));
            bytes[8 + i] = (byte) (this.low >>> (56 - 8 * i));
        }

        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, this.scale);
    }

    /**
     * Convert to double (may lose precision).
     *
     * @return double representation of this Decimal128
     */
    public double toDouble() {
        // Calculate the divisor (10^scale)
        double divisor = 1.0;
        for (int i = 0; i < scale; i++) {
            divisor *= 10.0;
        }

        // Convert 128-bit integer to double
        double result;

        if (high >= 0) {
            // Positive number
            // result = high * 2^64 + low (treating low as unsigned)
            result = (double) high * 18446744073709551616.0 + unsignedToDouble(low);
        } else {
            // Negative number - need to handle two's complement
            // For negative numbers, we need to convert from two's complement
            if (low == 0) {
                // Special case: low is 0, just negate after division
                result = (double) high * 18446744073709551616.0;
            } else {
                // Two's complement: ~high * 2^64 + ~low + 1
                // But we need to be careful with the arithmetic
                long negHigh = ~high;
                long negLow = ~low + 1;

                // Check for carry from low negation
                if (negLow == 0) {
                    negHigh += 1;
                }

                // Now we have the absolute value in negHigh:negLow
                result = -((double) negHigh * 18446744073709551616.0 + unsignedToDouble(negLow));
            }
        }

        return result / divisor;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        if (high == 0) {
            // Case: value fits in 64 bits (could be large unsigned)
            if (low >= 0) {
                // Positive value - use signed arithmetic
                longToDecimalSink(low, scale, sink);
            } else {
                // Large unsigned value that appears negative as signed long
                // Convert to unsigned string first, then format
                unsignedLongToDecimalSink(low, scale, sink);
            }
        } else if (high == -1 && low < 0) {
            // Simple negative case: small negative number
            longToDecimalSink(low, scale, sink);
        } else {
            // Complex case: full 128-bit conversion
            fullToSink(sink);
        }
    }

    /**
     * Returns a string representation of this decimal value.
     *
     * @return string representation of this Decimal128
     */
    @Override
    public String toString() {
        // Use StringSink which is already a CharSink - for compatibility
        StringSink sink = new StringSink();
        toSink(sink);
        return sink.toString();
    }

    /**
     * Generic function to make a 128-bit addition.
     *
     * @param result Decimal128 that will store the result of the operation
     * @param aH     High 64-bit part of the first operand.
     * @param aL     Low 64-bit part of the first operand.
     * @param aScale Scale of the first operand.
     * @param bH     High 64-bit part of the second operand.
     * @param bL     Low 64-bit part of the second operand.
     * @param bScale Scale of the second operand.
     */
    private static void add(Decimal128 result, long aH, long aL, int aScale, long bH, long bL, int bScale) {
        result.scale = aScale;
        if (aScale < bScale) {
            // We need to rescale a to the same scale as b
            result.high = aH;
            result.low = aL;
            result.rescale(bScale);
            aH = result.high;
            aL = result.low;
        } else if (aScale > bScale) {
            // We need to rescale b to the same scale as a
            result.high = bH;
            result.low = bL;
            result.scale = bScale;
            result.rescale(aScale);
            bH = result.high;
            bL = result.low;
        }

        // Perform 128-bit addition
        long sumLow = aL + bL;

        // Check for carry
        long carry = hasCarry(aL, bL, sumLow) ? 1 : 0;

        result.low = sumLow;
        try {
            result.high = Math.addExact(aH, Math.addExact(bH, carry));
        } catch (ArithmeticException e) {
            throw NumericException.instance().put("Overflow");
        }
        result.updateCompact();
    }

    /**
     * Append a long value to sink without allocation
     */
    private static void appendLongToSink(long value, CharSink<?> sink) {
        if (value == 0) {
            sink.putAscii('0');
            return;
        }

        // Find the highest power of 10 that fits in the value
        long divisor = 1;
        long temp = value;
        while (temp >= 10) {
            divisor *= 10;
            temp /= 10;
        }

        // Output digits from most significant to least significant
        while (divisor > 0) {
            int digit = (int) (value / divisor);
            sink.putAscii((char) ('0' + digit));
            value %= divisor;
            divisor /= 10;
        }
    }

    /**
     * Compare a against half of b.
     *
     * @param aH High 64-bit part of a.
     * @param aL Low 64-bit part of a.
     * @param bH High 64-bit part of b.
     * @param bL Low 64-bit part of b.
     * @return 1 if a > b/2, 0 if a == b/2 or -1 otherwise
     */
    private static int compareHalf(long aH, long aL, long bH, long bL) {
        int h = (int) (bL & 1L);
        bL = bL >>> 1 | bH << 63;
        bH >>>= 1;
        int cmp = compareUnsigned(aH, aL, bH, bL);
        if (cmp == 0) {
            return h == 0 ? 0 : -1;
        }
        return cmp;
    }

    /**
     * Compare two unsigned 128-bit numbers
     *
     * @return negative if a < b, 0 if a == b, positive if a > b
     */
    private static int compareUnsigned(long aHigh, long aLow, long bHigh, long bLow) {
        int highCmp = Long.compareUnsigned(aHigh, bHigh);
        if (highCmp != 0) {
            return highCmp;
        }
        return Long.compareUnsigned(aLow, bLow);
    }

    /**
     * Algorithm from MutableBigInteger::divideMagnitude to correct qhat in Knutd 4.3.1D algorithm.
     *
     * @param qhat  quotient estimation
     * @param rhat  remainder of quotient estimation
     * @param vnm1  Divisor highest 32-bit part v_(n-1)
     * @param vnm2  Divisor second-highest 32-bit part v_(n-2)
     * @param ujnm2 Dividend 32-bit part: u_(j+n-2)
     * @return the corrected qhat
     */
    private static long correctQhat(long qhat, long rhat, int vnm1, int vnm2, int ujnm2) {
        long vnm1Long = vnm1 & LONG_MASK;
        long nl = ujnm2 & LONG_MASK;
        long rs = ((rhat & LONG_MASK) << Integer.SIZE) | nl;
        long estProduct = (qhat & LONG_MASK) * (vnm2 & LONG_MASK);
        if (unsignedLongCompare(estProduct, rs)) {
            qhat--;
            rhat = (rhat + vnm1Long) & 0xFFFFFFFFL;
            if (rhat >= vnm1Long) {
                estProduct -= vnm2 & LONG_MASK;
                rs = ((rhat & LONG_MASK) << 32) | nl;
                if (unsignedLongCompare(estProduct, rs)) {
                    qhat--;
                }
            }
        }
        return qhat;
    }

    /**
     * Count the number of digits in a positive long value
     */
    private static int countDigits(long value) {
        if (value == 0) return 1;
        if (value < 10L) return 1;
        if (value < 100L) return 2;
        if (value < 1000L) return 3;
        if (value < 10000L) return 4;
        if (value < 100000L) return 5;
        if (value < 1000000L) return 6;
        if (value < 10000000L) return 7;
        if (value < 100000000L) return 8;
        if (value < 1000000000L) return 9;
        if (value < 10000000000L) return 10;
        if (value < 100000000000L) return 11;
        if (value < 1000000000000L) return 12;
        if (value < 10000000000000L) return 13;
        if (value < 100000000000000L) return 14;
        if (value < 1000000000000000L) return 15;
        if (value < 10000000000000000L) return 16;
        if (value < 100000000000000000L) return 17;
        if (value < 1000000000000000000L) return 18;
        return 19;
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for 2 128-bit numbers.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal128 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisorHigh  64-bit high part of the divisor
     * @param divisorLow   64-bit low part of the divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth128x128(Decimal128 result, long dividendHH, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
        // Check for overflow - if dividendHH has upper bits set, the result would overflow 128 bits
        // We can only handle at most 128 bits (32 bits in u4)
        if ((dividendHH >>> 32) != 0) {
            throw NumericException.instance().put("Overflow");
        }

        int v3 = (int) (divisorHigh >>> 32);
        int v2 = (int) (divisorHigh & LONG_MASK);
        int v1 = (int) (divisorLow >>> 32);
        int v0 = (int) divisorLow;

        int u4 = (int) (dividendHH);
        int u3 = (int) (dividendHigh >>> 32);
        int u2 = (int) dividendHigh;
        int u1 = (int) (dividendLow >>> 32);
        int u0 = (int) dividendLow;

        long v3Long = v3 & LONG_MASK;

        // Step D3
        long nChunk = ((long) u4 << Integer.SIZE) | (u3 & LONG_MASK);
        long qhat = Long.divideUnsigned(nChunk, v3Long);
        long rhat = Long.remainderUnsigned(nChunk, v3Long);

        qhat = correctQhat(qhat, rhat, v3, v2, u2);

        // Step D4
        long p = qhat * v0;
        long t = ((long) u0 & LONG_MASK) - (p & LONG_MASK);
        u0 = (int) t;
        long k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u1 & LONG_MASK) - (p & LONG_MASK) - k;
        u1 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v2 & LONG_MASK);
        t = ((long) u2 & LONG_MASK) - (p & LONG_MASK) - k;
        u2 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v3 & LONG_MASK);
        t = ((long) u3 & LONG_MASK) - (p & LONG_MASK) - k;
        u3 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u4 & LONG_MASK) - k;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u0 + v0 + k;
            u0 = (int) t;
            k = (t >>> 32);
            t = u1 + v1 + k;
            u1 = (int) t;
            k = (t >>> 32);
            t = u2 + v2 + k;
            u2 = (int) t;
            k = (t >>> 32);
            t = u3 + v3 + k;
            u3 = (int) t;
        }

        // qhat is now the quotient
        result.high = 0;
        result.low = qhat;

        final boolean oddQuot = (qhat & 1L) == 1L;
        final long remainderHigh = (u2 & LONG_MASK) | ((u3 & LONG_MASK) << 32);
        final long remainderLow = (u0 & LONG_MASK) | ((u1 & LONG_MASK) << 32);
        endKnuth(result, remainderHigh, remainderLow, divisorHigh, divisorLow, oddQuot, roundingMode, isNegative);
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for a 128-bit number divided by
     * a 96-bit number.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal128 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisorHigh  64-bit high part of the divisor
     * @param divisorLow   64-bit low part of the divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth128x96(Decimal128 result, long dividendHH, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
        int v2 = (int) (divisorHigh & LONG_MASK);
        int v1 = (int) (divisorLow >>> 32);
        int v0 = (int) divisorLow;

        int u4 = (int) dividendHH;
        int u3 = (int) (dividendHigh >>> 32);
        int u2 = (int) dividendHigh;
        int u1 = (int) (dividendLow >>> 32);
        int u0 = (int) dividendLow;

        long v2Long = v2 & LONG_MASK;

        // j = 1
        // Step D3
        long nChunk = ((long) u4 << Integer.SIZE) | (u3 & LONG_MASK);
        long qhat = Long.divideUnsigned(nChunk, v2Long);
        long rhat = Long.remainderUnsigned(nChunk, v2Long);

        qhat = correctQhat(qhat, rhat, v2, v1, u2);

        // Step D4
        long p = qhat * (v0 & LONG_MASK);
        long t = ((long) u1 & LONG_MASK) - (p & LONG_MASK);
        u1 = (int) t;
        long k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u2 & LONG_MASK) - (p & LONG_MASK) - k;
        u2 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v2 & LONG_MASK);
        t = ((long) u3 & LONG_MASK) - (p & LONG_MASK) - k;
        u3 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u4 & LONG_MASK) - k;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u1 + v0 + k;
            u1 = (int) t;
            k = (t >>> 32);
            t = u2 + v1 + k;
            u2 = (int) t;
            k = (t >>> 32);
            t = u3 + v2 + k;
            u3 = (int) t;
        }

        final long q1 = qhat;

        // j = 0
        // Step D3
        nChunk = ((long) u3 << Integer.SIZE) | (u2 & LONG_MASK);
        qhat = Long.divideUnsigned(nChunk, v2Long);
        rhat = Long.remainderUnsigned(nChunk, v2Long);

        qhat = correctQhat(qhat, rhat, v2, v1, u1);

        // Step D4
        p = qhat * (v0 & LONG_MASK);
        t = ((long) u0 & LONG_MASK) - (p & LONG_MASK);
        u0 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u1 & LONG_MASK) - (p & LONG_MASK) - k;
        u1 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v2 & LONG_MASK);
        t = ((long) u2 & LONG_MASK) - (p & LONG_MASK) - k;
        u2 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u3 & LONG_MASK) - k;
        u3 = (int) t;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u0 + v0 + k;
            u0 = (int) t;
            k = (t >>> 32);
            t = u1 + v1 + k;
            u1 = (int) t;
            k = (t >>> 32);
            t = u2 + v2 + k;
            u2 = (int) t;
        }

        result.high = 0;
        result.low = ((q1 & LONG_MASK) << 32) | (qhat & LONG_MASK);

        final boolean oddQuot = (qhat & 1L) == 1L;
        final long remainderHigh = (u2 & LONG_MASK) | ((u3 & LONG_MASK) << 32);
        final long remainderLow = (u0 & LONG_MASK) | ((u1 & LONG_MASK) << 32);
        endKnuth(result, remainderHigh, remainderLow, divisorHigh, divisorLow, oddQuot, roundingMode, isNegative);
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for a 128-bit number divided by
     * a 64-bit number.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal128 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisor      64-bit divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth128xLong(Decimal128 result, long dividendHH, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
        int v1 = (int) (divisor >>> 32);
        int v0 = (int) divisor;

        int u4 = (int) (dividendHH);
        int u3 = (int) (dividendHigh >>> 32);
        int u2 = (int) dividendHigh;
        int u1 = (int) (dividendLow >>> 32);
        int u0 = (int) dividendLow;

        long v1Long = v1 & LONG_MASK;

        // j = 2
        // Step D3
        long nChunk = ((u4 & LONG_MASK) << Integer.SIZE) | (u3 & LONG_MASK);
        long qhat = Long.divideUnsigned(nChunk, v1Long);
        long rhat = Long.remainderUnsigned(nChunk, v1Long);

        qhat = correctQhat(qhat, rhat, v1, v0, u2);

        // Step D4
        long p = qhat * (v0 & LONG_MASK);
        long t = ((long) u2 & LONG_MASK) - (p & LONG_MASK);
        u2 = (int) t;
        long k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u3 & LONG_MASK) - (p & LONG_MASK) - k;
        u3 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u4 & LONG_MASK) - k;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u2 + v0 + k;
            u2 = (int) t;
            k = (t >>> 32);
            t = u3 + v1 + k;
            u3 = (int) t;
        }

        final long q2 = qhat;

        // j = 1
        // Step D3
        nChunk = ((u3 & LONG_MASK) << Integer.SIZE) | (u2 & LONG_MASK);
        qhat = Long.divideUnsigned(nChunk, v1Long);
        rhat = Long.remainderUnsigned(nChunk, v1Long);

        qhat = correctQhat(qhat, rhat, v1, v0, u1);

        // Step D4
        p = qhat * (v0 & LONG_MASK);
        t = ((long) u1 & LONG_MASK) - (p & LONG_MASK);
        u1 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u2 & LONG_MASK) - (p & LONG_MASK) - k;
        u2 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u3 & LONG_MASK) - k;
        u3 = (int) t;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u1 + v0 + k;
            u1 = (int) t;
            k = (t >>> 32);
            t = u2 + v1 + k;
            u2 = (int) t;
        }

        final long q1 = qhat;


        // j = 0
        // Step D3
        nChunk = ((u2 & LONG_MASK) << Integer.SIZE) | (u1 & LONG_MASK);
        qhat = Long.divideUnsigned(nChunk, v1Long);
        rhat = Long.remainderUnsigned(nChunk, v1Long);

        qhat = correctQhat(qhat, rhat, v1, v0, u0);

        // Step D4
        p = qhat * (v0 & LONG_MASK);
        t = ((long) u0 & LONG_MASK) - (p & LONG_MASK);
        u0 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u1 & LONG_MASK) - (p & LONG_MASK) - k;
        u1 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u2 & LONG_MASK) - k;
        u2 = (int) t;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u0 + v0 + k;
            u0 = (int) t;
            k = (t >>> 32);
            t = u1 + v1 + k;
            u1 = (int) t;
        }

        result.high = q2 & LONG_MASK;
        result.low = ((q1 & LONG_MASK) << 32) | (qhat & LONG_MASK);

        final boolean oddQuot = (qhat & 1L) == 1L;
        final long remainderHigh = (u2 & LONG_MASK) | ((u3 & LONG_MASK) << 32);
        final long remainderLow = (u0 & LONG_MASK) | ((u1 & LONG_MASK) << 32);
        endKnuth(result, remainderHigh, remainderLow, 0, divisor, oddQuot, roundingMode, isNegative);
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for 2 96-bit numbers.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal128 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisorHigh  64-bit high part of the divisor
     * @param divisorLow   64-bit low part of the divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth96x96(Decimal128 result, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
        int v2 = (int) (divisorHigh & LONG_MASK);
        int v1 = (int) (divisorLow >>> 32);
        int v0 = (int) divisorLow;

        int u3 = (int) (dividendHigh >>> 32);
        int u2 = (int) dividendHigh;
        int u1 = (int) (dividendLow >>> 32);
        int u0 = (int) dividendLow;

        long v2Long = v2 & LONG_MASK;

        // Step D3
        long nChunk = ((long) u3 << Integer.SIZE) | (u2 & LONG_MASK);
        long qhat = Long.divideUnsigned(nChunk, v2Long);
        long rhat = Long.remainderUnsigned(nChunk, v2Long);

        qhat = correctQhat(qhat, rhat, v2, v1, u1);

        // Step D4
        long p = qhat * (v0 & LONG_MASK);
        long t = ((long) u0 & LONG_MASK) - (p & LONG_MASK);
        u0 = (int) t;
        long k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u1 & LONG_MASK) - (p & LONG_MASK) - k;
        u1 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v2 & LONG_MASK);
        t = ((long) u2 & LONG_MASK) - (p & LONG_MASK) - k;
        u2 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u3 & LONG_MASK) - k;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u0 + v0 + k;
            u0 = (int) t;
            k = (t >>> 32);
            t = u1 + v1 + k;
            u1 = (int) t;
            k = (t >>> 32);
            t = u2 + v2 + k;
            u2 = (int) t;
        }

        // qhat is now the quotient
        result.high = 0;
        result.low = qhat;

        final boolean oddQuot = (qhat & 1L) == 1L;
        final long remainderHigh = (u2 & LONG_MASK);
        final long remainderLow = (u0 & LONG_MASK) | ((u1 & LONG_MASK) << 32);
        endKnuth(result, remainderHigh, remainderLow, divisorHigh, divisorLow, oddQuot, roundingMode, isNegative);
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for a 96-bit number divided by
     * a 64-bit number.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal128 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisor      64-bit divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth96xLong(Decimal128 result, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
        int v1 = (int) ((divisor >>> 32) & LONG_MASK);
        int v0 = (int) divisor;

        int u3 = (int) (dividendHigh >>> 32);
        int u2 = (int) dividendHigh;
        int u1 = (int) (dividendLow >>> 32);
        int u0 = (int) dividendLow;

        long v1Long = v1 & LONG_MASK;

        // j = 1
        // Step D3
        long nChunk = ((long) u3 << Integer.SIZE) | (u2 & LONG_MASK);
        long qhat = Long.divideUnsigned(nChunk, v1Long);
        long rhat = Long.remainderUnsigned(nChunk, v1Long);

        qhat = correctQhat(qhat, rhat, v1, v0, u1);

        // Step D4
        long p = qhat * (v0 & LONG_MASK);
        long t = ((long) u1 & LONG_MASK) - (p & LONG_MASK);
        u1 = (int) t;
        long k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u2 & LONG_MASK) - (p & LONG_MASK) - k;
        u2 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u3 & LONG_MASK) - k;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u1 + v0 + k;
            u1 = (int) t;
            k = (t >>> 32);
            t = u2 + v1 + k;
            u2 = (int) t;
        }

        final long q1 = qhat;

        // j = 0
        // Step D3
        nChunk = ((long) u2 << Integer.SIZE) | (u1 & LONG_MASK);
        qhat = Long.divideUnsigned(nChunk, v1Long);
        rhat = Long.remainderUnsigned(nChunk, v1Long);

        qhat = correctQhat(qhat, rhat, v1, v0, u0);

        // Step D4
        p = qhat * (v0 & LONG_MASK);
        t = ((long) u0 & LONG_MASK) - (p & LONG_MASK);
        u0 = (int) t;
        k = (p >>> 32) - (t >> 32);
        p = qhat * (v1 & LONG_MASK);
        t = ((long) u1 & LONG_MASK) - (p & LONG_MASK) - k;
        u1 = (int) t;
        k = (p >>> 32) - (t >> 32);
        t = ((long) u2 & LONG_MASK) - k;
        u2 = (int) t;

        // Step D5
        if (t < 0) {
            // Step D6
            qhat--;
            t = u0 + v0 + k;
            u0 = (int) t;
            k = (t >>> 32);
            t = u1 + v1 + k;
            u1 = (int) t;
        }

        result.high = 0;
        result.low = ((q1 & LONG_MASK) << 32) | (qhat & LONG_MASK);

        final boolean oddQuot = (qhat & 1L) == 1L;
        final long remainderHigh = (u2 & LONG_MASK);
        final long remainderLow = (u0 & LONG_MASK) | ((u1 & LONG_MASK) << 32);
        endKnuth(result, remainderHigh, remainderLow, 0, divisor, oddQuot, roundingMode, isNegative);
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for 2 64-bit numbers.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal128 that will store the rounded result
     * @param dividend     64-bit dividend
     * @param divisor      64-bit divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuthLongxLong(Decimal128 result, long dividend, long divisor, boolean isNegative, RoundingMode roundingMode) {
        long q = Long.divideUnsigned(dividend, divisor);
        long r = Long.remainderUnsigned(dividend, divisor);

        result.high = 0;
        result.low = q;

        final boolean oddQuot = (q & 1L) == 1L;
        endKnuth(result, 0, r, 0, divisor, oddQuot, roundingMode, isNegative);
    }

    private static void divideKnuthXx128(Decimal128 result, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
        if ((dividendHigh >>> 32) != 0) {
            // Step D1: Normalize (common to every division)
            final int shift = Integer.numberOfLeadingZeros((int) (divisorHigh >>> 32));
            long dividendHH = 0;
            if (shift != 0) {
                dividendHH = dividendHigh >>> (63 & -shift);
                dividendHigh = dividendHigh << shift | (dividendLow >>> (63 & -shift));
                dividendLow <<= shift;
                divisorHigh = divisorHigh << shift | (divisorLow >>> (63 & -shift));
                divisorLow <<= shift;
            }

            divideKnuth128x128(result, dividendHH, dividendHigh, dividendLow, divisorHigh, divisorLow, isNegative, roundingMode);
        } else {
            result.high = 0;
            result.low = 0;
            endKnuth(result, dividendHigh, dividendLow, divisorHigh, divisorLow, false, roundingMode, isNegative);
        }
    }

    private static void divideKnuthXx96(Decimal128 result, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
        if (dividendHigh != 0) {
            boolean is128 = (dividendHigh >>> 32) != 0;
            // Step D1: Normalize (common to every division)
            final int shift = Integer.numberOfLeadingZeros((int) (divisorHigh & LONG_MASK));
            long dividendHH = 0;
            if (shift != 0) {
                dividendHH = dividendHigh >>> (63 & -shift);
                dividendHigh = dividendHigh << shift | (dividendLow >>> (63 & -shift));
                dividendLow <<= shift;
                divisorHigh = divisorHigh << shift | (divisorLow >>> (63 & -shift));
                divisorLow <<= shift;
            }

            if (is128) {
                divideKnuth128x96(result, dividendHH, dividendHigh, dividendLow, divisorHigh, divisorLow, isNegative, roundingMode);
            } else {
                divideKnuth96x96(result, dividendHigh, dividendLow, divisorHigh, divisorLow, isNegative, roundingMode);
            }
        } else {
            result.high = 0;
            result.low = 0;
            endKnuth(result, dividendHigh, dividendLow, divisorHigh, divisorLow, false, roundingMode, isNegative);
        }
    }

    private static void divideKnuthXxLong(Decimal128 result, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
        if (dividendHigh != 0) {
            boolean is128 = (dividendHigh >>> 32) != 0;

            // Step D1: Normalize (common to every division)
            final int shift = Integer.numberOfLeadingZeros((int) (divisor >>> 32));
            long dividendHH = 0;
            if (shift != 0) {
                dividendHH = dividendHigh >>> (63 & -shift);
                dividendHigh = dividendHigh << shift | (dividendLow >>> (63 & -shift));
                dividendLow <<= shift;
                divisor <<= shift;
            }

            if (is128) {
                divideKnuth128xLong(result, dividendHH, dividendHigh, dividendLow, divisor, isNegative, roundingMode);
            } else {
                divideKnuth96xLong(result, dividendHigh, dividendLow, divisor, isNegative, roundingMode);
            }
        } else {
            if ((dividendLow >>> 32) != 0) {
                divideKnuthLongxLong(result, dividendLow, divisor, isNegative, roundingMode);
            } else {
                result.high = 0;
                result.low = 0;
                endKnuth(result, 0, dividendLow, 0, divisor, false, roundingMode, isNegative);
            }
        }
    }

    /**
     * Divide any 128-bit numbers by a 32-bit one using Knuth 4.3.1 exercise 16.
     *
     * @param result       Decimal128 where the result will be written
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisor      32-bit divisor
     * @param isNegative   whether the result should be negative
     * @param roundingMode rounding mode used if there is a remainder
     */
    private static void divideKnuthXxWord(Decimal128 result, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
        int divisorInt = (int) divisor;
        if (divisor == 0) {
            throw NumericException.instance().put("Division by zero");
        }

        if (divisor == 1) {
            result.high = dividendHigh;
            result.low = dividendLow;
            if (isNegative) {
                result.negate();
            }
            return;
        }

        int u3 = (int) (dividendHigh >>> 32);
        int u2 = (int) dividendHigh;
        int u1 = (int) (dividendLow >>> 32);
        int u0 = (int) dividendLow;

        int r = u3;
        long rLong = (long) r & LONG_MASK;

        int q3 = 0;
        if (rLong >= divisor) {
            q3 = (int) (rLong / divisor);
            r = (int) (rLong - (long) q3 * divisor);
            rLong = (long) r & LONG_MASK;
        }

        long qhat = rLong << 32 | (u2 & LONG_MASK);
        int q2;
        if (qhat >= 0L) {
            q2 = (int) (qhat / divisor);
            r = (int) (qhat - (long) q2 * divisor);
        } else {
            long tmp = divWord(qhat, divisorInt);
            q2 = (int) (tmp & LONG_MASK);
            r = (int) (tmp >>> 32);
        }
        rLong = (long) r & LONG_MASK;

        qhat = rLong << 32 | (u1 & LONG_MASK);
        int q1;
        if (qhat >= 0L) {
            q1 = (int) (qhat / divisor);
            r = (int) (qhat - (long) q1 * divisor);
        } else {
            long tmp = divWord(qhat, divisorInt);
            q1 = (int) (tmp & LONG_MASK);
            r = (int) (tmp >>> 32);
        }
        rLong = (long) r & LONG_MASK;

        qhat = rLong << 32 | (u0 & LONG_MASK);
        int q0;
        if (qhat >= 0L) {
            q0 = (int) (qhat / divisor);
            r = (int) (qhat - (long) q0 * divisor);
        } else {
            long tmp = divWord(qhat, divisorInt);
            q0 = (int) (tmp & LONG_MASK);
            r = (int) (tmp >>> 32);
        }

        result.high = ((q3 & LONG_MASK) << 32) | (q2 & LONG_MASK);
        result.low = ((q1 & LONG_MASK) << 32) | (q0 & LONG_MASK);

        final boolean oddQuot = (q0 & 1L) == 1L;
        endKnuth(result, 0, r, 0, divisor, oddQuot, roundingMode, isNegative);
    }

    private static void endKnuth(Decimal128 result, long remainderHigh, long remainderLow, long divisorHigh, long divisorLow, boolean oddQuot, RoundingMode roundingMode, boolean isNegative) {
        if (remainderHigh == 0 && remainderLow == 0) {
            if (isNegative) {
                result.negate();
            }
            return;
        }

        // We can ignore Step D8 as we only need the remainder to compare with the divisor and if we don't unnormalize neither of them
        // then they are still comparable.

        boolean increment = false;
        switch (roundingMode) {
            case UNNECESSARY:
                throw NumericException.instance().put("Rounding necessary");
            case UP: // Away from zero
                increment = true;
                break;

            case DOWN: // Towards zero
                break;

            case CEILING: // Towards +infinity
                increment = !isNegative;
                break;

            case FLOOR: // Towards -infinity
                increment = isNegative;
                break;

            default: // Some kind of half-way rounding
                int cmp = compareHalf(remainderHigh, remainderLow, divisorHigh, divisorLow);
                if (cmp > 0) {
                    increment = true;
                } else if (cmp == 0) {
                    switch (roundingMode) {
                        case HALF_UP:
                            increment = true;
                            break;
                        case HALF_EVEN:
                            increment = oddQuot;
                            break;
                        default:
                    }
                }
        }

        if (increment) {
            result.low++;
            if (result.low == 0) {
                result.high++;
            }
        }

        if (isNegative) {
            result.negate();
        }
    }

    /**
     * Compare two longs as if they were unsigned.
     * Returns true iff one is bigger than two.
     */
    private static boolean unsignedLongCompare(long one, long two) {
        return (one + Long.MIN_VALUE) > (two + Long.MIN_VALUE);
    }

    /**
     * Convert unsigned long to double
     * Java's long is signed, but we need to treat it as unsigned for the low part
     */
    private static double unsignedToDouble(long value) {
        if (value >= 0) {
            return (double) value;
        } else {
            // For negative values (which represent large unsigned values)
            // Split into two parts to avoid precision loss
            // value = 2^63 + (value & 0x7FFFFFFFFFFFFFFF)
            return 9223372036854775808.0 + (double) (value & 0x7FFFFFFFFFFFFFFFL);
        }
    }

    /**
     * Validates that the scale is within allowed bounds
     */
    private static void validateScale(int scale) {
        // Use unsigned comparison for faster bounds check
        if (Integer.compareUnsigned(scale, MAX_SCALE) > 0) {
            throw new IllegalArgumentException("Scale must be between 0 and " + MAX_SCALE + ", got: " + scale);
        }
    }

    /**
     * Divide this by 10 in place
     */
    private void divideBy10InPlace() {
        // Simple case
        if (this.high == 0 && this.low < 10) {
            this.low = 0;
            return;
        }

        // Use our division algorithm for dividing by 10
        long quotientHigh = 0;
        long quotientLow = 0;
        long remainder = 0;

        // Divide high part
        if (this.high != 0) {
            quotientHigh = Long.divideUnsigned(this.high, 10);
            remainder = Long.remainderUnsigned(this.high, 10);
        }

        // Combine remainder with low part for division
        // We need to compute (remainder * 2^64 + low) / 10
        // Do this bit by bit to avoid overflow
        for (int i = 63; i >= 0; i--) {
            remainder = remainder * 2 + ((this.low >>> i) & 1);
            if (remainder >= 10) {
                quotientLow |= (1L << i);
                remainder -= 10;
            }
        }

        this.high = quotientHigh;
        this.low = quotientLow;
    }

    /**
     * Full 128-bit to sink conversion (simplified version)
     * For production use, consider using BigInteger for complex cases
     */
    private void fullToSink(CharSink<?> sink) {
        // Convert the 128-bit value to BigInteger first, then handle sign and formatting
        java.math.BigInteger bigInt;

        // Create BigInteger from the 128-bit representation
        if (high == 0) {
            // Simple positive case: fits in 64 bits
            bigInt = java.math.BigInteger.valueOf(low);
        } else if (high == -1 && low < 0) {
            // Simple negative case: small negative number that fits in signed long
            bigInt = java.math.BigInteger.valueOf(low);
        } else if (high < 0) {
            // Negative 128-bit number - use two's complement to get absolute value
            long absHigh = ~high;
            long absLow = ~low + 1;
            if (low == 0 && absLow == 0) {
                absHigh++; // Handle carry
            }

            // Create positive BigInteger for absolute value
            java.math.BigInteger absBigInt;
            if (absHigh == 0) {
                absBigInt = new java.math.BigInteger(Long.toUnsignedString(absLow));
            } else {
                absBigInt = java.math.BigInteger.valueOf(absHigh).shiftLeft(64).add(new java.math.BigInteger(Long.toUnsignedString(absLow)));
            }

            // Make it negative
            bigInt = absBigInt.negate();
        } else {
            // Positive 128-bit number
            bigInt = java.math.BigInteger.valueOf(high).shiftLeft(64).add(new java.math.BigInteger(Long.toUnsignedString(low)));
        }

        // Convert to string (BigInteger handles the sign)
        String valueStr = bigInt.toString();

        // Handle sign separately for formatting
        boolean negative = valueStr.startsWith("-");
        String digits = negative ? valueStr.substring(1) : valueStr;

        // Apply decimal formatting based on scale
        if (negative) {
            sink.putAscii('-');
        }

        if (scale == 0) {
            // Integer
            sink.put(digits);
        } else {
            // Decimal
            if (digits.length() <= scale) {
                // Number < 1: output 0.00...digits
                sink.putAscii('0').putAscii('.');
                for (int i = 0; i < scale - digits.length(); i++) {
                    sink.putAscii('0');
                }
                sink.put(digits);
            } else {
                // Number >= 1: split into integer.fractional
                int splitPoint = digits.length() - scale;
                sink.put(digits.substring(0, splitPoint));
                sink.putAscii('.');
                sink.put(digits.substring(splitPoint));
            }
        }
    }

    /**
     * Convert a long to decimal representation in a sink with scale (allocation-free)
     */
    private void longToDecimalSink(long value, int scale, CharSink<?> sink) {
        if (scale == 0) {
            sink.put(value);
            return;
        }

        // Handle negative numbers
        boolean negative = value < 0;
        long absValue = negative ? -value : value;

        if (negative) {
            sink.putAscii('-');
        }

        // Calculate number of digits in absValue
        int digits = countDigits(absValue);

        if (digits <= scale) {
            // Need to pad with leading zeros: 0.00...value
            sink.putAscii('0').putAscii('.');
            // Add leading zeros
            for (int i = 0; i < scale - digits; i++) {
                sink.putAscii('0');
            }
            // Add the actual digits
            appendLongToSink(absValue, sink);
        } else {
            // Split into integer and fractional parts
            // Extract integer part
            long divisor = 1;
            for (int i = 0; i < scale; i++) {
                divisor *= 10;
            }
            long integerPart = absValue / divisor;
            long fractionalPart = absValue % divisor;

            // Output integer part
            appendLongToSink(integerPart, sink);

            // Output decimal point
            sink.putAscii('.');

            // Output fractional part with leading zeros if needed
            if (fractionalPart == 0) {
                // Special case: all trailing zeros
                for (int i = 0; i < scale; i++) {
                    sink.putAscii('0');
                }
            } else {
                // Pad with leading zeros and append fractional part
                int fracDigits = countDigits(fractionalPart);
                for (int i = 0; i < scale - fracDigits; i++) {
                    sink.putAscii('0');
                }
                appendLongToSink(fractionalPart, sink);
            }
        }
    }

    /**
     * Multiply this Decimal128 by another (in-place)
     *
     * @param bH     the high 64 bits of the other Decimal128
     * @param bL     the low 64 bits of the other Decimal128
     * @param bScale the number of decimal places of the other Decimal128
     */
    private void multiply(long bH, long bL, int bScale) {
        // Result scale is sum of scales
        int resultScale = this.scale + bScale;

        // Save the original signs before we modify anything
        boolean thisNegative = this.isNegative();
        boolean bNegative = bH < 0;

        // Convert to positive values for multiplication algorithm
        if (thisNegative) {
            negate();
        }

        // Get absolute value of other
        if (bNegative) {
            // Negate other's values
            bL = ~bL + 1;
            bH = ~bH;
            if (bL == 0) {
                bH += 1;
            }
        }

        if (bH == 0 && bL >= 0) {
            multiplyBy64Bit(bL);
        } else {
            multiplyBy128Bit(bH, bL);
        }

        // Handle sign - use the saved original signs
        boolean negative = (thisNegative != bNegative);
        if (negative) {
            // Negate result
            this.low = ~this.low + 1;
            long newHigh = ~this.high;
            if (this.low == 0) {
                newHigh += 1;
            }
            this.high = newHigh;
        }

        this.scale = resultScale;
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 128-bit value in place
     */
    private void multiplyBy128Bit(long multiplierHigh, long multiplierLow) {
        // Perform multiplication using the algorithm from Decimal128
        // This is complex but avoids allocations
        long a3 = this.high >>> 32;
        long a2 = this.high & 0xFFFFFFFFL;
        long a1 = this.low >>> 32;
        long a0 = this.low & 0xFFFFFFFFL;

        long b3 = multiplierHigh >>> 32;
        long b2 = multiplierHigh & 0xFFFFFFFFL;
        long b1 = multiplierLow >>> 32;
        long b0 = multiplierLow & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;

        // Gather results into 128-bit result
        long r0 = (p00 & LONG_MASK);
        long r1 = (p00 >>> 32) + (p01 & LONG_MASK) + (p10 & LONG_MASK);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & LONG_MASK) + (p11 & LONG_MASK) + (p20 & LONG_MASK);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & LONG_MASK) + (p12 & LONG_MASK) + (p21 & LONG_MASK) + (p30 & LONG_MASK);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p13 & LONG_MASK) + (p22 & LONG_MASK) + (p31 & LONG_MASK);
        long r5 = (r4 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) +
                (p23 & LONG_MASK) + (p32 & LONG_MASK);
        long r6 = (r5 >>> 32) + (p23 >>> 32) + (p32 >>> 32) +
                (p33 & LONG_MASK);

        if ((r3 >>> 31) != 0 || r4 != 0 || r5 != 0 || r6 != 0) {
            throw NumericException.instance().put("Overflow");
        }

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 128-bit value in place without checking for overflow
     */
    private void multiplyBy128BitUnchecked(long multiplierHigh, long multiplierLow) {
        // Perform multiplication using the algorithm from Decimal128
        // This is complex but avoids allocations
        long a3 = this.high >>> 32;
        long a2 = this.high & 0xFFFFFFFFL;
        long a1 = this.low >>> 32;
        long a0 = this.low & 0xFFFFFFFFL;

        long b3 = multiplierHigh >>> 32;
        long b2 = multiplierHigh & 0xFFFFFFFFL;
        long b1 = multiplierLow >>> 32;
        long b0 = multiplierLow & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p30 = a3 * b0;

        // Gather results into 128-bit result
        long r0 = (p00 & LONG_MASK);
        long r1 = (p00 >>> 32) + (p01 & LONG_MASK) + (p10 & LONG_MASK);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & LONG_MASK) + (p11 & LONG_MASK) + (p20 & LONG_MASK);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & LONG_MASK) + (p12 & LONG_MASK) + (p21 & LONG_MASK) + (p30 & LONG_MASK);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 64-bit value in place
     */
    private void multiplyBy64Bit(long multiplier) {
        // Perform 128-bit × 64-bit multiplication
        // Result is at most 192 bits, but we keep only the lower 128 bits

        // Split multiplier into two 32-bit parts
        long m1 = multiplier >>> 32;
        long m0 = multiplier & 0xFFFFFFFFL;

        // Split this into four 32-bit parts
        long a3 = high >>> 32;
        long a2 = high & 0xFFFFFFFFL;
        long a1 = low >>> 32;
        long a0 = low & 0xFFFFFFFFL;

        // Compute partial products
        long p0 = a0 * m0;
        long p1 = a0 * m1 + a1 * m0;
        long p2 = a1 * m1 + a2 * m0;
        long p3 = a2 * m1 + a3 * m0;
        long p4 = a3 * m1;

        // Accumulate results
        long r0 = p0 & 0xFFFFFFFFL;
        long r1 = (p0 >>> 32) + (p1 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p1 >>> 32) + (p2 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p2 >>> 32) + (p3 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p3 >>> 32) + p4;

        // Check for overflow: if r4 has significant bits, the result exceeds 128 bits
        if (r4 != 0 || (r3 >> 31) != 0) {
            throw NumericException.instance().put("Overflow");
        }

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 64-bit value in place without checking for overflow
     */
    private void multiplyBy64BitUnchecked(long multiplier) {
        // Perform 128-bit × 64-bit multiplication
        // Result is at most 192 bits, but we keep only the lower 128 bits

        // Split multiplier into two 32-bit parts
        long m1 = multiplier >>> 32;
        long m0 = multiplier & 0xFFFFFFFFL;

        // Split this into four 32-bit parts
        long a3 = high >>> 32;
        long a2 = high & 0xFFFFFFFFL;
        long a1 = low >>> 32;
        long a0 = low & 0xFFFFFFFFL;

        // Compute partial products
        long p0 = a0 * m0;
        long p1 = a0 * m1 + a1 * m0;
        long p2 = a1 * m1 + a2 * m0;
        long p3 = a2 * m1 + a3 * m0;

        // Accumulate results
        long r0 = p0 & 0xFFFFFFFFL;
        long r1 = (p0 >>> 32) + (p1 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p1 >>> 32) + (p2 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p2 >>> 32) + (p3 & 0xFFFFFFFFL);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this (unsigned) by 10^n in place
     */
    private void multiplyByPowerOf10InPlace(int n) {
        if (n <= 0 || (high == 0 && low == 0)) {
            return;
        }
        if (n > 38) {
            throw NumericException.instance().put("Overflow");
        }

        // For small powers, use lookup table
        if (n < 18) {
            long multiplier = TEN_POWERS_TABLE_LOW[n];
            // Special case: if high is 0, use simple 64-bit multiplication
            if (this.high == 0 && this.low >= 0) {
                // Check if result will overflow 64 bits
                if (this.low <= Long.MAX_VALUE / multiplier) {
                    this.low *= multiplier;
                    updateCompact();
                    return;
                }
            }
        }

        // For larger powers, break down into smaller chunks, first check the threshold to ensure that we won't overflow
        // and then apply either a fast multiplyBy64 or multiplyBy128 depending on high/low.
        // The bound checks for these tables already happens at the beginning of the method.
        final long thresholdH = n >= TEN_POWERS_TABLE_THRESHOLD_HIGH.length ? 0 : TEN_POWERS_TABLE_THRESHOLD_HIGH[n];
        final long thresholdL = TEN_POWERS_TABLE_THRESHOLD_LOW[n];

        if (high > thresholdH || (high == thresholdH && unsignedLongCompare(low, thresholdL))) {
            throw NumericException.instance().put("Overflow");
        }

        final long multiplierHigh = n >= 20 ? TEN_POWERS_TABLE_HIGH[n - 20] : 0L;
        final long multiplierLow = TEN_POWERS_TABLE_LOW[n];
        if (multiplierHigh == 0L && multiplierLow >= 0L) {
            multiplyBy64BitUnchecked(multiplierLow);
        } else {
            multiplyBy128BitUnchecked(multiplierHigh, multiplierLow);
        }
        updateCompact();
    }

    /**
     * Rescale this Decimal128 in place
     *
     * @param newScale The new scale (must be >= current scale)
     */
    private void rescale(int newScale) {
        if (newScale < this.scale) {
            throw new IllegalArgumentException("Cannot reduce scale");
        }

        int scaleDiff = newScale - this.scale;

        boolean isNegative = isNegative();
        if (isNegative) {
            negate();
        }

        // Multiply by 10^scaleDiff
        multiplyByPowerOf10InPlace(scaleDiff);

        if (isNegative) {
            negate();
        }

        this.scale = newScale;
    }

    /**
     * Convert an unsigned long to decimal representation in a sink with scale (allocation-free)
     */
    private void unsignedLongToDecimalSink(long value, int scale, CharSink<?> sink) {
        if (scale == 0) {
            // Integer case - output as unsigned
            sink.put(Long.toUnsignedString(value));
            return;
        }

        // Convert to string as unsigned
        String digits = Long.toUnsignedString(value);

        if (digits.length() <= scale) {
            // Need to pad with leading zeros: 0.00...digits
            sink.putAscii('0').putAscii('.');
            // Add leading zeros
            for (int i = 0; i < scale - digits.length(); i++) {
                sink.putAscii('0');
            }
            // Add the actual digits
            sink.put(digits);
        } else {
            // Split into integer and fractional parts
            // Extract integer part
            int splitPoint = digits.length() - scale;
            sink.put(digits.substring(0, splitPoint));
            // Output decimal point
            sink.putAscii('.');
            // Output fractional part
            sink.put(digits.substring(splitPoint));
        }
    }

    private void updateCompact() {
        this.compact = (this.high == 0 || (this.high == -1 && this.low < 0)) ? this.low : INFLATED;
    }

    static long divWord(long n, int d) {
        long dLong = d & LONG_MASK;
        if (dLong == 1L) {
            return n & LONG_MASK;
        } else {
            long q = (n >>> 1) / (dLong >>> 1);

            long r;
            for (r = n - q * dLong; r < 0L; --q) {
                r += dLong;
            }

            while (r >= dLong) {
                r -= dLong;
                ++q;
            }

            return r << 32 | q & LONG_MASK;
        }
    }

    /**
     * Check if addition resulted in a carry
     * When adding two unsigned numbers a + b = sum, carry occurs iff sum < a (or sum < b)
     * This works because:
     * - No carry: sum = a + b, so sum >= a and sum >= b
     * - Carry: sum = a + b - 2^64, so sum < a and sum < b
     */
    static boolean hasCarry(long a, long b, long sum) {
        // We can check against either a or b - both work
        // Using a for consistency, b parameter kept for clarity
        return Long.compareUnsigned(sum, a) < 0;
    }

    static {
        for (int i = 0; i <= 100; i++) {
            SMALL_VALUES_CACHE[i] = new Decimal128(0, i, 0);
        }
    }
}