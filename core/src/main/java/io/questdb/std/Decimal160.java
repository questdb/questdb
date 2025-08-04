package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Decimal160 - A mutable 160-bit decimal number implementation
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 160-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 * <p>
 * This type tries to but doesn't follow IEEE 754; one of the main goals is using 128 bits to store
 * the sign and trailing significant field (T).
 * Using 1 bit for the sign, we have 127 bits for T, which gives us 38 digits of precision.
 * T valid values are in the (-10^38;10^38) interval, values outside are either invalid
 * or may be special like NaN or Inf.
 */
public class Decimal160 implements Sinkable {
    /**
     * Maximum allowed scale (number of decimal places)
     */
    public static final int MAX_SCALE = 38;
    static final long LONG_MASK = 0xffffffffL;
    private static final long B = (long) 1 << Integer.SIZE;
    private static final long INFLATED = Long.MIN_VALUE;
    private static final long HALF_LONG_MAX_VALUE = Long.MAX_VALUE / 2;
    private static final long HALF_LONG_MIN_VALUE = Long.MIN_VALUE / 2;
    private static final int LONG_SCALE_THRESHOLD = 19; // Maximum scale for a long before overflowing
    private static final long[] LONG_TEN_POWERS_TABLE = {
            1L,                     // 0 / 10^0
            10L,                    // 1 / 10^1
            100L,                   // 2 / 10^2
            1000L,                  // 3 / 10^3
            10000L,                 // 4 / 10^4
            100000L,                // 5 / 10^5
            1000000L,               // 6 / 10^6
            10000000L,              // 7 / 10^7
            100000000L,             // 8 / 10^8
            1000000000L,            // 9 / 10^9
            10000000000L,          // 10 / 10^10
            100000000000L,         // 11 / 10^11
            1000000000000L,        // 12 / 10^12
            10000000000000L,       // 13 / 10^13
            100000000000000L,      // 14 / 10^14
            1000000000000000L,     // 15 / 10^15
            10000000000000000L,    // 16 / 10^16
            100000000000000000L,   // 17 / 10^17
            1000000000000000000L   // 18 / 10^18
    };
    private static final long THRESHOLDS_TABLE[] = {
            Long.MAX_VALUE,                     // 0
            Long.MAX_VALUE / 10L,                 // 1
            Long.MAX_VALUE / 100L,                // 2
            Long.MAX_VALUE / 1000L,               // 3
            Long.MAX_VALUE / 10000L,              // 4
            Long.MAX_VALUE / 100000L,             // 5
            Long.MAX_VALUE / 1000000L,            // 6
            Long.MAX_VALUE / 10000000L,           // 7
            Long.MAX_VALUE / 100000000L,          // 8
            Long.MAX_VALUE / 1000000000L,         // 9
            Long.MAX_VALUE / 10000000000L,        // 10
            Long.MAX_VALUE / 100000000000L,       // 11
            Long.MAX_VALUE / 1000000000000L,      // 12
            Long.MAX_VALUE / 10000000000000L,     // 13
            Long.MAX_VALUE / 100000000000000L,    // 14
            Long.MAX_VALUE / 1000000000000000L,   // 15
            Long.MAX_VALUE / 10000000000000000L,  // 16
            Long.MAX_VALUE / 100000000000000000L, // 17
            Long.MAX_VALUE / 1000000000000000000L // 18
    };
    private static final double TWO_POW_64 = Math.pow(2, 64);
    // Cache for common small values
    private static final Decimal160[] ZERO_THROUGH_TEN = new Decimal160[11];
    private transient long compact;  // Compact representation for values fitting in a signed long
    private long high;  // High 64 bits
    private long low;   // Low 64 bits
    private int scale;  // Number of decimal places

    /**
     * Default constructor - creates zero with scale 0
     */
    public Decimal160() {
        this.high = 0;
        this.low = 0;
        this.scale = 0;
        this.compact = 0;
    }

    /**
     * Constructor with initial values
     */
    public Decimal160(long high, long low, int scale) {
        validateScale(scale);
        this.high = high;
        this.low = low;
        this.scale = scale;
        this.compact = computeCompact(high, low);
    }

    /**
     * Copy constructor for cached values
     */
    private Decimal160(Decimal160 other) {
        this.high = other.high;
        this.low = other.low;
        this.scale = other.scale;
        this.compact = other.compact;
    }

    /**
     * Add two Decimal160 numbers and store the result in sink
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink Destination for the result
     */
    public static void add(Decimal160 a, Decimal160 b, Decimal160 sink) {
        sink.copyFrom(a);
        sink.add(b);
    }

    /**
     * Divide two Decimal160 numbers and store the result in sink (a / b -> sink)
     * Uses optimal precision calculation up to MAX_SCALE
     *
     * @param a    First operand (dividend)
     * @param b    Second operand (divisor)
     * @param sink Destination for the result
     */
    public static void divide(Decimal160 a, Decimal160 b, Decimal160 sink) {
        sink.copyFrom(a);
        sink.divide(b);
    }

    /**
     * Performs a division using Knuth 4.3.1D algorithm, storing the quotient in dividend.
     *
     * @param dividend     Decimal160 that will be divided.
     * @param divisorHigh  High 64-bit part of the divisor.
     * @param divisorLow   Low 64-bit part of the divisor.
     * @param roundingMode How the quotient will be rounded if a remainder is present.
     */
    public static void divideKnuth(Decimal160 dividend, long divisorHigh, long divisorLow, boolean negResult, RoundingMode roundingMode) {
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
     * Create a Decimal160 from a BigDecimal value
     *
     * @param value The BigDecimal value
     */
    public static Decimal160 fromBigDecimal(BigDecimal value) {
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
        return new Decimal160(hi, lo, scale);
    }

    /**
     * Create a Decimal160 from a double value
     *
     * @param value The double value
     * @param scale Number of decimal places
     */
    public static Decimal160 fromDouble(double value, int scale) {
        validateScale(scale);
        long scaleFactor = scale <= 18 ? LONG_TEN_POWERS_TABLE[scale] : calculatePowerOf10(scale);
        long scaledValue = Math.round(value * scaleFactor);
        return fromLong(scaledValue, scale);
    }

    /**
     * Create a Decimal160 from a long value
     *
     * @param value The long value
     * @param scale Number of decimal places
     */
    public static Decimal160 fromLong(long value, int scale) {
        validateScale(scale);

        // Use cached values for common small values with scale 0
        if (scale == 0 && value >= 0 && value <= 10) {
            return new Decimal160(ZERO_THROUGH_TEN[(int) value]);
        }

        long h = value < 0 ? -1L : 0L;
        return new Decimal160(h, value, scale);
    }

    /**
     * Calculate modulo of two Decimal160 numbers and store the result in sink (a % b -> sink)
     *
     * @param a    First operand (dividend)
     * @param b    Second operand (divisor)
     * @param sink Destination for the result
     */
    public static void modulo(Decimal160 a, Decimal160 b, Decimal160 sink) {
        sink.copyFrom(a);
        sink.modulo(b);
    }

    /**
     * Multiply two Decimal160 numbers and store the result in sink
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink Destination for the result
     */
    public static void multiply(Decimal160 a, Decimal160 b, Decimal160 sink) {
        sink.copyFrom(a);
        sink.multiply(b);
    }

    /**
     * Negate a Decimal160 number and store the result in sink
     *
     * @param a    Input operand to negate
     * @param sink Destination for the result
     */
    public static void negate(Decimal160 a, Decimal160 sink) {
        sink.copyFrom(a);
        sink.negate();
    }

    /**
     * Subtract two Decimal160 numbers and store the result in sink (a - b -> sink)
     *
     * @param a    First operand (minuend)
     * @param b    Second operand (subtrahend)
     * @param sink Destination for the result
     */
    public static void subtract(Decimal160 a, Decimal160 b, Decimal160 sink) {
        sink.copyFrom(a);
        sink.subtract(b);
    }

    /**
     * Add another Decimal160 to this one (in-place)
     *
     * @param other The Decimal160 to add
     */
    public void add(Decimal160 other) {
        // If scales match, use direct addition
        if (this.scale == other.scale) {
            // Perform 128-bit addition
            long sumLow = this.low + other.low;

            // Check for carry
            long carry = hasCarry(this.low, other.low, sumLow) ? 1 : 0;

            // Update values in place
            this.low = sumLow;
            this.high = this.high + other.high + carry;
            updateCompact();
            return;
        }

        // Handle different scales
        if (this.scale < other.scale) {
            // Rescale this to match other's scale
            rescale(other.scale);
            // Now add with same scale
            long sumLow = this.low + other.low;
            long carry = hasCarry(this.low, other.low, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + other.high + carry;
            updateCompact();
        } else {
            // Need to rescale other - we'll do it mathematically
            // Scale difference
            int scaleDiff = this.scale - other.scale;
            long otherHigh = other.high;
            long otherLow = other.low;

            // Multiply other by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (otherHigh << 3) | (otherLow >>> 61);
                long low8 = otherLow << 3;
                long high2 = (otherHigh << 1) | (otherLow >>> 63);
                long low2 = otherLow << 1;

                otherLow = low8 + low2;
                long carry = hasCarry(low8, low2, otherLow) ? 1 : 0;
                otherHigh = high8 + high2 + carry;
            }

            // Now add the scaled value
            long sumLow = this.low + otherLow;
            long carry = hasCarry(this.low, otherLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + otherHigh + carry;
            updateCompact();
        }
    }

    /**
     * Compare this to another Decimal160 (handles different scales)
     */
    public int compareTo(Decimal160 other) {
        if (this.scale == other.scale) {
            // Same scale - direct comparison
            if (this.high != other.high) {
                return Long.compare(this.high, other.high);
            }
            return Long.compareUnsigned(this.low, other.low);
        }

        // Different scales - need to align for comparison
        // We'll scale up the one with smaller scale
        if (this.scale < other.scale) {
            // Scale up this to match other's scale
            int scaleDiff = other.scale - this.scale;
            long scaledHigh = this.high;
            long scaledLow = this.low;

            // Multiply by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (scaledHigh << 3) | (scaledLow >>> 61);
                long low8 = scaledLow << 3;
                long high2 = (scaledHigh << 1) | (scaledLow >>> 63);
                long low2 = scaledLow << 1;

                scaledLow = low8 + low2;
                long carry = hasCarry(low8, low2, scaledLow) ? 1 : 0;
                scaledHigh = high8 + high2 + carry;
            }

            // Compare scaled this with other
            if (scaledHigh != other.high) {
                return Long.compare(scaledHigh, other.high);
            }
            return Long.compareUnsigned(scaledLow, other.low);
        } else {
            // Scale up other to match this scale
            int scaleDiff = this.scale - other.scale;
            long scaledHigh = other.high;
            long scaledLow = other.low;

            // Multiply by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (scaledHigh << 3) | (scaledLow >>> 61);
                long low8 = scaledLow << 3;
                long high2 = (scaledHigh << 1) | (scaledLow >>> 63);
                long low2 = scaledLow << 1;

                scaledLow = low8 + low2;
                long carry = hasCarry(low8, low2, scaledLow) ? 1 : 0;
                scaledHigh = high8 + high2 + carry;
            }

            // Compare this with scaled other
            if (this.high != scaledHigh) {
                return Long.compare(this.high, scaledHigh);
            }
            return Long.compareUnsigned(this.low, scaledLow);
        }
    }

    /**
     * Copy values from another Decimal160
     */
    public void copyFrom(Decimal160 source) {
        this.high = source.high;
        this.low = source.low;
        this.scale = source.scale;
        this.compact = source.compact;
    }

    /**
     * Divide this Decimal160 by another (in-place) with optimal precision
     *
     * @param divisor      The Decimal160 to divide by
     * @param scale        The decimal place
     * @param roundingMode The Rounding mode to use if the remainder is non-zero
     */
    public void divide(Decimal160 divisor, int scale, RoundingMode roundingMode) {
        // Compute the delta: how much power of 10 we should raise either the dividend or divisor.
        int delta = scale + (divisor.scale - this.scale);

        // Tries to avoid heavy computation if we have compacted values,
        // we may have to fall back if we're overflowing when scaling values.
        boolean compactSuccess = false;
        if (compact != INFLATED && (delta <= 0 || (this.scale + delta) <= LONG_SCALE_THRESHOLD)) {
            if (divisor.compact != INFLATED  && (delta >= 0 || (divisor.scale - delta) <= LONG_SCALE_THRESHOLD)) {
                compactSuccess = divide(this, low, divisor.low, delta, roundingMode);
            }
        }
        if (compactSuccess) {
            this.scale = scale;
            this.updateCompact();
            return;
        }

        // Fail early if we're sure to overflow.
        if (delta > 0 && (this.scale + delta) > MAX_SCALE) {
            throw new ArithmeticException("Overflow, not enough precision to accommodate for scale");
        } else if (delta < 0 && (divisor.scale + delta) > MAX_SCALE) {
            throw new ArithmeticException("Overflow, not enough precision to accommodate for scale");
        }

        long divisorHigh = divisor.high;
        long divisorLow = divisor.low;
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

    /**
     * Divide this Decimal160 by another (in-place) with optimal precision
     * Uses dynamic scale calculation up to MAX_SCALE to avoid excessive trailing zeros
     * Always uses UNNECESSARY rounding - caller should use round() method if rounding needed
     *
     * @param divisor The Decimal160 to divide by
     */
    public void divide(Decimal160 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Fast path for zero dividend
        if (this.isZero()) {
            this.scale = MAX_SCALE;
            return;
        }

        // Fast path for simple 64-bit native division
        if (canUseSimple64BitNativeDivision(divisor)) {
            performSimple64BitNativeDivision(divisor);
            return;
        }

        // Calculate optimal result scale
        // We want to maximize precision without causing overflow during calculation
        // First, determine the natural scale for the division
        int resultScale = MAX_SCALE;
        // Calculate scale adjustment needed
        int scaleAdjustment = MAX_SCALE + divisor.scale - this.scale;
        // Limit scale adjustment to prevent overflow
        // We need to be conservative because multiplication by 10^n can overflow
        // Rule of thumb: each multiplication by 10 adds about 3.32 bits
        // For a value using k bits, we can multiply by 10^n where n <= (127-k)/3.32
        if (scaleAdjustment > 0) {
            // Estimate bits used by current value
            int bitsUsed = estimateBitsUsed();
            // Maximum safe multiplications (conservative estimate)
            int maxSafeMultiplications = Math.max(0, (125 - bitsUsed) / 4);
            if (scaleAdjustment > maxSafeMultiplications) {
                // Reduce scale adjustment to safe level
                scaleAdjustment = maxSafeMultiplications;
                // Adjust result scale accordingly
                resultScale = this.scale - divisor.scale + scaleAdjustment;
                // Ensure result scale is non-negative
                if (resultScale < 0) {
                    resultScale = 0;
                    scaleAdjustment = divisor.scale - this.scale;
                    if (scaleAdjustment < 0) {
                        scaleAdjustment = 0;
                    }
                }
            }
        }

        // Scale the dividend if needed
        if (scaleAdjustment > 0) {
            multiplyByPowerOf10InPlace(scaleAdjustment);
        }

        // Determine sign of result
        boolean resultNegative = this.isNegative() ^ divisor.isNegative();

        // Convert both operands to positive for division
        if (this.isNegative()) {
            this.negate();
        }

        Decimal160 posDivisor = new Decimal160();
        posDivisor.copyFrom(divisor);
        if (posDivisor.isNegative()) {
            posDivisor.negate();
        }

        // Perform the division using the helper method
        divide(posDivisor.high, posDivisor.low, RoundingMode.DOWN, resultNegative);

        // Apply the result sign
        if (resultNegative && !this.isZero()) {
            this.negate();
        }

        // Set the result scale
        this.scale = resultScale;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Decimal160)) return false;
        Decimal160 other = (Decimal160) obj;
        return this.high == other.high &&
                this.low == other.low &&
                this.scale == other.scale;
    }

    // Getters
    public long getHigh() {
        return high;
    }

    public long getLow() {
        return low;
    }

    public int getScale() {
        return scale;
    }

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
     * Calculate modulo in-place
     *
     * @param divisor The divisor
     */
    public void modulo(Decimal160 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Save original dividend
        Decimal160 originalDividend = new Decimal160();
        originalDividend.copyFrom(this);

        // Use simple repeated subtraction for modulo: a % b = a - (a / b) * b
        // First compute integer division (a / b)
        Decimal160 quotient = new Decimal160();
        quotient.copyFrom(this);
        quotient.divide(divisor); // Use new signature
        quotient.round(0, RoundingMode.DOWN); // Integer division (scale 0)

        // Now compute quotient * divisor
        quotient.multiply(divisor);

        // Finally compute remainder: a - (a / b) * b
        this.subtract(quotient);

        // Handle scale adjustment
        if (this.scale != resultScale) {
            if (this.scale < resultScale) {
                int scaleUp = resultScale - this.scale;
                multiplyByPowerOf10InPlace(scaleUp);
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
     * Multiply this Decimal160 by another (in-place)
     *
     * @param other The Decimal160 to multiply by
     */
    public void multiply(Decimal160 other) {
        // Fast path for 64-bit values using magnitude arithmetic
        // TEMPORARILY DISABLED - debugging modulo issue
        // if (this.is64BitValue() && other.is64BitValue()) {
        //     multiply64BitOptimized(other);
        //     return;
        // }

        // Result scale is sum of scales
        int resultScale = this.scale + other.scale;

        // Save the original signs before we modify anything
        boolean thisNegative = this.isNegative();
        boolean otherNegative = other.isNegative();

        // Convert to positive values for multiplication algorithm
        if (thisNegative) {
            negate();
        }

        // Get absolute value of other
        long otherHighAbs = other.high;
        long otherLowAbs = other.low;
        if (otherNegative) {
            // Negate other's values
            otherLowAbs = ~otherLowAbs + 1;
            otherHighAbs = ~otherHighAbs;
            if (otherLowAbs == 0) {
                otherHighAbs += 1;
            }
        }

        // Perform multiplication using the algorithm from Decimal160
        // This is complex but avoids allocations
        long a3 = this.high >>> 32;
        long a2 = this.high & 0xFFFFFFFFL;
        long a1 = this.low >>> 32;
        long a0 = this.low & 0xFFFFFFFFL;

        long b3 = otherHighAbs >>> 32;
        long b2 = otherHighAbs & 0xFFFFFFFFL;
        long b1 = otherLowAbs >>> 32;
        long b0 = otherLowAbs & 0xFFFFFFFFL;

        // Multiply all combinations
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p10 = a1 * b0;
        long p02 = a0 * b2;
        long p11 = a1 * b1;
        long p20 = a2 * b0;
        long p03 = a0 * b3;
        long p12 = a1 * b2;
        long p21 = a2 * b1;
        long p30 = a3 * b0;

        // Accumulate results
        long r0 = p00 & 0xFFFFFFFFL;
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);

        // Handle sign - use the saved original signs
        boolean negative = (thisNegative != otherNegative);
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
     * Round this Decimal160 to the specified scale using the given rounding mode
     * This method performs in-place rounding without requiring a divisor
     *
     * @param targetScale  The desired scale (number of decimal places)
     * @param roundingMode The rounding mode to use
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        validateScale(targetScale);

        // UNNECESSARY mode should be a complete no-op
        if (roundingMode == RoundingMode.UNNECESSARY) {
            return;
        }

        if (this.scale == targetScale) {
            // No rounding needed
            return;
        }

        if (this.scale < targetScale) {
            // Need to increase scale (add trailing zeros)
            int scaleIncrease = targetScale - this.scale;
            multiplyByPowerOf10InPlace(scaleIncrease);
            this.scale = targetScale;
            return;
        }

        // Need to decrease scale (remove decimal places with rounding)
        int scaleDecrease = this.scale - targetScale;

        // Handle zero specially
        if (isZero()) {
            this.scale = targetScale;
            return;
        }

        // Save the sign and work with absolute value
        boolean isNegative = isNegative();
        if (isNegative) {
            negate();
        }

        // Perform the rounding by dividing by 10^scaleDecrease
        long divisor = 1;
        for (int i = 0; i < scaleDecrease; i++) {
            divisor *= 10;
        }

        // Calculate remainder for rounding decision
        long remainder;
        if (this.high == 0) {
            // Simple case - fits in single long
            remainder = Long.remainderUnsigned(this.low, divisor);
            this.low = Long.divideUnsigned(this.low, divisor);
        } else {
            // Complex 128-bit case - calculate remainder and quotient
            remainder = calculateRemainder(divisor);
            divide(0, divisor, RoundingMode.DOWN, false);
        }

        // Apply rounding based on remainder and rounding mode
        boolean shouldRoundUp;

        if (remainder != 0) {
            long halfDivisor = divisor / 2;

            switch (roundingMode) {
                case UP:
                    shouldRoundUp = true;
                    break;
                case DOWN:
                    shouldRoundUp = false;
                    break;
                case CEILING:
                    // Round towards positive infinity
                    shouldRoundUp = !isNegative;
                    break;
                case FLOOR:
                    // Round towards negative infinity
                    shouldRoundUp = isNegative;
                    break;
                case HALF_UP:
                    shouldRoundUp = remainder >= halfDivisor;
                    break;
                case HALF_DOWN:
                    shouldRoundUp = remainder > halfDivisor;
                    break;
                case HALF_EVEN:
                    if (remainder > halfDivisor) {
                        shouldRoundUp = true;
                    } else if (remainder == halfDivisor) {
                        // Tie case - round to even
                        shouldRoundUp = (this.low & 1) == 1;
                    } else {
                        shouldRoundUp = false;
                    }
                    break;
                default:
                    shouldRoundUp = false;
            }
        } else {
            shouldRoundUp = false;
        }

        if (shouldRoundUp) {
            // Add 1 to the result
            this.low++;
            if (this.low == 0) { // Overflow in low part
                this.high++;
            }
        }

        // Restore sign if needed
        if (isNegative) {
            negate();
        }

        // Set the new scale
        this.scale = targetScale;
    }

    /**
     * Set values directly
     */
    public void set(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
    }

    /**
     * Set from a long value
     */
    public void setFromLong(long value, int scale) {
        validateScale(scale);
        this.high = value < 0 ? -1L : 0L;
        this.low = value;
        this.scale = scale;
        updateCompact();
    }

    // Setters for individual fields
    public void setHigh(long high) {
        this.high = high;
        updateCompact();
    }

    public void setLow(long low) {
        this.low = low;
        updateCompact();
    }

    public void setScale(int scale) {
        validateScale(scale);
        this.scale = scale;
    }

    /**
     * Subtract another Decimal160 from this one (in-place)
     *
     * @param other The Decimal160 to subtract
     */
    public void subtract(Decimal160 other) {
        // Handle scale differences first
        if (this.scale < other.scale) {
            // Rescale this to match other's scale
            rescale(other.scale);
        }

        // Now perform subtraction
        if (this.scale == other.scale) {
            // Special case: subtracting zero
            if (other.isZero()) {
                // Nothing to do - subtracting zero doesn't change the value
                return;
            }

            // Direct subtraction via two's complement addition
            // Negate other: ~other + 1
            long otherLow = ~other.low + 1;
            long otherHigh = ~other.high;
            if (otherLow == 0 && other.low != 0) {
                otherHigh += 1;
            }

            long sumLow = this.low + otherLow;
            long carry = hasCarry(this.low, otherLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + otherHigh + carry;
        } else {
            // this.scale > other.scale

            // Special case: subtracting zero
            if (other.isZero()) {
                // Nothing to do - subtracting zero doesn't change the value
                return;
            }

            // Need to scale other up by (this.scale - other.scale)
            int scaleDiff = this.scale - other.scale;
            long otherHigh = other.high;
            long otherLow = other.low;

            // Multiply other by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (otherHigh << 3) | (otherLow >>> 61);
                long low8 = otherLow << 3;
                long high2 = (otherHigh << 1) | (otherLow >>> 63);
                long low2 = otherLow << 1;

                otherLow = low8 + low2;
                long carry = hasCarry(low8, low2, otherLow) ? 1 : 0;
                otherHigh = high8 + high2 + carry;
            }

            // Now negate the scaled value and add
            long negLow = ~otherLow + 1;
            long negHigh = ~otherHigh;
            if (negLow == 0 && otherLow != 0) {
                negHigh += 1;
            }

            long sumLow = this.low + negLow;
            long carry = hasCarry(this.low, negLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + negHigh + carry;
        }
    }

    /**
     * Convert to BigDecimal with full precision
     *
     * @return BigDecimal representation of this Decimal160
     */
    public java.math.BigDecimal toBigDecimal() {
        StringSink sink = new StringSink();
        toSink(sink);
        return new java.math.BigDecimal(sink.toString());
    }

    /**
     * Convert to double (may lose precision)
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

    @Override
    public String toString() {
        // Use StringSink which is already a CharSink - for compatibility
        StringSink sink = new StringSink();
        toSink(sink);
        return sink.toString();
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

    private static long calculatePowerOf10(int n) {
        if (n <= 18) {
            return LONG_TEN_POWERS_TABLE[n];
        }
        long result = 1;
        while (n >= 18) {
            result *= LONG_TEN_POWERS_TABLE[18];
            n -= 18;
        }
        result *= LONG_TEN_POWERS_TABLE[n];
        return result;
    }

    /**
     * Shared logic of need increment computation.
     */
    private static boolean commonNeedIncrement(RoundingMode roundingMode, int qsign,
                                               int cmpFracHalf, boolean oddQuot) {
        switch (roundingMode) {
            case UNNECESSARY:
                throw new ArithmeticException("Rounding necessary");

            case UP: // Away from zero
                return true;

            case DOWN: // Towards zero
                return false;

            case CEILING: // Towards +infinity
                return qsign > 0;

            case FLOOR: // Towards -infinity
                return qsign < 0;

            default: // Some kind of half-way rounding
                if (cmpFracHalf < 0) // We're closer to higher digit
                    return false;
                else if (cmpFracHalf > 0) // We're closer to lower digit
                    return true;
                else { // half-way
                    switch (roundingMode) {
                        case HALF_DOWN:
                            return false;
                        case HALF_UP:
                            return true;
                        case HALF_EVEN:
                            return oddQuot;
                        default:
                            return false;
                    }
                }
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

    private static long computeCompact(long high, long low) {
        // Check if value fits in a single long (high is either 0 or -1)
        if (high == 0 || (high == -1 && low < 0)) {
            return low;
        }
        return INFLATED;
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
            rhat += vnm1Long;
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
        int count = 0;
        while (value > 0) {
            count++;
            value /= 10;
        }
        return count;
    }

    /**
     * Divide 2 compacted values by one another, it might fail when raising operands to the right scale.
     *
     * @param delta        The power of 10 to either raise the dividend or divisor
     * @param roundingMode The Rounding mode to use if the remainder is non-zero
     * @return true if the division was able to take place
     */
    private static boolean divide(Decimal160 result, long dividend, long divisor, int delta, RoundingMode roundingMode) {
        if (dividend == 0) {
            return true;
        } else if (divisor == 0) {
            throw new ArithmeticException("Division by zero");
        }

        if (delta > 0) {
            if ((dividend = longMultiplyPowerTen(dividend, delta)) == 0) {
                // Overflow, we need to fallback on bigger values to properly scale.
                return false;
            }
        } else if (delta < 0) {
            if ((divisor = longMultiplyPowerTen(divisor, -delta)) == 0) {
                // Overflow, we need to fallback on bigger values to properly scale.
                return false;
            }
        }
        divideAndRound(result, dividend, divisor, roundingMode);
        return true;
    }

    /**
     * Internally used for division operation for division {@code long} by
     * {@code long}.
     * The returned {@code BigDecimal} object is the quotient whose scale is set
     * to the passed in scale. If the remainder is not zero, it will be rounded
     * based on the passed in roundingMode. Also, if the remainder is zero and
     * the last parameter, i.e. preferredScale is NOT equal to scale, the
     * trailing zeros of the result is stripped to match the preferredScale.
     */
    private static void divideAndRound(Decimal160 result, long ldividend, long ldivisor, RoundingMode roundingMode) {
        int qsign; // quotient sign
        long q = ldividend / ldivisor; // store quotient in long
        if (roundingMode == RoundingMode.DOWN) {
            result.low = q;
            return;
        }
        long r = ldividend % ldivisor; // store remainder in long
        qsign = ((ldividend < 0) == (ldivisor < 0)) ? 1 : -1;
        if (r != 0) {
            boolean increment = needIncrement(ldivisor, roundingMode, qsign, q, r);
            q = increment ? q + qsign : q;
        }
        result.low = q;
        result.high = q < 0 ? -1L : 0;
    }

    /**
     * Perform an unsigned division of dividend in-place using Knuth 4.3.1D algorithm for 2 128-bit numbers.
     * Note that dividends must have the same scale.
     *
     * @param result       Decimal160 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisorHigh  64-bit high part of the divisor
     * @param divisorLow   64-bit low part of the divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth128x128(Decimal160 result, long dividendHH, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
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
     * @param result       Decimal160 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisorHigh  64-bit high part of the divisor
     * @param divisorLow   64-bit low part of the divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth128x96(Decimal160 result, long dividendHH, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
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
     * @param result       Decimal160 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisor      64-bit divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth128xLong(Decimal160 result, long dividendHH, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
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
        long nChunk = ((long) u4 << Integer.SIZE) | (u3 & LONG_MASK);
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
        nChunk = ((long) u3 << Integer.SIZE) | (u2 & LONG_MASK);
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
     * @param result       Decimal160 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisorHigh  64-bit high part of the divisor
     * @param divisorLow   64-bit low part of the divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth96x96(Decimal160 result, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
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
     * @param result       Decimal160 that will store the rounded result
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisor      64-bit divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuth96xLong(Decimal160 result, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
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
     * @param result       Decimal160 that will store the rounded result
     * @param dividend     64-bit dividend
     * @param divisor      64-bit divisor
     * @param roundingMode Rounding mode that will be used to round the result
     */
    private static void divideKnuthLongxLong(Decimal160 result, long dividend, long divisor, boolean isNegative, RoundingMode roundingMode) {
        long q = Long.divideUnsigned(dividend, divisor);
        long r = Long.remainderUnsigned(dividend, divisor);

        result.high = 0;
        result.low = q;

        final boolean oddQuot = (q & 1L) == 1L;
        endKnuth(result, 0, r, 0, divisor, oddQuot, roundingMode, isNegative);
    }

    private static void divideKnuthXx128(Decimal160 result, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
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
        }
    }

    private static void divideKnuthXx96(Decimal160 result, long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, boolean isNegative, RoundingMode roundingMode) {
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
        }
    }

    private static void divideKnuthXxLong(Decimal160 result, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
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
            }
        }
    }

    /**
     * Divide any 128-bit numbers by a 32-bit one using Knuth 4.3.1 exercise 16.
     *
     * @param result       Decimal160 where the result will be written
     * @param dividendHigh 64-bit high part of the dividend
     * @param dividendLow  64-bit low part of the dividend
     * @param divisor      32-bit divisor
     * @param isNegative   whether the result should be negative
     * @param roundingMode rounding mode used if there is a remainder
     */
    private static void divideKnuthXxWord(Decimal160 result, long dividendHigh, long dividendLow, long divisor, boolean isNegative, RoundingMode roundingMode) {
        int divisorInt = (int) divisor;
        if (divisor == 0) {
            throw new ArithmeticException("Division by zero");
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

    private static void endKnuth(Decimal160 result, long remainderHigh, long remainderLow, long divisorHigh, long divisorLow, boolean oddQuot, RoundingMode roundingMode, boolean isNegative) {
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
                throw new ArithmeticException("Rounding necessary");
            case UP: // Away from zero
                increment = true;
                break;

            case DOWN: // Towards zero
                increment = false;
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

    private static int longCompareMagnitude(long x, long y) {
        if (x < 0)
            x = -x;
        if (y < 0)
            y = -y;
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    /**
     * Compute val * 10 ^ n; return this product if it is
     * representable as a long, 0 otherwise.
     */
    private static long longMultiplyPowerTen(long val, int n) {
        long[] tab = LONG_TEN_POWERS_TABLE;
        long[] bounds = THRESHOLDS_TABLE;
        if (n < tab.length && n < bounds.length) {
            long tenpower = tab[n];
            if (val == 1)
                return tenpower;
            if (Math.abs(val) <= bounds[n])
                return val * tenpower;
        }
        return 0;
    }

    /**
     * Tests if quotient has to be incremented according the roundingMode
     */
    private static boolean needIncrement(long ldivisor, RoundingMode roundingMode,
                                         int qsign, long q, long r) {
        assert r != 0L;

        int cmpFracHalf;
        if (r <= HALF_LONG_MIN_VALUE || r > HALF_LONG_MAX_VALUE) {
            cmpFracHalf = 1; // 2 * r can't fit into long
        } else {
            cmpFracHalf = longCompareMagnitude(2 * r, ldivisor);
        }

        return commonNeedIncrement(roundingMode, qsign, cmpFracHalf, (q & 1L) != 0L);
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
        if (scale < 0 || scale > MAX_SCALE) {
            throw new IllegalArgumentException("Scale must be between 0 and " + MAX_SCALE + ", got: " + scale);
        }
    }

    /**
     * Calculate optimal scale for division to minimize computation while maintaining precision
     * This is much simpler: we want to achieve a target precision, typically 6-16 digits
     */
    private int calculateOptimalScale(long dividendMag, long divisorMag, int dividendScale, int divisorScale) {
        // Calculate how much we need to scale the dividend so that the quotient is a meaningful integer
        // We want: (scaledDividend / divisorMag) >= 1
        // So: scaledDividend >= divisorMag
        // So: dividendMag * 10^scaleAdjustment >= divisorMag
        // So: scaleAdjustment >= log10(divisorMag / dividendMag)

        double ratio = (double) divisorMag / (double) dividendMag;
        int minScaleAdjustment = (int) Math.ceil(Math.log10(ratio));

        // Add a few extra digits for precision
        int targetScaleAdjustment = minScaleAdjustment + 6;

        // Cap to prevent overflow
        return Math.min(15, Math.max(0, targetScaleAdjustment));
    }

    /**
     * Calculate optimal target scale for the division result
     */
    private int calculateOptimalTargetScale(Decimal160 divisor) {
        // Intel-style: Use smart scale instead of always MAX_SCALE
        // Aim for 16 significant digits in the result

        // Estimate magnitude of result using logarithms
        int thisDigits = estimateDecimalDigits128(this);
        int divisorDigits = estimateDecimalDigits128(divisor);
        int resultDigits = Math.max(1, thisDigits - divisorDigits + 1);

        // Target scale to get ~16 significant digits
        int targetScale = Math.min(MAX_SCALE, Math.max(6, 16 - resultDigits));
        return targetScale;
    }

    /**
     * Calculate the remainder when dividing this 128-bit value by a single long divisor
     */
    private long calculateRemainder(long divisor) {
        // Save original values
        long originalHigh = this.high;
        long originalLow = this.low;

        // Use binary long division to find remainder
        long remainderHigh = 0;
        long remainderLow = 0;

        // Process each bit of the dividend from MSB to LSB
        for (int i = 127; i >= 0; i--) {
            // Shift remainder left by 1
            remainderHigh = (remainderHigh << 1) | (remainderLow >>> 63);
            remainderLow = remainderLow << 1;

            // Get bit i from dividend
            boolean dividendBit;
            if (i >= 64) {
                dividendBit = ((originalHigh >>> (i - 64)) & 1) == 1;
            } else {
                dividendBit = ((originalLow >>> i) & 1) == 1;
            }

            if (dividendBit) {
                remainderLow |= 1;
            }

            // Check if remainder >= divisor (divisor has high part = 0)
            if (remainderHigh > 0 || Long.compareUnsigned(remainderLow, divisor) >= 0) {
                // Subtract divisor from remainder
                if (remainderHigh > 0 || Long.compareUnsigned(remainderLow, divisor) >= 0) {
                    remainderLow = remainderLow - divisor;
                    if (Long.compareUnsigned(remainderLow + divisor, remainderLow) < 0) {
                        // There was a borrow
                        remainderHigh--;
                    }
                }
            }
        }

        // The remainder should fit in a single long since divisor is single long
        if (remainderHigh != 0) {
            // This shouldn't happen for proper inputs, but let's handle it gracefully
            return remainderLow;
        }

        return remainderLow;
    }

    /**
     * Intel-style optimization: Check if operands can use 64-bit native division
     * This is much faster than 128-bit binary long division.
     */
    private boolean canUse64BitNativeDivision(Decimal160 divisor) {
        // Both must be 64-bit values
        if (!this.is64BitValue() || !divisor.is64BitValue()) {
            return false;
        }

        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();

        // Avoid division by zero
        if (divisorMag == 0) {
            return false;
        }

        // Simple heuristic: if we can scale the dividend to get a reasonable quotient without overflow
        // Use a conservative scale adjustment based on scale difference
        int scaleDiff = Math.max(6, divisor.scale - this.scale + 6); // Target 6 digits precision

        // Check if scaling is safe - be more generous than the double estimation path
        if (scaleDiff > 0 && scaleDiff <= 15) {
            // Check if dividendMag * 10^scaleDiff fits in long
            long maxScale = (scaleDiff >= LONG_TEN_POWERS_TABLE.length) ? 0 : Long.MAX_VALUE / LONG_TEN_POWERS_TABLE[scaleDiff];
            return dividendMag <= maxScale;
        }

        // If no scaling needed, always use native division
        return scaleDiff <= 0;
    }

    /**
     * EXTREMELY conservative check for 64-bit native division
     * Only applies to very specific cases similar to the user's example:
     * - Both operands are 64-bit integers
     * - Dividend magnitude is reasonably large (> 10^6)
     * - Divisor magnitude is reasonably large (> 10^6)
     * - Scale difference is small (within [-2, 2])
     * - Result will be in a reasonable range
     */
    private boolean canUseSimple64BitNativeDivision(Decimal160 divisor) {
        // Both must be 64-bit values
        if (!this.is64BitValue() || !divisor.is64BitValue()) {
            return false;
        }

        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();

        // Avoid division by zero
        if (divisorMag == 0) {
            return false;
        }

        // CONSERVATIVE: Avoid very small magnitudes that could cause precision issues
        // Set thresholds low enough to handle normal decimal values like 123.456 and 7.89
        if (dividendMag < 100L || divisorMag < 100L) {
            return false;
        }

        // CONSERVATIVE: Avoid cases where dividend is much smaller than divisor
        // This prevents quotient = 0 cases that can cause scale handling bugs
        if (dividendMag * 1000000L < divisorMag) {
            return false;
        }

        // CONSERVATIVE: Scale difference should be small
        // Avoid complex scale handling that can introduce bugs
        int scaleDiff = divisor.scale - this.scale;
        if (scaleDiff < -3 || scaleDiff > 3) {
            return false;
        }

        // Determine optimal scaling factor to maximize precision without overflow
        // Start with 10^6 for 6 decimal places, scale down if needed
        long scalingFactor = 1000000L;
        while (dividendMag > Long.MAX_VALUE / scalingFactor && scalingFactor > 1L) {
            scalingFactor /= 10L;
        }

        // Must have at least some scaling for meaningful precision
        if (scalingFactor < 100L) {
            return false;
        }

        // Estimate result magnitude with dynamic scaling
        double estimatedResult = ((double) dividendMag * (double) scalingFactor) / (double) divisorMag;

        // Only accept if result is in reasonable range (not too big, not too small)
        return estimatedResult >= 0.001 && estimatedResult < 1000000000000.0; // Between 10^-3 and 10^12
    }

    /**
     * Perform unsigned division in-place using binary long division
     *
     * @param divHigh High 64 bits of divisor
     * @param divLow  Low 64 bits of divisor
     */
    private void divide(long divHigh, long divLow, RoundingMode roundingMode, boolean resultNegative) {
        // Handle simple cases first
        if (divHigh == 0 && divLow == 1) {
            // Division by 1 - result is unchanged
            return;
        }

        if (divHigh == 0 && this.high == 0) {
            // Both operands fit in single long - use simple division
            this.low = Long.divideUnsigned(this.low, divLow);
            return;
        }

        // Handle division by zero (should not happen, but safety check)
        if (divHigh == 0 && divLow == 0) {
            throw new ArithmeticException("Division by zero");
        }

        // Save dividend values
        long dividendHigh = this.high;
        long dividendLow = this.low;

        // Initialize result (quotient) to zero
        this.high = 0;
        this.low = 0;

        // If dividend is smaller than divisor, result is 0
        if (compareUnsigned(dividendHigh, dividendLow, divHigh, divLow) < 0) {
            return;
        }

        // Special case: divisor fits in 64 bits (divHigh == 0)
        if (divHigh == 0) {
            // Use optimized 128-bit by 64-bit division
            divide128By64(dividendHigh, dividendLow, divLow);
            return;
        }

        // Use proper binary division
        long quotientHigh = 0;
        long quotientLow = 0;
        long remainderHigh = 0;
        long remainderLow = 0;

        // Find the most significant bit of the dividend
        int startBit;
        if (dividendHigh == 0) {
            startBit = 63 - Long.numberOfLeadingZeros(dividendLow);
        } else {
            startBit = 127 - Long.numberOfLeadingZeros(dividendHigh);
        }

        // Process each bit of the dividend from MSB to LSB
        for (int i = startBit; i >= 0; i--) {
            // Shift remainder left by 1
            remainderHigh = (remainderHigh << 1) | (remainderLow >>> 63);
            remainderLow = remainderLow << 1;

            // Get bit i from dividend
            if (i >= 64) {
                if ((dividendHigh & (1L << (i - 64))) != 0) {
                    remainderLow |= 1;
                }
            } else {
                if ((dividendLow & (1L << i)) != 0) {
                    remainderLow |= 1;
                }
            }

            // Check if remainder >= divisor
            if (remainderHigh > divHigh || (remainderHigh == divHigh && Long.compareUnsigned(remainderLow, divLow) >= 0)) {
                // Subtract divisor from remainder
                long newLow = remainderLow - divLow;
                long borrow = (Long.compareUnsigned(remainderLow, divLow) < 0) ? 1 : 0;
                remainderHigh = remainderHigh - divHigh - borrow;
                remainderLow = newLow;

                // Set bit in quotient
                if (i >= 64) {
                    quotientHigh |= (1L << (i - 64));
                } else {
                    quotientLow |= (1L << i);
                }
            }
        }

        // Apply rounding based on remainder and rounding mode
        // remainderHigh:remainderLow contains the final remainder
        boolean shouldRoundUp = false;

        // Only apply rounding if there's a remainder
        if (remainderHigh != 0 || remainderLow != 0) {
            switch (roundingMode) {
                case UP:
                    shouldRoundUp = true;
                    break;
                case DOWN:
                    shouldRoundUp = false;
                    break;
                case CEILING:
                    // Round towards positive infinity
                    shouldRoundUp = !resultNegative;
                    break;
                case FLOOR:
                    // Round towards negative infinity
                    shouldRoundUp = resultNegative;
                    break;
                case HALF_UP:
                case HALF_DOWN:
                case HALF_EVEN:
                    // Calculate divisor/2 for comparison
                    long halfDivisorHigh = divHigh;
                    long halfDivisorLow = divLow;

                    // Divide by 2: shift right by 1 bit
                    halfDivisorLow = (halfDivisorHigh << 63) | (halfDivisorLow >>> 1);
                    halfDivisorHigh = halfDivisorHigh >>> 1;

                    int cmp = compareUnsigned(remainderHigh, remainderLow, halfDivisorHigh, halfDivisorLow);
                    if (cmp > 0) {
                        // remainder > divisor/2 - always round up
                        shouldRoundUp = true;
                    } else if (cmp < 0) {
                        // remainder < divisor/2 - always round down
                        shouldRoundUp = false;
                    } else {
                        // remainder == divisor/2 - tie case
                        switch (roundingMode) {
                            case HALF_UP:
                                shouldRoundUp = true;
                                break;
                            case HALF_DOWN:
                                shouldRoundUp = false;
                                break;
                            case HALF_EVEN:
                                // Round to even - check if quotient is odd
                                shouldRoundUp = (quotientLow & 1) == 1;
                                break;
                        }
                    }
                    break;
                case UNNECESSARY:
                    // Don't round - keep exact result even if there's a remainder
                    shouldRoundUp = false;
                    break;
            }
        }

        if (shouldRoundUp) {
            // Add 1 to quotient
            quotientLow++;
            if (quotientLow == 0) { // Overflow in low part
                quotientHigh++;
            }
        }

        // Store final quotient
        this.high = quotientHigh;
        this.low = quotientLow;
    }

    /**
     * Optimized division of 128-bit by 64-bit value
     */
    private void divide128By64(long dividendHigh, long dividendLow, long divisor) {
        if (dividendHigh == 0) {
            // Simple case - dividend fits in 64 bits
            this.low = Long.divideUnsigned(dividendLow, divisor);
            this.high = 0;
            return;
        }

        // Divide high part first
        long quotientHigh = Long.divideUnsigned(dividendHigh, divisor);
        long remainder = Long.remainderUnsigned(dividendHigh, divisor);

        // Now divide (remainder:dividendLow) by divisor
        // This is a 128-bit value where the high part (remainder) is guaranteed to be < divisor
        // We can use a more efficient algorithm here

        // For each bit position from 63 to 0
        long quotientLow = 0;
        for (int i = 63; i >= 0; i--) {
            // Shift remainder left by 1 and bring in next bit from dividendLow
            remainder = (remainder << 1) | ((dividendLow >>> i) & 1);

            // Check if remainder >= divisor
            if (Long.compareUnsigned(remainder, divisor) >= 0) {
                remainder -= divisor;
                quotientLow |= (1L << i);
            }
        }

        this.high = quotientHigh;
        this.low = quotientLow;
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
     * Estimate the number of bits used by this value
     */
    private int estimateBitsUsed() {
        if (this.high == 0 || this.high == -1) {
            // Value fits in low part
            return 64 - Long.numberOfLeadingZeros(Math.abs(this.low));
        } else {
            // Value uses high part
            return 128 - Long.numberOfLeadingZeros(Math.abs(this.high));
        }
    }

    /**
     * Estimate decimal digits for 128-bit value
     */
    private int estimateDecimalDigits128(Decimal160 value) {
        if (value.isZero()) return 1;

        // Use binary approximation: log10(x) ≈ log2(x) * 0.30103
        int leadingZeros = value.high != 0 ?
                Long.numberOfLeadingZeros(Math.abs(value.high)) :
                64 + Long.numberOfLeadingZeros(Math.abs(value.low));

        int binaryBits = 128 - leadingZeros;
        return Math.max(1, (int) (binaryBits * 0.30103) + 1);
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
     * Get the absolute magnitude of a 64-bit value (assumes is64BitValue() is true)
     */
    private long get64BitMagnitude() {
        if (this.high == -1 && this.low < 0) {
            return -this.low;  // Convert negative to positive magnitude
        } else {
            return this.low;   // Already positive
        }
    }

    /**
     * Check if this value fits in a 64-bit signed integer (positive or negative)
     */
    private boolean is64BitValue() {
        return (this.high == 0 && this.low >= 0) || (this.high == -1 && this.low < 0);
    }

    /**
     * Check if this value is a negative 64-bit value
     */
    private boolean isNegative64BitValue() {
        return this.high == -1 && this.low < 0;
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
        // if (r4 != 0 && r4 != 999999999999999999L) {
        //     throw new ArithmeticException("Overflow in 128-bit multiplication");
        // }

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this by 10^n in place
     */
    private void multiplyByPowerOf10InPlace(int n) {
        if (n == 0) {
            return;
        }

        // For small powers, use lookup table
        if (n <= 18) {
            long multiplier = LONG_TEN_POWERS_TABLE[n];
            // Special case: if high is 0, use simple 64-bit multiplication
            if (this.high == 0) {
                // Check if result will overflow 64 bits
                if (this.low <= Long.MAX_VALUE / multiplier) {
                    this.low *= multiplier;
                    updateCompact();
                    return;
                }
            }

            // Full 128-bit multiplication by 64-bit value
            multiplyBy64Bit(multiplier);
            updateCompact();
            return;
        }

        // For larger powers, break down into smaller chunks
        // First multiply by largest power that fits in 64 bits (10^18)
        while (n >= 18) {
            multiplyBy64Bit(LONG_TEN_POWERS_TABLE[18]);
            n -= 18;
        }

        // Multiply by remaining power
        if (n > 0) {
            multiplyBy64Bit(LONG_TEN_POWERS_TABLE[n]);
        }
        updateCompact();
    }

    /**
     * Perform simple 64-bit native division with dynamic precision scaling
     */
    private void performSimple64BitNativeDivision(Decimal160 divisor) {
        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();
        boolean resultNegative = this.isNegative() ^ divisor.isNegative();

        // Determine optimal scaling factor (same logic as in canUse method)
        long scalingFactor = 1000000L;
        while (dividendMag > Long.MAX_VALUE / scalingFactor && scalingFactor > 1L) {
            scalingFactor /= 10L;
        }

        // Scale dividend for precision
        long scaledDividend = dividendMag * scalingFactor;

        // Native 64-bit division
        long quotient = scaledDividend / divisorMag;

        // Calculate how many decimal places we added
        int scalingDigits = (int) Math.log10(scalingFactor);

        // Result scale: we added scalingDigits to dividend scale, then subtracted divisor scale
        int resultScale = this.scale + scalingDigits - divisor.scale;

        // Ensure result scale is within bounds
        if (resultScale < 0) {
            // Divide by 10^(-resultScale) to adjust
            long divisorAdj = LONG_TEN_POWERS_TABLE[-resultScale];
            quotient /= divisorAdj;
            resultScale = 0;
        } else if (resultScale > MAX_SCALE) {
            // Multiply by 10^(resultScale - MAX_SCALE) if safe
            int extraScale = resultScale - MAX_SCALE;
            if (extraScale <= 18 && quotient <= Long.MAX_VALUE / LONG_TEN_POWERS_TABLE[extraScale]) {
                quotient *= LONG_TEN_POWERS_TABLE[extraScale];
            }
            resultScale = MAX_SCALE;
        }

        // Set the result
        this.set64BitValue(quotient, resultNegative, resultScale);
    }

    /**
     * Rescale this Decimal160 in place
     *
     * @param newScale The new scale (must be >= current scale)
     */
    private void rescale(int newScale) {
        if (newScale < this.scale) {
            throw new IllegalArgumentException("Cannot reduce scale");
        }

        int scaleDiff = newScale - this.scale;

        // Multiply by 10^scaleDiff
        multiplyByPowerOf10InPlace(scaleDiff);

        this.scale = newScale;
    }

    /**
     * Set this decimal to a 64-bit value with given sign and magnitude
     */
    private void set64BitValue(long magnitude, boolean negative, int scale) {
        if (negative) {
            this.high = -1L;
            this.low = -magnitude;
        } else {
            this.high = 0L;
            this.low = magnitude;
        }
        this.scale = scale;
        updateCompact();
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
        this.compact = computeCompact(this.high, this.low);
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
        for (int i = 0; i <= 10; i++) {
            ZERO_THROUGH_TEN[i] = new Decimal160(0, i, 0);
        }
    }
}