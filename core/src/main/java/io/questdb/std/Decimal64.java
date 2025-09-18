package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Decimal64 - a mutable decimal number implementation using 64-bit arithmetic.
 * The value is a signed number with two's complement representation.
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 64-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 * </p>
 * <p>
 * This is optimized for decimal values that fit within 64 bits, providing better
 * performance than Decimal128 for smaller precision requirements.
 * Maximum precision is approximately 18 digits.
 * </p>
 */
public class Decimal64 implements Sinkable {
    /**
     * Maximum allowed scale (number of decimal places)
     * Limited by the range of 64-bit signed long
     */
    public static final int MAX_SCALE = 18;
    public static final Decimal64 MAX_VALUE = new Decimal64(Long.MAX_VALUE, 0);
    public static final Decimal64 MIN_VALUE = new Decimal64(Long.MIN_VALUE, 0);
    public static final Decimal64 ONE = new Decimal64(1, 0);
    public static final Decimal64 ZERO = new Decimal64(0, 0);
    // Maximum values that 10^n can multiply without overflow
    private static final long[] MAX_SAFE_MULTIPLY = {
            Long.MAX_VALUE,
            Long.MAX_VALUE / 10L,
            Long.MAX_VALUE / 100L,
            Long.MAX_VALUE / 1000L,
            Long.MAX_VALUE / 10000L,
            Long.MAX_VALUE / 100000L,
            Long.MAX_VALUE / 1000000L,
            Long.MAX_VALUE / 10000000L,
            Long.MAX_VALUE / 100000000L,
            Long.MAX_VALUE / 1000000000L,
            Long.MAX_VALUE / 10000000000L,
            Long.MAX_VALUE / 100000000000L,
            Long.MAX_VALUE / 1000000000000L,
            Long.MAX_VALUE / 10000000000000L,
            Long.MAX_VALUE / 100000000000000L,
            Long.MAX_VALUE / 1000000000000000L,
            Long.MAX_VALUE / 10000000000000000L,
            Long.MAX_VALUE / 100000000000000000L,
            Long.MAX_VALUE / 1000000000000000000L,
    };
    // Power of 10 lookup table for 64-bit arithmetic (10^0 to 10^18)
    private static final long[] TEN_POWERS_TABLE = {
            1L,                     // 10^0
            10L,                    // 10^1
            100L,                   // 10^2
            1000L,                  // 10^3
            10000L,                 // 10^4
            100000L,                // 10^5
            1000000L,               // 10^6
            10000000L,              // 10^7
            100000000L,             // 10^8
            1000000000L,            // 10^9
            10000000000L,           // 10^10
            100000000000L,          // 10^11
            1000000000000L,         // 10^12
            10000000000000L,        // 10^13
            100000000000000L,       // 10^14
            1000000000000000L,      // 10^15
            10000000000000000L,     // 10^16
            100000000000000000L,    // 10^17
            1000000000000000000L,   // 10^18
    };
    private int scale;   // Number of decimal places
    private long value;  // The decimal value as an unscaled long

    /**
     * Default constructor - creates zero with scale 0
     */
    public Decimal64() {
        this.value = 0;
        this.scale = 0;
    }

    /**
     * Constructor with initial values.
     *
     * @param value the unscaled decimal value
     * @param scale the number of decimal places
     * @throws NumericException if scale is invalid
     */
    public Decimal64(long value, int scale) {
        validateScale(scale);
        this.value = value;
        this.scale = scale;
    }

    /**
     * Add two Decimal64 numbers and store the result in sink
     */
    public static void add(Decimal64 a, Decimal64 b, Decimal64 sink) {
        sink.copyFrom(a);
        sink.add(b);
    }

    /**
     * Divide two Decimal64 numbers and store the result in sink (a / b -> sink)
     */
    public static void divide(Decimal64 a, Decimal64 b, Decimal64 sink, int resultScale, RoundingMode roundingMode) {
        sink.copyFrom(a);
        sink.divide(b, resultScale, roundingMode);
    }

    /**
     * Create a Decimal64 from a BigDecimal value.
     */
    public static Decimal64 fromBigDecimal(BigDecimal value) {
        if (value == null) {
            throw NumericException.instance().put("BigDecimal cannot be null");
        }

        int scale = value.scale();
        if (scale < 0) {
            // Transform negative scale to positive
            value = value.multiply(BigDecimal.TEN.pow(-scale));
            scale = 0;
        }
        validateScale(scale);

        BigInteger bi = value.unscaledValue();
        if (bi.bitLength() > 63) {
            throw NumericException.instance().put("Overflow: BigDecimal value exceeds 64-bit capacity during conversion");
        }
        long unscaled = bi.longValueExact();
        return new Decimal64(unscaled, scale);
    }

    /**
     * Create a Decimal64 from a double value, using HALF_UP when rounding is needed.
     */
    public static Decimal64 fromDouble(double value, int scale) {
        validateScale(scale);
        BigDecimal bd = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        return fromBigDecimal(bd);
    }

    /**
     * Create a Decimal64 from a long value.
     */
    public static Decimal64 fromLong(long value, int scale) {
        validateScale(scale);
        return new Decimal64(value, scale);
    }

    /**
     * Returns whether the Decimal64 is null or not.
     */
    public static boolean isNull(long value) {
        return value == Decimals.DECIMAL64_NULL;
    }

    /**
     * Calculate modulo of two Decimal64 numbers and store the result in sink (a % b -> sink)
     */
    public static void modulo(Decimal64 a, Decimal64 b, Decimal64 sink) {
        sink.copyFrom(a);
        sink.modulo(b);
    }

    /**
     * Multiply two Decimal64 numbers and store the result in sink
     */
    public static void multiply(Decimal64 a, Decimal64 b, Decimal64 sink) {
        sink.copyFrom(a);
        sink.multiply(b);
    }

    /**
     * Negate a Decimal64 number and store the result in sink
     */
    public static void negate(Decimal64 a, Decimal64 sink) {
        sink.copyFrom(a);
        sink.negate();
    }

    /**
     * Subtract two Decimal64 numbers and store the result in sink (a - b -> sink)
     */
    public static void subtract(Decimal64 a, Decimal64 b, Decimal64 sink) {
        sink.copyFrom(a);
        sink.subtract(b);
    }

    /**
     * Add another Decimal64 to this one (in-place)
     */
    public void add(long otherValue, int otherScale) {
        try {
            if (this.scale == otherScale) {
                // Same scale - direct addition
                this.value = Math.addExact(this.value, otherValue);
            } else if (this.scale < otherScale) {
                // Scale up this to match other's scale
                int scaleDiff = otherScale - this.scale;
                this.value = Math.addExact(scaleUp(this.value, scaleDiff), otherValue);
                this.scale = otherScale;
            } else {
                // Scale up other to match this scale
                int scaleDiff = this.scale - otherScale;
                this.value = Math.addExact(this.value, scaleUp(otherValue, scaleDiff));
            }
        } catch (ArithmeticException e) {
            throw NumericException.instance().put("Overflow in addition: result exceeds 64-bit capacity");
        }
    }

    /**
     * Add another Decimal64 to this one (in-place)
     */
    public void add(Decimal64 other) {
        add(other.value, other.scale);
    }

    /**
     * Compare this to another Decimal64
     */
    public int compareTo(Decimal64 other) {
        return compareTo(other.value, other.scale);
    }

    /**
     * Compare this to another Decimal64
     */
    public int compareTo(long otherValue, int otherScale) {
        if (this.scale == otherScale) {
            return Long.compare(this.value, otherValue);
        }

        // Different scales - need to align for comparison
        long thisScaled, otherScaled;

        if (this.scale < otherScale) {
            int scaleDiff = otherScale - this.scale;
            thisScaled = scaleUp(this.value, scaleDiff);
            otherScaled = otherValue;
        } else {
            int scaleDiff = this.scale - otherScale;
            thisScaled = this.value;
            otherScaled = scaleUp(otherValue, scaleDiff);
        }

        return Long.compare(thisScaled, otherScaled);
    }

    /**
     * Copy values from another Decimal64
     */
    public void copyFrom(Decimal64 source) {
        this.value = source.value;
        this.scale = source.scale;
    }

    /**
     * Divide this by another Decimal64 (in-place)
     */
    public void divide(Decimal64 other, int resultScale, RoundingMode roundingMode) {
        divide(other.value, other.scale, resultScale, roundingMode);
    }

    /**
     * Divide this by another Decimal64 (in-place)
     */
    public void divide(long otherValue, int otherScale, int resultScale, RoundingMode roundingMode) {
        if (otherValue == 0) {
            throw NumericException.instance().put("Division by zero");
        }

        validateScale(resultScale);

        // Scale dividend to get the desired result scale
        // result = (dividend * 10^resultScale) / (divisor * 10^(this.scale - other.scale))
        int scaleAdjustment = resultScale + otherScale - this.scale;

        long dividend = this.value;
        long divisor = otherValue;
        boolean isNegative = (dividend < 0) ^ (divisor < 0);

        // Make both values positive for division
        if (dividend < 0) {
            dividend = -dividend;
        }
        if (divisor < 0) {
            divisor = -divisor;
        }

        // Scale up the dividend if needed
        if (scaleAdjustment > 0) {
            dividend = scaleUpPositive(dividend, scaleAdjustment);
        } else if (scaleAdjustment < 0) {
            // Scale down the dividend (equivalent to scaling up the divisor)
            divisor = scaleUpPositive(divisor, -scaleAdjustment);
        }

        long quotient = dividend / divisor;
        long remainder = dividend % divisor;

        // Apply rounding if there's a remainder
        if (remainder != 0) {
            boolean oddQuotient = (quotient & 1L) == 1L;
            boolean increment = shouldRoundUp(remainder, divisor, oddQuotient, isNegative, roundingMode);

            if (increment) {
                quotient++;
            }
        }

        if (isNegative) {
            quotient = -quotient;
        }

        this.value = quotient;
        this.scale = resultScale;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Decimal64 other = (Decimal64) obj;
        return compareTo(other) == 0;
    }

    /**
     * Get the scale (number of decimal places)
     */
    public int getScale() {
        return this.scale;
    }

    /**
     * Get the unscaled value
     */
    public long getValue() {
        return this.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value) ^ Integer.hashCode(scale);
    }

    /**
     * Check if this decimal is negative
     */
    public boolean isNegative() {
        return this.value < 0;
    }

    /**
     * Returns whether this is null or not.
     *
     * @return true if null, false otherwise
     */
    public boolean isNull() {
        return value == Decimals.DECIMAL64_NULL;
    }

    /**
     * Check if this decimal is zero
     */
    public boolean isZero() {
        return this.value == 0;
    }

    /**
     * Calculate modulo of this by another Decimal64 (in-place)
     */
    public void modulo(Decimal64 other) {
        modulo(other.value, other.scale);
    }

    /**
     * Calculate modulo of this by another Decimal64 (in-place)
     */
    public void modulo(long otherValue, int otherScale) {
        if (otherValue == 0) {
            throw NumericException.instance().put("Division by zero");
        }

        long dividend = this.value;
        long divisor = otherValue;

        // Convert to the same scale for modulo operation
        if (this.scale < otherScale) {
            dividend = scaleUp(dividend, otherScale - this.scale);
        } else if (this.scale > otherScale) {
            divisor = scaleUp(divisor, this.scale - otherScale);
        }

        this.value = dividend % divisor;
        this.scale = Math.max(this.scale, otherScale);
    }

    /**
     * Multiply this by another Decimal64 (in-place)
     */
    public void multiply(Decimal64 other) {
        multiply(other.value, other.scale);
    }

    /**
     * Multiply this by another Decimal64 (in-place)
     */
    public void multiply(long otherValue, int otherScale) {
        try {
            this.value = Math.multiplyExact(this.value, otherValue);
        } catch (ArithmeticException ignored) {
            throw NumericException.instance().put("Overflow in multiplication: product exceeds 64-bit capacity");
        }
        this.scale += otherScale;

        if (this.scale > MAX_SCALE) {
            throw NumericException.instance().put("Overflow in multiplication: resulting scale exceeds maximum (" + MAX_SCALE + ")");
        }
    }

    /**
     * Negate this decimal (in-place)
     */
    public void negate() {
        if (isNull()) {
            return;
        }
        this.value = -this.value;
    }

    public void of(long value, int scale) {
        this.value = value;
        this.scale = scale;
    }

    /**
     * Set this Decimal64 to the null value.
     */
    public void ofNull() {
        value = Decimals.DECIMAL64_NULL;
        scale = 0;
    }

    /**
     * Round this Decimal128 to the specified scale using the given rounding mode.
     * This method performs in-place rounding without requiring a divisor.
     *
     * @param targetScale  the desired scale (number of decimal places)
     * @param roundingMode the rounding mode to use
     * @throws NumericException if targetScale is invalid
     * @throws NumericException if roundingMode is UNNECESSARY and rounding is required
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        if (targetScale == this.scale) {
            // No rounding needed
            return;
        }

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
            this.value = scaleUp(this.value, scaleIncrease);
            this.scale = targetScale;

            if (isNegative) {
                negate();
            }
            return;
        }

        divide(1, 0, targetScale, roundingMode);
    }

    /**
     * Subtract another Decimal64 from this one (in-place)
     */
    public void subtract(Decimal64 other) {
        subtract(other.value, other.scale);
    }

    /**
     * Subtract another Decimal64 from this one (in-place)
     */
    public void subtract(long otherValue, int otherScale) {
        try {
            if (this.scale == otherScale) {
                // Same scale - direct subtraction
                this.value = Math.subtractExact(this.value, otherValue);
            } else if (this.scale < otherScale) {
                // Scale up this to match other's scale
                int scaleDiff = otherScale - this.scale;
                this.value = Math.subtractExact(scaleUp(this.value, scaleDiff), otherValue);
                this.scale = otherScale;
            } else {
                // Scale up other to match this scale
                int scaleDiff = this.scale - otherScale;
                this.value = Math.subtractExact(this.value, scaleUp(otherValue, scaleDiff));
            }
        } catch (ArithmeticException ignored) {
            throw NumericException.instance().put("Overflow in subtraction: result exceeds 64-bit capacity");
        }
    }

    /**
     * Convert to BigDecimal with full precision
     */
    public BigDecimal toBigDecimal() {
        return BigDecimal.valueOf(this.value, this.scale);
    }

    /**
     * Convert to double (may lose precision)
     */
    public double toDouble() {
        return toBigDecimal().doubleValue();
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(toString());
    }

    /**
     * Convert to string representation
     */
    @Override
    public String toString() {
        return toBigDecimal().toString();
    }

    // Helper methods

    /**
     * Compare remainder with half of divisor
     * Returns: negative if remainder < divisor/2, 0 if equal, positive if remainder > divisor/2
     */
    private static int compareWithHalf(long remainder, long divisor) {
        // Calculate divisor/2
        boolean divisorOdd = (divisor & 1L) == 1L;
        long halfDivisor = divisor >>> 1;

        int cmp = Long.compareUnsigned(remainder, halfDivisor);
        if (cmp == 0) {
            // If divisor was odd, then divisor/2 was rounded down, so remainder is actually < divisor/2
            return divisorOdd ? -1 : 0;
        }
        return cmp;
    }

    private static long scaleUp(long value, int scaleDiff) {
        if (scaleDiff == 0) return value;

        long multiplier = TEN_POWERS_TABLE[scaleDiff];

        // Check for overflow
        if (value > 0 && value > MAX_SAFE_MULTIPLY[scaleDiff]) {
            throw NumericException.instance().put("Overflow in scale adjustment: multiplying by 10^" + scaleDiff + " exceeds 64-bit capacity");
        }
        if (value < 0 && value < -MAX_SAFE_MULTIPLY[scaleDiff]) {
            throw NumericException.instance().put("Overflow in scale adjustment: multiplying by 10^" + scaleDiff + " exceeds 64-bit capacity");
        }

        return value * multiplier;
    }

    private static long scaleUpPositive(long value, int scaleDiff) {
        if (scaleDiff >= TEN_POWERS_TABLE.length) {
            throw NumericException.instance().put("Overflow in scale adjustment: multiplying by 10^" + scaleDiff + " exceeds 64-bit capacity");
        }

        long multiplier = TEN_POWERS_TABLE[scaleDiff];

        // Check for overflow
        if (value > 0 && value > MAX_SAFE_MULTIPLY[scaleDiff]) {
            throw NumericException.instance().put("Overflow in scale adjustment: multiplying by 10^" + scaleDiff + " exceeds 64-bit capacity");
        }

        return value * multiplier;
    }

    /**
     * Determine if we should round up based on the remainder and rounding mode
     * Adapted from Decimal128's endKnuth method
     */
    private static boolean shouldRoundUp(long remainder, long divisor, boolean oddQuotient, boolean isNegative, RoundingMode roundingMode) {
        switch (roundingMode) {
            case UNNECESSARY:
                throw NumericException.instance().put("Rounding necessary");
            case UP: // Away from zero
                return true;
            case DOWN: // Towards zero
                return false;
            case CEILING: // Towards +infinity
                return !isNegative;
            case FLOOR: // Towards -infinity
                return isNegative;
            default: // Half-way rounding modes
                int cmp = compareWithHalf(remainder, divisor);
                if (cmp > 0) {
                    return true;
                } else if (cmp == 0) {
                    switch (roundingMode) {
                        case HALF_UP:
                            return true;
                        case HALF_EVEN:
                            return oddQuotient;
                        default:
                            return false;
                    }
                } else {
                    return false;
                }
        }
    }

    private static void validateScale(int scale) {
        if (Integer.compareUnsigned(scale, MAX_SCALE) > 0) {
            throw NumericException.instance().put("Scale must be between 0 and " + MAX_SCALE + ", got: " + scale);
        }
    }
}
