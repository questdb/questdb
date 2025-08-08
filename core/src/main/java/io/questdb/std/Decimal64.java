package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Decimal64 - A mutable decimal number implementation using 64-bit arithmetic.
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
    // Maximum values that can be multiplied by 10^n without overflow
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
    private static final Decimal64[] SMALL_VALUES_CACHE = new Decimal64[101]; // Cache 0-100
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
    // Cache for common small values
    private static final Decimal64[] ZERO_THROUGH_TEN = new Decimal64[11];
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
     * @throws IllegalArgumentException if scale is invalid
     */
    public Decimal64(long value, int scale) {
        validateScale(scale);
        this.value = value;
        this.scale = scale;
    }

    /**
     * Copy constructor for cached values.
     *
     * @param other the Decimal64 to copy from
     */
    private Decimal64(Decimal64 other) {
        this.value = other.value;
        this.scale = other.scale;
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
        int scale = value.scale();
        if (scale < 0) {
            // Transform negative scale to positive
            value = value.multiply(BigDecimal.TEN.pow(-scale));
            scale = 0;
        }
        validateScale(scale);

        long unscaled = value.unscaledValue().longValueExact();
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

        // Use cached values for common small values with scale 0
        if (scale == 0 && value >= 0) {
            if (value <= 10) {
                return new Decimal64(ZERO_THROUGH_TEN[(int) value]);
            } else if (value <= 100) {
                return new Decimal64(SMALL_VALUES_CACHE[(int) value]);
            }
        }

        return new Decimal64(value, scale);
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
    public void add(Decimal64 other) {
        if (this.scale == other.scale) {
            // Same scale - direct addition
            this.value = Math.addExact(this.value, other.value);
        } else if (this.scale < other.scale) {
            // Scale up this to match other's scale
            int scaleDiff = other.scale - this.scale;
            this.value = Math.addExact(scaleUp(this.value, scaleDiff), other.value);
            this.scale = other.scale;
        } else {
            // Scale up other to match this scale
            int scaleDiff = this.scale - other.scale;
            this.value = Math.addExact(this.value, scaleUp(other.value, scaleDiff));
        }
    }

    /**
     * Compare this to another Decimal64
     */
    public int compareTo(Decimal64 other) {
        if (this.scale == other.scale) {
            return Long.compare(this.value, other.value);
        }

        // Different scales - need to align for comparison
        long thisScaled, otherScaled;

        if (this.scale < other.scale) {
            int scaleDiff = other.scale - this.scale;
            thisScaled = scaleUp(this.value, scaleDiff);
            otherScaled = other.value;
        } else {
            int scaleDiff = this.scale - other.scale;
            thisScaled = this.value;
            otherScaled = scaleUp(other.value, scaleDiff);
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
        if (other.value == 0) {
            throw new ArithmeticException("Division by zero");
        }

        validateScale(resultScale);

        // Scale dividend to get the desired result scale
        // result = (dividend * 10^resultScale) / (divisor * 10^(this.scale - other.scale))
        int scaleAdjustment = resultScale + other.scale - this.scale;

        long dividend = this.value;
        long divisor = other.value;
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
            if (scaleAdjustment >= TEN_POWERS_TABLE.length) {
                throw new ArithmeticException("Scale adjustment too large: " + scaleAdjustment);
            }

            long multiplier = TEN_POWERS_TABLE[scaleAdjustment];

            // Check for overflow before multiplication
            if (dividend > MAX_SAFE_MULTIPLY[scaleAdjustment]) {
                throw new ArithmeticException("Overflow in division scaling");
            }

            dividend = dividend * multiplier;
        } else if (scaleAdjustment < 0) {
            // Scale down the dividend (equivalent to scaling up the divisor)
            int absAdjustment = -scaleAdjustment;
            if (absAdjustment >= TEN_POWERS_TABLE.length) {
                throw new ArithmeticException("Scale adjustment too large: " + absAdjustment);
            }

            long multiplier = TEN_POWERS_TABLE[absAdjustment];

            // Check for overflow before multiplication  
            if (divisor > MAX_SAFE_MULTIPLY[absAdjustment]) {
                throw new ArithmeticException("Overflow in division scaling");
            }

            divisor = divisor * multiplier;
        }

        // Perform the division using Knuth algorithm for 64-bit values
        long quotient = divideKnuthLong(dividend, divisor, roundingMode);

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
     * Check if this decimal is zero
     */
    public boolean isZero() {
        return this.value == 0;
    }

    /**
     * Calculate modulo of this by another Decimal64 (in-place)
     */
    public void modulo(Decimal64 other) {
        if (other.value == 0) {
            throw new ArithmeticException("Division by zero");
        }

        // Convert to same scale for modulo operation
        if (this.scale != other.scale) {
            alignScales(this, other);
        }

        this.value = this.value % other.value;
    }

    /**
     * Multiply this by another Decimal64 (in-place)
     */
    public void multiply(Decimal64 other) {
        this.value = Math.multiplyExact(this.value, other.value);
        this.scale += other.scale;

        if (this.scale > MAX_SCALE) {
            throw new ArithmeticException("Scale overflow: result scale would exceed " + MAX_SCALE);
        }
    }

    /**
     * Negate this decimal (in-place)
     */
    public void negate() {
        this.value = Math.negateExact(this.value);
    }

    /**
     * Subtract another Decimal64 from this one (in-place)
     */
    public void subtract(Decimal64 other) {
        if (this.scale == other.scale) {
            // Same scale - direct subtraction
            this.value = Math.subtractExact(this.value, other.value);
        } else if (this.scale < other.scale) {
            // Scale up this to match other's scale
            int scaleDiff = other.scale - this.scale;
            this.value = Math.subtractExact(scaleUp(this.value, scaleDiff), other.value);
            this.scale = other.scale;
        } else {
            // Scale up other to match this scale
            int scaleDiff = this.scale - other.scale;
            this.value = Math.subtractExact(this.value, scaleUp(other.value, scaleDiff));
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

    private static void alignScales(Decimal64 a, Decimal64 b) {
        if (a.scale == b.scale) return;

        if (a.scale < b.scale) {
            int scaleDiff = b.scale - a.scale;
            a.value = scaleUp(a.value, scaleDiff);
            a.scale = b.scale;
        } else {
            int scaleDiff = a.scale - b.scale;
            b.value = scaleUp(b.value, scaleDiff);
            b.scale = a.scale;
        }
    }

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

    /**
     * Knuth division algorithm for 64-bit values
     * Adapted from Decimal128's divideKnuthLongxLong method
     */
    private static long divideKnuthLong(long dividend, long divisor, RoundingMode roundingMode) {
        long quotient = Long.divideUnsigned(dividend, divisor);
        long remainder = Long.remainderUnsigned(dividend, divisor);

        // Apply rounding if there's a remainder
        if (remainder != 0) {
            boolean oddQuotient = (quotient & 1L) == 1L;
            boolean increment = shouldRoundUp(remainder, divisor, oddQuotient, roundingMode);

            if (increment) {
                quotient++;
            }
        }

        return quotient;
    }

    private static long scaleUp(long value, int scaleDiff) {
        if (scaleDiff == 0) return value;
        if (scaleDiff >= TEN_POWERS_TABLE.length) {
            throw new ArithmeticException("Scale difference too large: " + scaleDiff);
        }

        long multiplier = TEN_POWERS_TABLE[scaleDiff];

        // Check for overflow
        if (value > 0 && value > MAX_SAFE_MULTIPLY[scaleDiff]) {
            throw new ArithmeticException("Overflow in scaling operation");
        }
        if (value < 0 && value < -MAX_SAFE_MULTIPLY[scaleDiff]) {
            throw new ArithmeticException("Overflow in scaling operation");
        }

        return Math.multiplyExact(value, multiplier);
    }

    /**
     * Determine if we should round up based on the remainder and rounding mode
     * Adapted from Decimal128's endKnuth method
     */
    private static boolean shouldRoundUp(long remainder, long divisor, boolean oddQuotient, RoundingMode roundingMode) {
        switch (roundingMode) {
            case UNNECESSARY:
                throw new ArithmeticException("Rounding necessary");
            case UP: // Away from zero
                return true;
            case DOWN: // Towards zero
                return false;
            case CEILING: // Towards +infinity (always false since we're dealing with positive values here)
                return true;
            case FLOOR: // Towards -infinity (always false since we're dealing with positive values here)
                return false;
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
                        case HALF_DOWN:
                            return false;
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
            throw new IllegalArgumentException("Scale must be between 0 and " + MAX_SCALE + ", got: " + scale);
        }
    }

    static {
        for (int i = 0; i <= 10; i++) {
            ZERO_THROUGH_TEN[i] = new Decimal64(i, 0);
        }
        for (int i = 0; i <= 100; i++) {
            SMALL_VALUES_CACHE[i] = new Decimal64(i, 0);
        }
    }
}