package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;

/**
 * Decimal128 - A mutable 128-bit decimal number implementation
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 128-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 */
public class Decimal128 implements Sinkable {
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
    }

    /**
     * Constructor with initial values
     */
    public Decimal128(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
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
     *
     * @param a           First operand (dividend)
     * @param b           Second operand (divisor)
     * @param resultScale Desired scale of the result
     * @param sink        Destination for the result
     */
    public static void divide(Decimal128 a, Decimal128 b, int resultScale, Decimal128 sink) {
        sink.copyFrom(a);
        sink.divide(b, resultScale, RoundingMode.HALF_UP);
    }

    /**
     * Divide two Decimal128 numbers and store the result in sink with specified rounding
     *
     * @param a            First operand (dividend)
     * @param b            Second operand (divisor)
     * @param resultScale  Desired scale of the result
     * @param roundingMode The rounding mode to use
     * @param sink         Destination for the result
     */
    public static void divide(Decimal128 a, Decimal128 b, int resultScale, RoundingMode roundingMode, Decimal128 sink) {
        sink.copyFrom(a);
        sink.divide(b, resultScale, roundingMode);
    }

    /**
     * Create a Decimal128 from a double value
     *
     * @param value The double value
     * @param scale Number of decimal places
     */
    public static Decimal128 fromDouble(double value, int scale) {
        long scaleFactor = 1;
        for (int i = 0; i < scale; i++) {
            scaleFactor *= 10;
        }
        long scaledValue = Math.round(value * scaleFactor);
        return fromLong(scaledValue, scale);
    }

    /**
     * Create a Decimal128 from a long value
     *
     * @param value The long value
     * @param scale Number of decimal places
     */
    public static Decimal128 fromLong(long value, int scale) {
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
        sink.negateInPlace();
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
        // If scales match, use direct addition
        if (this.scale == other.scale) {
            // Perform 128-bit addition
            long sumLow = this.low + other.low;

            // Check for carry
            long carry = hasCarry(this.low, other.low, sumLow) ? 1 : 0;

            // Update values in place
            this.low = sumLow;
            this.high = this.high + other.high + carry;
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
        }
    }

    // Static helper methods for non-destructive operations

    /**
     * Compare this to another Decimal128 (handles different scales)
     */
    public int compareTo(Decimal128 other) {
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
     * Copy values from another Decimal128
     */
    public void copyFrom(Decimal128 source) {
        this.high = source.high;
        this.low = source.low;
        this.scale = source.scale;
    }

    /**
     * Divide this Decimal128 by another (in-place) using HALF_UP rounding
     *
     * @param divisor     The Decimal128 to divide by
     * @param resultScale The desired scale of the result
     */
    public void divide(Decimal128 divisor, int resultScale) {
        divide(divisor, resultScale, RoundingMode.HALF_UP);
    }

    /**
     * Divide this Decimal128 by another (in-place) with specified rounding
     *
     * @param divisor      The Decimal128 to divide by
     * @param resultScale  The desired scale of the result
     * @param roundingMode The rounding mode to use
     */
    public void divide(Decimal128 divisor, int resultScale, RoundingMode roundingMode) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // For division: dividend/divisor = result
        // In terms of scales: (dividend * 10^dividend.scale) / (divisor * 10^divisor.scale) = result * 10^result.scale
        // Rearranging: dividend / divisor = result * 10^(result.scale + divisor.scale - dividend.scale)
        // To get the correct result, we need to scale up the dividend by (result.scale + divisor.scale - dividend.scale)
        int scaleAdjustment = resultScale + divisor.scale - this.scale;

        // Scale up this (dividend) if needed to get the right precision
        if (scaleAdjustment > 0) {
            for (int i = 0; i < scaleAdjustment; i++) {
                multiplyBy10InPlace();
            }
        }

        // Track sign - result is negative if signs differ
        boolean thisNeg = this.isNegative();
        boolean divNeg = divisor.isNegative();
        boolean resultNegative = thisNeg != divNeg;

        // Make both positive for unsigned division
        if (thisNeg) {
            negateInPlace();
        }

        // We need divisor as positive - create temp vars (no allocation)
        long divHigh = divisor.high;
        long divLow = divisor.low;
        if (divNeg) {
            // Negate divisor values using two's complement
            divLow = ~divLow + 1;
            divHigh = ~divHigh;
            if (divLow == 0) { // Carry occurred
                divHigh += 1;
            }
        }

        // Handle case where we need to scale down the divisor instead
        if (scaleAdjustment < 0) {
            // Scale up the divisor by |scaleAdjustment|
            for (int i = 0; i < -scaleAdjustment; i++) {
                // Multiply divisor by 10: (8x + 2x)
                long high8 = (divHigh << 3) | (divLow >>> 61);
                long low8 = divLow << 3;
                long high2 = (divHigh << 1) | (divLow >>> 63);
                long low2 = divLow << 1;

                divLow = low8 + low2;
                long carry = hasCarry(low8, low2, divLow) ? 1 : 0;
                divHigh = high8 + high2 + carry;
            }
        }

        // Perform unsigned division in-place
        divideUnsignedInPlace(divHigh, divLow, roundingMode, resultNegative);

        // Set result scale
        this.scale = resultScale;

        // Apply sign if needed
        if (resultNegative) {
            negateInPlace();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Decimal128)) return false;
        Decimal128 other = (Decimal128) obj;
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
    public void modulo(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Save original dividend
        Decimal128 originalDividend = new Decimal128();
        originalDividend.copyFrom(this);

        // Use simple repeated subtraction for modulo: a % b = a - (a / b) * b
        // First compute integer division (a / b)
        Decimal128 quotient = new Decimal128();
        quotient.copyFrom(this);
        quotient.divide(divisor, 0); // Integer division (scale 0)

        // Now compute quotient * divisor
        quotient.multiply(divisor);

        // Finally compute remainder: a - (a / b) * b
        this.subtract(quotient);

        // Handle scale adjustment
        if (this.scale != resultScale) {
            if (this.scale < resultScale) {
                int scaleUp = resultScale - this.scale;
                for (int i = 0; i < scaleUp; i++) {
                    multiplyBy10InPlace();
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
        // Result scale is sum of scales
        int resultScale = this.scale + other.scale;

        // Save the original signs before we modify anything
        boolean thisNegative = this.isNegative();
        boolean otherNegative = other.isNegative();

        // Convert to positive values for multiplication algorithm
        if (thisNegative) {
            negateInPlace();
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

        // Perform multiplication using the algorithm from Decimal128
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
    public void negateInPlace() {
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
     * Round this Decimal128 to the specified scale using the given rounding mode
     * This method performs in-place rounding without requiring a divisor
     *
     * @param targetScale  The desired scale (number of decimal places)
     * @param roundingMode The rounding mode to use
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        if (targetScale < 0) {
            throw new IllegalArgumentException("Target scale cannot be negative");
        }

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
            for (int i = 0; i < scaleIncrease; i++) {
                multiplyBy10InPlace();
            }
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
            negateInPlace();
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
            remainder = this.low % divisor;
            this.low = this.low / divisor;
        } else {
            // Complex 128-bit case - calculate remainder and quotient
            remainder = calculateRemainder(divisor);
            divideUnsignedInPlace(0, divisor, RoundingMode.DOWN, false);
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
            negateInPlace();
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
        this.high = value < 0 ? -1L : 0L;
        this.low = value;
        this.scale = scale;
    }

    // Setters for individual fields
    public void setHigh(long high) {
        this.high = high;
    }

    public void setLow(long low) {
        this.low = low;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * Subtract another Decimal128 from this one (in-place)
     *
     * @param other The Decimal128 to subtract
     */
    public void subtract(Decimal128 other) {
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
     * @return BigDecimal representation of this Decimal128
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
     * Perform unsigned division in-place using binary long division
     *
     * @param divHigh High 64 bits of divisor
     * @param divLow  Low 64 bits of divisor
     */
    private void divideUnsignedInPlace(long divHigh, long divLow, RoundingMode roundingMode, boolean resultNegative) {
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

        // Binary long division - process bit by bit from left to right
        for (int i = 127; i >= 0; i--) {
            // Shift quotient left by 1
            this.high = (this.high << 1) | (this.low >>> 63);
            this.low = this.low << 1;

            // Get bit i from dividend and shift remainder left
            boolean dividendBit;
            if (i >= 64) {
                dividendBit = ((dividendHigh >>> (i - 64)) & 1) == 1;
            } else {
                dividendBit = ((dividendLow >>> i) & 1) == 1;
            }

            // Shift remainder left and add dividend bit
            long remainderHigh = this.high;
            long remainderLow = this.low;

            if (dividendBit) {
                remainderLow |= 1;
            }

            // Check if remainder >= divisor
            if (compareUnsigned(remainderHigh, remainderLow, divHigh, divLow) >= 0) {
                // Subtract divisor from remainder
                long newLow = remainderLow - divLow;
                long borrow = (Long.compareUnsigned(remainderLow, divLow) < 0) ? 1 : 0;
                remainderHigh = remainderHigh - divHigh - borrow;
                remainderLow = newLow;

                // Set bit in quotient
                this.low |= 1;
            }

            // Update remainder for next iteration
            this.high = remainderHigh;
            this.low = remainderLow;
        }

        // The quotient is now in the registers - we built it bit by bit
        // But we need to move it since we were using the registers for remainder too
        // Actually, let's restart with a cleaner approach

        // Reset and use proper binary division
        long quotientHigh = 0;
        long quotientLow = 0;
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
                dividendBit = ((dividendHigh >>> (i - 64)) & 1) == 1;
            } else {
                dividendBit = ((dividendLow >>> i) & 1) == 1;
            }

            if (dividendBit) {
                remainderLow |= 1;
            }

            // Check if remainder >= divisor
            if (compareUnsigned(remainderHigh, remainderLow, divHigh, divLow) >= 0) {
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
                    throw new ArithmeticException("Rounding necessary");
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

    // Note: moduloUnsignedInPlace method removed - modulo now uses division-based approach

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
     * Multiply this by 10 in place
     */
    private void multiplyBy10InPlace() {
        // Multiply by 10: (8x + 2x)
        long high8 = (this.high << 3) | (this.low >>> 61);
        long low8 = this.low << 3;
        long high2 = (this.high << 1) | (this.low >>> 63);
        long low2 = this.low << 1;

        this.low = low8 + low2;
        long carry = hasCarry(low8, low2, this.low) ? 1 : 0;
        this.high = high8 + high2 + carry;
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

        // Multiply by 10^scaleDiff
        for (int i = 0; i < scaleDiff; i++) {
            multiplyBy10InPlace();
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

}